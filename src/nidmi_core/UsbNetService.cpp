#include "UsbNetService.h"

#if NIDMI_USB_NET_SUPPORTED

#include <string.h>

#include "class/net/net_device.h"
#include "device/usbd_pvt.h"
#include "esp32-hal-tinyusb.h"

#include <esp_event.h>
#include <esp_mac.h>
#include <esp_netif.h>
#include <esp_netif_defaults.h>
#include <freertos/FreeRTOS.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>
#include <freertos/task.h>
#include <mdns.h>

namespace {

// --- Identite -------------------------------------------------------------
// Deux MAC distinctes : une pour le netif ESP32, une annoncee a l'hote.
// Les faire identiques casse l'ARP sur le lien.
uint8_t s_devMac[6] = {0};
uint8_t s_hostMac[6] = {0};
char s_devMacStr[18] = {0};   // aa:bb:cc:dd:ee:ff
char s_hostMacStr[13] = {0};  // AABBCCDDEEFF — format impose par NCM
char s_ifName[40] = "NiDMI USB Network";

// --- Etat -----------------------------------------------------------------
esp_netif_t* s_netif = nullptr;
esp_netif_driver_base_t s_driverBase = {};
bool s_interfaceEnabled = false;
bool s_started = false;
bool s_linkUp = false;
bool s_manageMdns = true;
bool s_mdnsAnnounced = false;
uint32_t s_lastLinkAssert = 0;
uint32_t s_lastAnnounce = 0;
uint8_t s_announcesLeft = 0;

nidmi_core::UsbNetStep s_lastStep = nidmi_core::UsbNetStep::NotRegistered;
nidmi_core::UsbNetStats s_stats;

// --- Chemin RX ------------------------------------------------------------
// tud_network_recv_cb s'execute dans la task usbd. On copie la trame, on la
// pousse dans une queue, et une task dediee la remet a lwIP puis renouvelle.
// Renouveler dans le callback ferait recurser recv_renew -> recv_cb autant de
// fois qu'il y a de datagrammes dans le NTB NCM courant.
struct RxFrame {
  void* buf;
  uint16_t len;
};
QueueHandle_t s_rxQueue = nullptr;
TaskHandle_t s_rxTask = nullptr;

// --- Chemin TX ------------------------------------------------------------
uint8_t s_txBuf[CFG_TUD_NET_MTU];
SemaphoreHandle_t s_txMutex = nullptr;
SemaphoreHandle_t s_txDone = nullptr;

constexpr TickType_t kTxWaitTicks = pdMS_TO_TICKS(100);

// --- Tout appel tud_network_* passe par la task usbd ------------------------
// Le driver NCM de TinyUSB n'est pas reentrant : ses listes de NTB et son
// endpoint de notification sont manipules par la task usbd (SET_INTERFACE,
// fins de transfert, reset de bus). Les appeler depuis lwIP, la task RX ou
// loop() les corrompt en course : l'hote ne recoit pas NETWORK_CONNECTION et
// n'active jamais l'interface de donnees (lien « inactive », envois en
// timeout), ou la boucle se fige. Meme parti que esp_tinyusb (tinyusb_net.c) :
// usbd_defer_func() execute l'appel dans la task usbd.
bool s_txOk = false;
uint16_t s_txLen = 0;

void txDeferred(void* arg) {
  (void)arg;
  s_txOk = false;
  if (tud_network_can_xmit(s_txLen)) {
    tud_network_xmit(s_txBuf, s_txLen);  // xmit_cb recopie de facon synchrone
    s_txOk = true;
  }
  xSemaphoreGive(s_txDone);
}

void recvRenewDeferred(void* arg) {
  (void)arg;
  tud_network_recv_renew();
}

void linkStateDeferred(void* arg) {
  tud_network_link_state(0, arg != nullptr);
}

void deferLinkState(bool up) {
  usbd_defer_func(linkStateDeferred, up ? (void*)1 : nullptr, false);
}

void deriveMacs() {
  if (s_devMacStr[0]) {
    return;
  }
  esp_read_mac(s_devMac, ESP_MAC_ETH);
  memcpy(s_hostMac, s_devMac, 6);
  // Meme OUI, dernier octet decale : deux stations distinctes sur le lien.
  s_hostMac[5] = (uint8_t)(s_hostMac[5] ^ 0x01);
  snprintf(s_devMacStr, sizeof(s_devMacStr), "%02x:%02x:%02x:%02x:%02x:%02x", s_devMac[0], s_devMac[1],
           s_devMac[2], s_devMac[3], s_devMac[4], s_devMac[5]);
  snprintf(s_hostMacStr, sizeof(s_hostMacStr), "%02X%02X%02X%02X%02X%02X", s_hostMac[0], s_hostMac[1],
           s_hostMac[2], s_hostMac[3], s_hostMac[4], s_hostMac[5]);
}

// --- Descripteur ----------------------------------------------------------
bool s_descriptorLoaded = false;

extern "C" uint16_t nidmi_usbnet_load_descriptor(uint8_t* dst, uint8_t* itf) {
  if (s_descriptorLoaded) {
    return 0;
  }
  s_descriptorLoaded = true;

  deriveMacs();

  uint8_t strIndex = tinyusb_add_string_descriptor(s_ifName);
  uint8_t macIndex = tinyusb_add_string_descriptor(s_hostMacStr);

  // L'ORDRE COMPTE, et c'est celui-ci qui est valide sur macOS : notification
  // d'abord, paire de donnees en duplex ensuite. On obtient
  //   MIDI IN1/OUT1, NCM notif 0x82, NCM data 0x83/0x03
  //
  // Ne pas « optimiser » cet ordre. L'avoir inverse pour tenter de laisser de
  // la place a un CDC produit notif 0x83 / data 0x82-0x02, et dans cette
  // disposition macOS lie bien AppleUSBNCMData et cree l'interface, mais
  // n'active jamais l'alternate setting 1 : `status: inactive`, aucun bail,
  // zero trame. Le CDC ne rentre de toute facon pas (voir docs/USB_NET.md).
  uint8_t epNotif = tinyusb_get_free_in_endpoint();
  TU_VERIFY(epNotif != 0);
  uint8_t epData = tinyusb_get_free_duplex_endpoint();
  TU_VERIFY(epData != 0);

  uint8_t descriptor[TUD_CDC_NCM_DESC_LEN] = {
    TUD_CDC_NCM_DESCRIPTOR(*itf, strIndex, macIndex, (uint8_t)(0x80 | epNotif), 64, epData,
                           (uint8_t)(0x80 | epData), CFG_TUD_ENDOINT_SIZE, CFG_TUD_NET_MTU)
  };
  *itf += 2;  // interface de controle + interface de donnees
  memcpy(dst, descriptor, TUD_CDC_NCM_DESC_LEN);
  return TUD_CDC_NCM_DESC_LEN;
}

// --- Glue esp_netif -------------------------------------------------------

esp_err_t usbnetTransmit(void* h, void* buffer, size_t len) {
  (void)h;
  if (!s_linkUp || len == 0 || len > sizeof(s_txBuf)) {
    return ESP_ERR_INVALID_STATE;
  }
  if (xSemaphoreTake(s_txMutex, kTxWaitTicks) != pdTRUE) {
    s_stats.txTimeouts++;
    return ESP_ERR_TIMEOUT;
  }

  esp_err_t result = ESP_ERR_TIMEOUT;
  memcpy(s_txBuf, buffer, len);
  s_txLen = (uint16_t)len;
  const TickType_t deadline = xTaskGetTickCount() + kTxWaitTicks;
  while (xTaskGetTickCount() < deadline) {
    xSemaphoreTake(s_txDone, 0);  // purge un eventuel reliquat
    usbd_defer_func(txDeferred, nullptr, false);
    if (xSemaphoreTake(s_txDone, kTxWaitTicks) != pdTRUE) {
      break;  // task usbd muette : on abandonne cette trame
    }
    if (s_txOk) {
      result = ESP_OK;
      break;
    }
    vTaskDelay(1);  // NTB pleins : on laisse l'hote en vider un
  }

  if (result == ESP_OK) {
    s_stats.txFrames++;
  } else {
    s_stats.txTimeouts++;
  }
  xSemaphoreGive(s_txMutex);
  return result;
}

void usbnetFreeRxBuffer(void* h, void* buffer) {
  (void)h;
  free(buffer);
}

esp_err_t usbnetPostAttach(esp_netif_t* netif, esp_netif_iodriver_handle h) {
  esp_netif_driver_ifconfig_t ifconfig = {};
  ifconfig.handle = h;
  ifconfig.transmit = usbnetTransmit;
  ifconfig.driver_free_rx_buffer = usbnetFreeRxBuffer;
  esp_err_t err = esp_netif_set_driver_config(netif, &ifconfig);
  if (err == ESP_OK) {
    s_netif = netif;
  }
  return err;
}

void rxTask(void* arg) {
  (void)arg;
  RxFrame frame;
  for (;;) {
    if (xQueueReceive(s_rxQueue, &frame, portMAX_DELAY) != pdTRUE) {
      continue;
    }
    if (frame.buf != nullptr) {
      if (s_netif == nullptr) {
        free(frame.buf);
        s_stats.rxDropped++;
      } else {
        // esp_netif_receive prend possession du buffer : lwIP le liberera via
        // usbnetFreeRxBuffer, y compris sur ses chemins d'erreur. Ne jamais
        // free() ici apres l'appel, ce serait un double free.
        esp_netif_receive(s_netif, frame.buf, frame.len, frame.buf);
        s_stats.rxFrames++;
      }
    }
    usbd_defer_func(recvRenewDeferred, nullptr, false);
  }
}

}  // namespace

// --- Callbacks TinyUSB (surchargent les weak du core) ----------------------

/**
 * Contrat NCM, verifie dans ncm_device.c : la valeur de retour est IGNOREE
 * (contrairement a ECM/RNDIS qui represente la trame si on rend false), et
 * tud_network_recv_renew() rappelle directement tud_network_recv_cb() pour le
 * datagramme suivant du NTB. Donc exactement un renew par appel, y compris
 * quand on jette la trame : en oublier un fige le RX definitivement.
 */
extern "C" bool tud_network_recv_cb(const uint8_t* src, uint16_t size) {
  RxFrame frame = {nullptr, 0};

  if (size > 0 && s_rxQueue != nullptr) {
    void* buf = malloc(size);
    if (buf != nullptr) {
      memcpy(buf, src, size);
      frame.buf = buf;
      frame.len = size;
    } else {
      s_stats.rxDropped++;
    }
  }

  // Une entree nulle vaut « rien a livrer, renouvelle quand meme ».
  if (s_rxQueue == nullptr || xQueueSend(s_rxQueue, &frame, 0) != pdTRUE) {
    free(frame.buf);
    s_stats.rxDropped++;
    // Dernier recours dans la task usbd. La recursion bornee est moins grave
    // qu'un RX bloque.
    tud_network_recv_renew();
  }
  return true;
}

extern "C" uint16_t tud_network_xmit_cb(uint8_t* dst, void* ref, uint16_t arg) {
  memcpy(dst, ref, arg);
  return arg;
}

extern "C" void tud_network_init_cb(void) {
  s_linkUp = false;
}

namespace nidmi_core {

UsbNetService::UsbNetService() {
  enableInterface();
}

bool UsbNetService::enableInterface() {
  if (s_interfaceEnabled) {
    return true;
  }
  // Pas de deriveMacs() ici : appele depuis un constructeur global, on serait
  // en initialisation statique. Les MAC sont derivees paresseusement, dans le
  // callback de descripteur (execute a USB.begin()) et dans les accesseurs.
  //
  // USB_INTERFACE_CUSTOM : seul slot du core Arduino pour une classe qu'il
  // n'expose pas lui-meme.
  if (tinyusb_enable_interface(USB_INTERFACE_CUSTOM, TUD_CDC_NCM_DESC_LEN, nidmi_usbnet_load_descriptor) !=
      ESP_OK) {
    return false;
  }
  s_interfaceEnabled = true;
  return true;
}

bool UsbNetService::begin(const UsbNetConfig& cfg) {
  if (s_started) {
    return true;
  }
  s_lastStep = UsbNetStep::NotRegistered;
  if (!s_interfaceEnabled) {
    log_e("UsbNetService::enableInterface() doit etre appele avant USB.begin()");
    return false;
  }

  strlcpy(s_ifName, cfg.interfaceName, sizeof(s_ifName));
  s_manageMdns = cfg.manageMdns;

  s_lastStep = UsbNetStep::SyncAlloc;
  s_txMutex = xSemaphoreCreateMutex();
  s_txDone = xSemaphoreCreateBinary();
  s_rxQueue = xQueueCreate(8, sizeof(RxFrame));
  if (s_txMutex == nullptr || s_txDone == nullptr || s_rxQueue == nullptr) {
    return false;
  }

  // Arduino ne les appelle que via WiFi.begin() ; ici le WiFi peut rester eteint.
  s_lastStep = UsbNetStep::NetifInit;
  if (esp_netif_init() != ESP_OK) {
    return false;
  }
  s_lastStep = UsbNetStep::EventLoop;
  esp_err_t err = esp_event_loop_create_default();
  if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
    return false;
  }

  static esp_netif_ip_info_t ipInfo;
  ipInfo.ip.addr = esp_ip4addr_aton(cfg.ip);
  ipInfo.netmask.addr = esp_ip4addr_aton(cfg.netmask);
  ipInfo.gw.addr = cfg.advertiseRouter ? ipInfo.ip.addr : 0;

  static esp_netif_inherent_config_t base = ESP_NETIF_INHERENT_DEFAULT_ETH();
  // Cle ETH_DEF, et pas une cle propre : CONFIG_MDNS_MAX_INTERFACES vaut 3
  // dans les libs Arduino et les trois slots sont deja pris par les
  // interfaces predefinies STA / AP / ETH. mdns_register_netif() echoue donc
  // pour toute interface enregistree a l'execution, et ce n'est pas reglable
  // sans reconstruire les libs IDF. En prenant la cle du slot ETH predefini,
  // le composant mdns nous resout via esp_netif_get_handle_from_ifkey().
  //
  // CONTRAINTE : sur cette pile, ce netif USB et un vrai Ethernet ne peuvent
  // pas coexister sous mDNS.
  base.if_key = "ETH_DEF";
  base.if_desc = "usb_ncm";
  base.route_prio = 10;  // sous le WiFi : jamais l'interface par defaut cote ESP
  base.flags = (esp_netif_flags_t)((cfg.dhcpServer ? ESP_NETIF_DHCP_SERVER : 0) | ESP_NETIF_FLAG_AUTOUP);
  base.ip_info = &ipInfo;
  base.get_ip_event = 0;
  base.lost_ip_event = 0;

  esp_netif_config_t netifConfig = {};
  netifConfig.base = &base;
  netifConfig.driver = nullptr;
  netifConfig.stack = ESP_NETIF_NETSTACK_DEFAULT_ETH;

  s_lastStep = UsbNetStep::NetifNew;
  esp_netif_t* netif = esp_netif_new(&netifConfig);
  if (netif == nullptr) {
    return false;
  }

  s_lastStep = UsbNetStep::NetifAttach;
  s_driverBase.post_attach = usbnetPostAttach;
  s_driverBase.netif = netif;
  if (esp_netif_attach(netif, &s_driverBase) != ESP_OK) {
    esp_netif_destroy(netif);
    return false;
  }

  esp_netif_set_mac(netif, s_devMac);

  if (cfg.dhcpServer) {
    esp_netif_dhcps_stop(netif);  // les options ne se posent qu'a l'arret
    const uint8_t offer = cfg.advertiseRouter ? 1 : 0;
    esp_netif_dhcps_option(netif, ESP_NETIF_OP_SET, ESP_NETIF_ROUTER_SOLICITATION_ADDRESS, (void*)&offer,
                           sizeof(offer));
    esp_netif_dhcps_option(netif, ESP_NETIF_OP_SET, ESP_NETIF_DOMAIN_NAME_SERVER, (void*)&offer,
                           sizeof(offer));
  }

  esp_netif_action_start(netif, nullptr, 0, nullptr);
  if (cfg.dhcpServer) {
    esp_netif_dhcps_start(netif);
  }

  s_lastStep = UsbNetStep::RxTask;
  if (xTaskCreate(rxTask, "usbnet_rx", 4096, nullptr, 12, &s_rxTask) != pdPASS) {
    return false;
  }

  s_lastStep = UsbNetStep::Ok;
  s_started = true;
  return true;
}

void UsbNetService::update() {
  // Gate sur l'enregistrement du descripteur, PAS sur s_started : l'annonce de
  // lien est une affaire purement USB. La conditionner au succes du netif
  // faisait qu'un echec cote reseau privait l'hote de porteur, le laissait sur
  // l'alternate setting 0, et interdisait tout trafic — sans aucun symptome
  // exploitable.
  if (!s_interfaceEnabled) {
    return;
  }

  const bool mounted = tud_mounted();
  if (mounted != s_linkUp) {
    s_linkUp = mounted;
    deferLinkState(mounted);

    if (s_started && s_netif != nullptr) {
      if (mounted) {
        esp_netif_action_connected(s_netif, nullptr, 0, nullptr);
      } else {
        esp_netif_action_disconnected(s_netif, nullptr, 0, nullptr);
      }

      // mDNS seulement maintenant : emise depuis begin(), l'activation ne
      // prend pas, le netif n'etant ni monte ni joignable tant que l'hote n'a
      // pas active l'interface de donnees NCM.
      if (s_manageMdns) {
        if (mounted) {
          mdns_netif_action(s_netif,
                            (mdns_event_actions_t)(MDNS_EVENT_ENABLE_IP4 | MDNS_EVENT_ANNOUNCE_IP4));
          s_mdnsAnnounced = true;
          s_announcesLeft = 5;
          s_lastAnnounce = millis();
        } else {
          mdns_netif_action(s_netif, MDNS_EVENT_DISABLE_IP4);
          s_mdnsAnnounced = false;
          s_announcesLeft = 0;
        }
      }
    }
  }

  // Certains hotes n'activent l'interface de donnees qu'apres avoir recu la
  // notification NETWORK_CONNECTION. Emise une seule fois, elle se perd si
  // l'hote n'a pas fini de se configurer, et personne ne la relance.
  const uint32_t now = millis();
  if (s_linkUp && now - s_lastLinkAssert > 1000) {
    s_lastLinkAssert = now;
    deferLinkState(true);
    // Filet : un renew reporte peut se perdre si la file d'evenements usbd
    // deborde, et le RX resterait fige. Un renew de trop est sans effet.
    usbd_defer_func(recvRenewDeferred, nullptr, false);
  }

  // Meme raison cote mDNS : la pile de l'hote peut n'etre prete qu'apres le
  // bail DHCP.
  if (s_mdnsAnnounced && s_announcesLeft > 0 && now - s_lastAnnounce > 3000) {
    s_lastAnnounce = now;
    s_announcesLeft--;
    mdns_netif_action(s_netif, MDNS_EVENT_ANNOUNCE_IP4);
  }
}

bool UsbNetService::isLinkUp() const {
  return s_linkUp;
}

UsbNetStep UsbNetService::lastStep() const {
  return s_lastStep;
}

UsbNetStats UsbNetService::stats() const {
  return s_stats;
}

IPAddress UsbNetService::localIp() const {
  if (s_netif == nullptr) {
    return IPAddress((uint32_t)0);
  }
  esp_netif_ip_info_t info = {};
  if (esp_netif_get_ip_info(s_netif, &info) != ESP_OK) {
    return IPAddress((uint32_t)0);
  }
  return IPAddress(info.ip.addr);
}

String UsbNetService::broadcastAddress() const {
  if (s_netif == nullptr) {
    return String();
  }
  esp_netif_ip_info_t info = {};
  if (esp_netif_get_ip_info(s_netif, &info) != ESP_OK) {
    return String();
  }
  const uint32_t bcast = (info.ip.addr & info.netmask.addr) | ~info.netmask.addr;
  return IPAddress(bcast).toString();
}

const char* UsbNetService::deviceMac() const {
  deriveMacs();
  return s_devMacStr;
}

const char* UsbNetService::hostMac() const {
  deriveMacs();
  return s_hostMacStr;
}

esp_netif_t* UsbNetService::netif() const {
  return s_netif;
}

}  // namespace nidmi_core

#else  // cible sans USB-OTG (ESP32-C3) ou compilee en usb_mode=1

namespace nidmi_core {

UsbNetService::UsbNetService() {}
bool UsbNetService::enableInterface() {
  return false;
}
bool UsbNetService::begin(const UsbNetConfig&) {
  return false;
}
void UsbNetService::update() {}
bool UsbNetService::isLinkUp() const {
  return false;
}
UsbNetStep UsbNetService::lastStep() const {
  return UsbNetStep::NotRegistered;
}
UsbNetStats UsbNetService::stats() const {
  return UsbNetStats();
}
IPAddress UsbNetService::localIp() const {
  return IPAddress((uint32_t)0);
}
String UsbNetService::broadcastAddress() const {
  return String();
}
const char* UsbNetService::deviceMac() const {
  return "";
}
const char* UsbNetService::hostMac() const {
  return "";
}

}  // namespace nidmi_core

#endif

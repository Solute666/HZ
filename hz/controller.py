import socket
import threading
from pyModbusTCP.client import ModbusClient

from .utils import log_error_once, threads_all
from .settings import get_settings_data
from .network import get_local_ip, get_local_ips

connected_controllers = []
connected_controllers_lock = threading.Lock()
write_lock = threading.Lock()
read_lock = threading.Lock()


class ModbusConnection:
    def __init__(self, ip, port):
        self.master = None
        self.ip = ip
        self.port = int(port)
        self.connected = False
        self.connect_lock = threading.Lock()

    def connect(self):
        if self.connected:
            return
        with self.connect_lock:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((self.ip, self.port))
                sock.close()
                if result != 0:
                    log_error_once(
                        f"Не удалось установить TCP-соединение с {self.ip}:{self.port}"
                    )
                    return
                self.master = ModbusClient(host=self.ip, port=self.port, timeout=2)
                if not self.master.open():
                    log_error_once(
                        f"Не удалось открыть соединение Modbus с {self.ip}:{self.port}"
                    )
                    return
                self.connected = True
                with connected_controllers_lock:
                    if self.ip not in connected_controllers:
                        connected_controllers.append(self.ip)
            except Exception as e:
                self.connected = False
                log_error_once(
                    f"Ошибка подключения к контроллеру {self.ip}:{self.port}: {e}"
                )


def reconnect_unconnected(interval_reconnect, modbus_connections):
    for connection in modbus_connections:
        if connection is None:
            continue
        if not connection.connected:
            connection.connect()


reconnect_timer = None

def schedule_reconnect(interval_reconnect, modbus_connections):
    global reconnect_timer
    if reconnect_timer is not None:
        reconnect_timer.cancel()
    reconnect_timer = threading.Timer(
        interval_reconnect, reconnect_unconnected, args=[interval_reconnect, modbus_connections]
    )
    reconnect_timer.start()
    threads_all.append(reconnect_timer)


modbus_connection_cache = {}
cache_lock = threading.Lock()
settings_lock = threading.Lock()


def get_modbus_connections():
    settings_file = get_settings_data()
    if not settings_file or "controllers" not in settings_file:
        log_error_once("Файл настроек пуст или отсутствует ключ 'controllers'.")
        return []
    controllers = settings_file.get("controllers", [])
    connections = []
    for controller in controllers:
        ip = controller.get("ip")
        port = controller.get("port", 502)
        conn = ModbusConnection(ip, port)
        conn.connect()
        connections.append(conn if conn.connected else None)
    return connections

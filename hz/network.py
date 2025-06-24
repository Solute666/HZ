import socket
import psutil
import netifaces as ni
from .utils import log_error_once


def get_local_ip():
    try:
        interfaces = ni.interfaces()
        for interface in interfaces:
            addresses = ni.ifaddresses(interface)
            if ni.AF_INET in addresses:
                ip_address = addresses[ni.AF_INET][0]['addr']
                if ip_address != '127.0.0.1':
                    return ip_address
        return None
    except Exception as e:
        log_error_once(f"Ошибка при получении локального IP-адреса: {e}")
        return None


def get_local_ips():
    ips = []
    for interface, addrs in psutil.net_if_addrs().items():
        for addr in addrs:
            if addr.family == socket.AF_INET and not addr.address.startswith("127."):
                ips.append(addr.address)
    return ips

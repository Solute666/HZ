import hashlib
import json
import os
import platform
import subprocess
import tkinter as tk
from tkinter import messagebox

MASTER_KEY = "H4743J-KDI42L-KFJGUF-DKEUI7"
LICENSE_FILE = "license.json"


def get_device_fingerprint():
    system_info = {
        "machine": platform.machine(),
        "processor": platform.processor(),
        "system": platform.system(),
        "node": platform.node(),
    }
    if platform.system() == "Windows":
        try:
            disk_serial = subprocess.check_output(
                "wmic diskdrive get serialnumber", shell=True
            ).decode().strip()
            system_info["disk_serial"] = disk_serial.split("\n")[-1].strip()
        except Exception:
            system_info["disk_serial"] = "unknown"
        try:
            uuid_system = subprocess.check_output(
                "wmic csproduct get uuid", shell=True
            ).decode().strip()
            system_info["uuid"] = uuid_system.split("\n")[-1].strip()
        except Exception:
            system_info["uuid"] = "unknown"
    fingerprint_data = "".join(f"{k}:{v}" for k, v in system_info.items())
    return hashlib.sha256(fingerprint_data.encode()).hexdigest()


def generate_license_hash(device_id, master_key):
    salt = os.urandom(16).hex()
    combined_data = f"{device_id}:{master_key}:{salt}"
    hash_object = hashlib.sha256(combined_data.encode())
    return f"{hash_object.hexdigest()}:{salt}"


def create_license_file(device_id, master_key):
    license_hash_with_salt = generate_license_hash(device_id, master_key)
    data = {"device_id": device_id, "license_hash": license_hash_with_salt}
    with open(LICENSE_FILE, "w") as f:
        json.dump(data, f, indent=4)
    print("Файл лицензии создан.")


def validate_license():
    if not os.path.exists(LICENSE_FILE):
        print("Файл лицензии не найден.")
        return False
    try:
        with open(LICENSE_FILE, "r") as f:
            data = json.load(f)
        stored_device_id = data.get("device_id")
        stored_license_hash, stored_salt = data.get("license_hash").split(":")
    except Exception as e:
        print(f"Ошибка чтения файла лицензии: {e}")
        return False
    current_device_id = get_device_fingerprint()
    expected_hash = hashlib.sha256(
        f"{current_device_id}:{MASTER_KEY}:{stored_salt}".encode()
    ).hexdigest()
    if stored_device_id == current_device_id and stored_license_hash == expected_hash:
        print("Лицензия действительна.")
        return True
    print("Лицензия недействительна.")
    return False


def activate_license():
    def on_submit():
        nonlocal license_key
        license_key = entry.get()
        dialog.destroy()

    dialog = tk.Toplevel()
    dialog.title("Активация")
    dialog.geometry("300x100")
    dialog.resizable(False, False)
    label = tk.Label(dialog, text="Введите лицензионный ключ:")
    label.pack(pady=5)
    entry = tk.Entry(dialog, show="*")
    entry.pack(pady=5)
    entry.focus_set()
    submit_button = tk.Button(dialog, text="Подтвердить", command=on_submit)
    submit_button.pack(pady=5)
    license_key = None
    dialog.wait_window(dialog)
    if not license_key:
        messagebox.showerror("Ошибка", "Лицензионный ключ не может быть пустым.")
        return False
    if license_key != MASTER_KEY:
        messagebox.showerror("Ошибка", "Неверный лицензионный ключ.")
        return False
    device_id = get_device_fingerprint()
    create_license_file(device_id, MASTER_KEY)
    messagebox.showinfo("Успех", "Лицензия успешно активирована!")
    return True

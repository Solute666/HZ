import json
import os
import threading
import tkinter as tk
from tkinter import messagebox
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .utils import (
    threads_all,
    process_error_queue,
    process_info_queue,
    log_error_once,
    log_info_once,
)
from .settings import get_settings_data, restart_application
from .network import get_local_ip
from .controller import get_modbus_connections, schedule_reconnect
from .license_utils import validate_license, activate_license


def add_product(product_output):
    try:
        with open('Product_map.json', 'r') as f:
            product_map = json.load(f)
    except FileNotFoundError:
        product_map = {}
    product = tk.Toplevel()
    threads_all.append(product)
    product.resizable(False, False)
    product.geometry(
        "300x150+{}+{}".format(product.winfo_screenwidth() // 2, product.winfo_screenheight() // 2)
    )
    product.title("Добавить продукт")
    label1 = tk.Label(product, text="Введите код продукта:")
    label1.pack()
    entry1 = tk.Entry(product)
    entry1.config(width=30)
    entry1.pack()
    label2 = tk.Label(product, text="Введите название продукта:")
    label2.pack()
    entry2 = tk.Entry(product)
    entry2.config(width=30)
    entry2.pack()

    def add_to_product_map():
        product_name = entry1.get()
        product_code = entry2.get()
        product_map[product_name] = product_code
        with open('Product_map.json', 'w') as F:
            json.dump(product_map, F, indent=4)
        product_output.delete(0, tk.END)
        for code, name in product_map.items():
            product_output.insert(tk.END, f"Код: {code}, Название: {name}")
        product.destroy()

    spacer = tk.Frame(product, height=10)
    spacer.pack()
    button = create_styled_button(product, text="Добавить", command=add_to_product_map, width=10, height=1)
    button.config(width=30, padx=10, pady=10, compound="bottom")
    button.pack()
    product.mainloop()


def delete_product(product_output):
    try:
        selected_index = product_output.curselection()
        if not selected_index:
            log_info_once("Выберите продукт для удаления.")
            return
        selected_index = int(selected_index[0])
        selected_item = product_output.get(selected_index)
        _, product_data = selected_item.split(": ", 1)
        product_code, product_name = product_data.split(", Название: ")
        with open('Product_map.json', 'r') as f:
            product_map = json.load(f)
        del product_map[product_code.strip()]
        with open('Product_map.json', 'w') as f:
            json.dump(product_map, f, indent=4)
        product_output.delete(selected_index)
        product_output.delete(0, tk.END)
        for code, name in product_map.items():
            product_output.insert(tk.END, f"Код: {code}, Название: {name}\n")
    except Exception as e:
        log_error_once(f"Ошибка при удалении продукта: {e}")


def create_styled_button(parent, text, command, width=None, height=None, text_height=12):
    def on_enter(event):
        button.config(bg="#6200EA", fg="white")

    def on_leave(event):
        button.config(bg="#3700B3", fg="white")

    button = tk.Button(
        parent,
        text=text,
        font=("Helvetica", text_height, "bold"),
        bg="#3700B3",
        fg="white",
        relief="flat",
        activebackground="#6200EA",
        activeforeground="white",
        command=command,
        padx=15,
        pady=8,
        borderwidth=0,
        highlightthickness=0,
        width=width,
        height=height,
    )
    button.bind("<Enter>", on_enter)
    button.bind("<Leave>", on_leave)
    return button


def update_ui(root, modbus_connections):
    # Заглушка для сокращения объема примера
    log_info_once("Интерфейс обновлен")


def main():
    root = tk.Tk()
    root.withdraw()
    is_valid = validate_license()
    if not is_valid:
        tk.messagebox.showinfo("Лицензия", "Программа запущена на новом устройстве. Требуется активация.")
        if not activate_license():
            tk.messagebox.showerror("Ошибка", "Активация не завершена. Программа будет закрыта.")
            return
    root.deiconify()
    root.title("Групповая агрегация")
    modbus_connections = get_modbus_connections()
    update_ui(root, modbus_connections)
    settings_data = get_settings_data()
    local_ip = settings_data.get("server_ip", '') or get_local_ip()
    log_info_once(f"Сервер запущен на {local_ip}, ожидание подключений...")
    interval_reconnect = 60
    schedule_reconnect(interval_reconnect, modbus_connections)
    exit_event_1 = threading.Event()
    error_thread = threading.Thread(target=process_error_queue, args=(exit_event_1,))
    info_thread = threading.Thread(target=process_info_queue, args=(exit_event_1,))
    threads_all.extend([error_thread, info_thread])
    error_thread.start()
    info_thread.start()

    class SettingsChangeHandler(FileSystemEventHandler):
        def on_modified(self, event):
            if event.src_path.endswith("settings.json"):
                root.after(0, update_ui, root, modbus_connections)
                log_info_once("Перезагрузка интерфейса")

    observer = Observer()
    observer.schedule(SettingsChangeHandler(), path=os.getcwd(), recursive=False)
    observer.start()
    root.mainloop()

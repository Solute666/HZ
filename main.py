import datetime
import gc
import logging
import queue
import socket
import subprocess
import sys
import threading
import platform
import os
import hashlib
import json
import signal
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
import re
import psutil
import serial
import time
import netifaces as ni
from pyModbusTCP.client import ModbusClient
from serial.tools import list_ports
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

threads_all = []
error_queue = queue.Queue()
info_queue = queue.Queue()
last_info = None

error_history = []  # Хранение последних 5 ошибок
error_history_lock = threading.Lock()
MAX_ERROR_HISTORY = 1  # Максимальное количество записей в истории


def process_error_queue(exit_event_1):
    global error_history
    while not exit_event_1.wait(0.1):
        try:
            error_message = error_queue.get(timeout=0)

            # Проверка, была ли такая ошибка недавно
            if error_message not in error_history:
                current_time = datetime.datetime.now().strftime("%H:%M:%S")
                logging.error(f"[{current_time}] {error_message}")

                # Добавляем новую ошибку в историю
                error_history.append(error_message)

                # Удаляем старые записи, если их больше 5
                if len(error_history) > MAX_ERROR_HISTORY:
                    error_history.pop(0)
        except queue.Empty:
            pass


def process_info_queue(exit_event_1):
    global last_info
    while not exit_event_1.wait(0.1):
        try:
            info_message = info_queue.get(timeout=0.1)
            if info_message != last_info:
                current_time = datetime.datetime.now().strftime("%H:%M:%S")
                logging.info(f"[{current_time}] {info_message}")
                last_info = info_message
        except queue.Empty:
            pass


def log_error_once(error_message):
    if not error_queue.full():
        error_queue.put(error_message)


def log_info_once(info_message):
    if not info_queue.full():
        info_queue.put(info_message)


def add_product(product_output):
    try:
        with open('Product_map.json', 'r') as f:
            product_map = json.load(f)
    except FileNotFoundError:
        product_map = {}

    product = tk.Toplevel()
    threads_all.append(product)
    product.resizable(False, False)
    product.geometry("300x150+{}+{}".format(product.winfo_screenwidth() // 2, product.winfo_screenheight() // 2))
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

    spacer = tk.Frame(product, height=10)  # Создаем пустой фрейм высотой 10 пикселей
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
        del product_map[product_code.strip()]  # Убираем лишние пробелы из названия продукта
        with open('Product_map.json', 'w') as f:
            json.dump(product_map, f, indent=4)
        product_output.delete(selected_index)
        product_output.delete(0, tk.END)
        for code, name in product_map.items():
            product_output.insert(tk.END, f"Код: {code}, Название: {name}\n")
    except Exception as e:
        log_error_once(f"Ошибка при удалении продукта: {e}")


def create_settings_file():
    default_settings = {
        'camera_count': 1,
        'controller_count': 1,
        'cameras': [
            {'ip': "192.168.1.1", 'port': "5000", 'line': "1"}
        ],
        'controllers': [
            {'ip': "192.168.1.1", 'port': "502", 'delay1': 0, 'delay2': 0, 'delay3': 0, 'delay4': 0}
        ],
        'folder_path': os.getcwd(),
        'left': 30,
        'right': 60
    }

    try:
        with open("settings.json", "w") as f:
            json.dump(default_settings, f, indent=4)
        log_info_once("Файл настроек создан.")
    except IOError as e:
        log_error_once(f"Ошибка при создании файла настроек: {e}")


def save_settings(settings_window, camera_count_var, controller_count_var, camera_g_count_var, scanner_count_var,
                  folder_path_label, ip_entries, port_entries,
                  controller_ip_entries, controller_port_entries, ip_entries_g, port_entries_g,
                  com_comboboxes_scan, left_border_value, right_border_value, selected_ip_var):
    try:
        # Загружаем текущие настройки из файла
        try:
            with open("settings.json", "r") as f:
                settings_data = json.load(f)
        except FileNotFoundError:
            log_error_once("Файл настроек не найден. Создается новый файл.")
            settings_data = {}

        # Проверяем длины списков
        if len(ip_entries) != len(port_entries):
            log_error_once("Количество IP-адресов и портов для камер не совпадает.")
            return

        if len(controller_ip_entries) != len(controller_port_entries):
            log_error_once("Количество IP-адресов и портов для контроллеров не совпадает.")
            return

        if len(ip_entries_g) != len(port_entries_g):
            log_error_once("Количество IP-адресов и портов для групповых камер не совпадает.")
            return

        # Обновляем общие настройки
        settings_data['camera_count'] = int(camera_count_var.get())
        settings_data['controller_count'] = int(controller_count_var.get())
        settings_data['camera_g_count'] = int(camera_g_count_var.get())
        settings_data['scanner_count'] = int(scanner_count_var.get())
        settings_data['folder_path'] = folder_path_label.cget("text")
        settings_data['left'] = int(left_border_value)
        settings_data['right'] = int(right_border_value)

        # Обновляем данные об обычных камерах
        cameras = []
        for i in range(len(ip_entries)):
            ip = ip_entries[i].get().strip()
            port = port_entries[i].get().strip()
            if not ip or not port:
                log_error_once(f"IP или порт для камеры {i} пуст. Пропускаем.")
                continue
            cameras.append({'ip': ip, 'port': port, 'line': i})
        settings_data['cameras'] = cameras

        # Обновляем данные о групповых камерах
        cameras_g = []
        mode_aggr = settings_data.get("aggregation_mode", "mode1")
        camera_g_count = int(camera_g_count_var.get())  # Количество линий групповых камер

        for i in range(camera_g_count):
            # Проверяем наличие данных для камеры
            if i >= len(ip_entries_g) or i >= len(port_entries_g):
                log_error_once(f"Отсутствует IP или порт для групповой камеры {i}. Пропускаем.")
                continue

            ip = ip_entries_g[i].get().strip()
            port = port_entries_g[i].get().strip()

            if not ip or not port:
                log_error_once(f"IP или порт для групповой камеры {i} пуст. Пропускаем.")
                continue

            camera_g = {'ip': ip, 'port': port, 'line': i}

            # Если режим mode2, добавляем данные для сканера
            if mode_aggr == "mode2":
                scanner_index = i + camera_g_count  # Индекс для сканера
                if scanner_index >= len(ip_entries_g) or scanner_index >= len(port_entries_g):
                    log_error_once(f"Отсутствует IP или порт для сканера групповой камеры {i}. Пропускаем.")
                    continue

                scanner_ip = ip_entries_g[scanner_index].get().strip()
                scanner_port = port_entries_g[scanner_index].get().strip()

                if not scanner_ip or not scanner_port:
                    log_error_once(f"IP или порт для сканера групповой камеры {i} пуст. Пропускаем.")
                    continue

                camera_g.update({'scanner_ip': scanner_ip, 'scanner_port': scanner_port})

            cameras_g.append(camera_g)

        settings_data['cameras_g'] = cameras_g

        # Обновляем данные о ручных сканерах
        scanners = []
        for i, combobox in enumerate(com_comboboxes_scan):
            com_port = combobox.get().strip()
            if not com_port:
                log_error_once(f"COM-порт для сканера {i} пуст. Пропускаем.")
                continue
            scanners.append({'com_port': com_port, 'line': i})
        settings_data['scanners'] = scanners

        # Обновляем данные о контроллерах
        controllers = []
        for i in range(len(controller_ip_entries)):
            ip = controller_ip_entries[i].get().strip()
            port = controller_port_entries[i].get().strip()
            if not ip or not port:
                log_error_once(f"IP или порт для контроллера {i} пуст. Пропускаем.")
                continue
            controller = {'ip': ip, 'port': port, 'line': i}
            # Добавляем задержки, если они уже были сохранены
            if 'controllers' in settings_data and i < len(settings_data['controllers']):
                delays = settings_data['controllers'][i].get('delays', {})
                controller.update({'delays': delays})
            controllers.append(controller)
        settings_data['controllers'] = controllers

        server_ip = selected_ip_var.get()
        if server_ip:
            settings_data['server_ip'] = server_ip
        else:
            log_info_once("Выбранный IP не корректен или пуст. Используется автоматическое определение.")

        # Сохраняем обновленные настройки в файл
        with open("settings.json", "w") as f:
            json.dump(settings_data, f, indent=4)

        # Закрываем окно настроек
        settings_window.destroy()
        log_info_once("Настройки изменены")

        # Перезапускаем приложение
        restart_application()
        settings_window.destroy()

    except Exception as e:
        log_error_once(f"Ошибка при сохранении настроек: {e}")


def restart_application():
    for thread in threading.enumerate():
        if thread != threading.current_thread():
            try:
                # Пытаемся остановить поток (если он поддерживает это)
                thread.join(timeout=0.1)
            except Exception as e:
                print(f"Ошибка при завершении потока: {e}")
    close_sockets(sockets)
    close_sockets(sockets_controller)
    close_sockets(sockets_s)
    close_sockets(sockets_g)
    python = sys.executable
    try:
        # Перезапускаем процесс
        os.execl(python, python, *sys.argv)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Не удалось перезапустить программу: {e}")
        sys.exit(1)


ip_entries = []
port_entries = []
ip_labels = []
port_labels = []
camera_frames = []


def settings_w():
    global ip_labels, ip_entries, port_labels, port_entries, camera_frames
    settings_window = tk.Toplevel()
    settings_window.title("Настройки")
    settings_window.resizable(False, False)
    screen_width = settings_window.winfo_screenwidth()
    screen_height = settings_window.winfo_screenheight()

    # Вычислить размеры шрифтов и виджетов
    base_font_size = int(screen_height * 0.01)  # 1.5% от высоты экрана
    header_font_size = int(screen_height * 0.02)  # 2% от высоты экрана
    text_width = int(screen_width * 0.035)

    text_height = int(screen_height * 0.02)
    text_width2 = int(text_width * 0.2)

    camera_frames = []
    controller_frames = []
    camera_g_frames = []
    scanner_frames = []

    ip_labels = []
    port_labels = []
    ip_entries = []
    port_entries = []

    ip_labels_g = []
    port_labels_g = []
    ip_entries_g = []
    port_entries_g = []

    com_labels_s = []
    com_comboboxes_s = []

    def update_camera_count(*args):
        settings_data_2 = get_settings_data()
        ip_entries_cameras = []
        port_entries_cameras = []

        # Получение данных камер
        cameras = settings_data_2.get("cameras", [])
        for camera in cameras:
            ip_entries_cameras.append(camera["ip"])
            port_entries_cameras.append(camera["port"])

        # Уничтожение существующих элементов
        for entry in ip_entries:
            entry.destroy()
        for entry in port_entries:
            entry.destroy()
        for label in ip_labels:
            label.destroy()
        for label in port_labels:
            label.destroy()
        ip_entries.clear()
        port_entries.clear()
        ip_labels.clear()
        port_labels.clear()

        for frame in camera_frames:
            frame.destroy()
        camera_frames.clear()

        # Создание новой разметки
        header_label_5 = tk.Label(settings_window, text="Сериализация", font=("Helvetica", header_font_size))
        header_label_5.grid(row=0, column=0, columnspan=8, padx=10, pady=10)

        for i in range(int(camera_count_var.get())):
            frame = tk.Frame(settings_window, borderwidth=2, relief="groove")
            frame.grid(row=i + 1, column=0, columnspan=4, padx=10, pady=5, sticky="ew")
            camera_frames.append(frame)

            ip_label = tk.Label(frame, text=f"IP камеры {i + 1}:", font=("Helvetica", base_font_size))
            ip_label.grid(row=1, column=0, padx=10, pady=5, sticky=tk.E)
            ip_entry = tk.Entry(frame, font=("Helvetica", base_font_size), width=text_width2)
            ip_entry.grid(row=1, column=1, padx=10, pady=5, sticky="e")
            ip_entry.insert(0, ip_entries_cameras[i] if i < len(ip_entries_cameras) else "192.168.1.1")
            ip_labels.append(ip_label)
            ip_entries.append(ip_entry)

            port_label = tk.Label(frame, text=f"Порт камеры {i + 1}:", font=("Helvetica", base_font_size))
            port_label.grid(row=1, column=2, padx=10, pady=5, sticky=tk.E)
            port_entry = tk.Entry(frame, font=("Helvetica", base_font_size), width=text_width2)
            port_entry.grid(row=1, column=3, padx=10, pady=5, sticky="e")
            port_entry.insert(0, port_entries_cameras[i] if i < len(port_entries_cameras) else f"50{i // 10}{i % 10}")
            port_labels.append(port_label)
            port_entries.append(port_entry)

    def update_controller_count(*args):
        settings_data_1 = get_settings_data()
        ip_entries_controllers = []
        port_entries_controllers = []
        controllers = settings_data_1.get("controllers", [])
        for i, controller in enumerate(controllers):
            ip_entry = controller.get("ip")
            port_entry = controller.get("port")
            if ip_entry is not None:
                ip_entries_controllers.append(ip_entry)
            if port_entry is not None:
                port_entries_controllers.append(port_entry)

        for entry in controller_ip_entries:
            if entry and entry.winfo_exists():
                entry.destroy()
        for entry in controller_port_entries:
            if entry and entry.winfo_exists():
                entry.destroy()
        for label in controller_ip_labels:
            if label and label.winfo_exists():
                label.destroy()
        for label in controller_port_labels:
            if label and label.winfo_exists():
                label.destroy()
        controller_ip_entries.clear()
        controller_port_entries.clear()
        controller_ip_labels.clear()
        controller_port_labels.clear()

        for frame in controller_frames:
            frame.destroy()
        controller_frames.clear()

        for i in range(int(controller_count_var.get())):
            frame = tk.Frame(settings_window, borderwidth=2, relief="groove")
            frame.grid(row=i + 1, column=4, columnspan=4, padx=10, pady=5, sticky="w")
            controller_frames.append(frame)

            ip_label = tk.Label(frame, text=f"IP контроллера {i + 1}:", font=("Helvetica", base_font_size))
            ip_label.grid(row=1, column=0, padx=10, pady=5, sticky=tk.E)
            ip_entry = tk.Entry(frame, font=("Helvetica", base_font_size), width=text_width2)
            ip_entry.grid(row=1, column=1, padx=10, pady=5)
            ip_entry.insert(0, ip_entries_controllers[i] if i < len(ip_entries_controllers) else "192.168.1.1")
            controller_ip_labels.append(ip_label)
            controller_ip_entries.append(ip_entry)

            port_label = tk.Label(frame, text=f"Порт контроллера {i + 1}:", font=("Helvetica", base_font_size))
            port_label.grid(row=1, column=2, padx=10, pady=5, sticky=tk.E)
            port_entry = tk.Entry(frame, font=("Helvetica", base_font_size), width=text_width2)
            port_entry.grid(row=1, column=3, padx=10, pady=5)
            port_entry.insert(0, port_entries_controllers[i] if i < len(port_entries_controllers) else "502")
            controller_port_labels.append(port_label)
            controller_port_entries.append(port_entry)

        update_settings_button_position(settings_button2)

    def update_camera_g_count(*args):
        try:
            # Загружаем текущие настройки из файла
            settings_data_2 = get_settings_data()
            mode_aggr = settings_data_2.get("aggregation_mode", "mode1")  # Получаем текущий режим агрегации
            cameras_g = settings_data_2.get("cameras_g", [])

            # Извлекаем IP и порты из данных групповых камер
            ip_entries_cameras_g = [camera.get("ip", "") for camera in cameras_g]
            port_entries_cameras_g = [camera.get("port", "") for camera in cameras_g]
            scanner_ips = [camera.get("scanner_ip", "") for camera in cameras_g if mode_aggr == "mode2"]
            scanner_ports = [camera.get("scanner_port", "") for camera in cameras_g if mode_aggr == "mode2"]

            # Уничтожаем старые элементы интерфейса
            for entry in ip_entries_g + port_entries_g:
                entry.destroy()
            for label in ip_labels_g + port_labels_g:
                label.destroy()

            ip_entries_g.clear()
            port_entries_g.clear()
            ip_labels_g.clear()
            port_labels_g.clear()

            for frame in camera_g_frames:
                frame.destroy()
            camera_g_frames.clear()

            # Создаем новые элементы интерфейса
            camera_count = int(camera_g_count_var.get())
            for i in range(camera_count):
                frame = tk.Frame(settings_window, borderwidth=2, relief="groove")
                frame.grid(row=i + 5, column=0, columnspan=8, padx=10, pady=5, sticky="ew")
                camera_g_frames.append(frame)

                # IP-адрес группы камеры
                ip_label_g = tk.Label(frame, text=f"IP групповой камеры {i + 1}:", font=("Helvetica", base_font_size))
                ip_label_g.grid(row=0, column=0, padx=10, pady=5, sticky=tk.E)
                ip_entry_g = tk.Entry(frame, font=("Helvetica", base_font_size), width=text_width2)
                ip_entry_g.grid(row=0, column=1, padx=10, pady=5, sticky="e")
                ip_entry_g.insert(0, ip_entries_cameras_g[i] if i < len(ip_entries_cameras_g) else "192.168.1.1")
                ip_labels_g.append(ip_label_g)
                ip_entries_g.append(ip_entry_g)

                # Порт группы камеры
                port_label_g = tk.Label(frame, text=f"Порт групповой камеры {i + 1}:",
                                        font=("Helvetica", base_font_size))
                port_label_g.grid(row=0, column=2, padx=10, pady=5, sticky=tk.E)
                port_entry_g = tk.Entry(frame, font=("Helvetica", base_font_size), width=text_width2)
                port_entry_g.grid(row=0, column=3, padx=10, pady=5, sticky="e")
                port_entry_g.insert(0, port_entries_cameras_g[i] if i < len(
                    port_entries_cameras_g) else f"50{i // 10}{i % 10}")
                port_labels_g.append(port_label_g)
                port_entries_g.append(port_entry_g)

                # Добавляем поле для стационарного сканера, если режим aggregation_mode == "mode2"
                if mode_aggr == "mode2":
                    scanner_ip_label = tk.Label(frame, text=f"IP сканера {i + 1}:", font=("Helvetica", base_font_size))
                    scanner_ip_label.grid(row=0, column=4, padx=10, pady=5, sticky=tk.E)
                    scanner_ip_entry = tk.Entry(frame, font=("Helvetica", base_font_size), width=text_width2)
                    scanner_ip_entry.grid(row=0, column=5, padx=10, pady=5, sticky="e")
                    scanner_ip_entry.insert(0, scanner_ips[i] if i < len(scanner_ips) else "192.168.1.1")
                    ip_labels_g.append(scanner_ip_label)
                    ip_entries_g.append(scanner_ip_entry)

                    scanner_port_label = tk.Label(frame, text=f"Порт сканера {i + 1}:",
                                                  font=("Helvetica", base_font_size))
                    scanner_port_label.grid(row=0, column=6, padx=10, pady=5, sticky=tk.E)
                    scanner_port_entry = tk.Entry(frame, font=("Helvetica", base_font_size), width=text_width2)
                    scanner_port_entry.grid(row=0, column=7, padx=10, pady=5, sticky="e")
                    scanner_port_entry.insert(0, scanner_ports[i] if i < len(scanner_ports) else f"50{i // 10}{i % 10}")
                    port_labels_g.append(scanner_port_label)
                    port_entries_g.append(scanner_port_entry)

        except Exception as e:
            log_error_once(f"Ошибка при обновлении количества групповых камер: {e}")

    def update_scaner_count(*args):
        """
        Обновляет интерфейс для ручных сканеров при изменении их количества.
        """
        try:
            # Загружаем текущие настройки
            settings_data = get_settings_data()
            scanners = settings_data.get("scanners", [])

            # Очищаем существующие элементы интерфейса для сканеров
            for frame in scanner_frames:
                frame.destroy()
            scanner_frames.clear()
            for label in com_labels_s:
                label.destroy()
            com_labels_s.clear()
            for combobox in com_comboboxes_s:
                combobox.destroy()
            com_comboboxes_s.clear()

            # Проверяем, нужно ли отображать раздел для ручных сканеров
            if not settings_data.get("use_pall_aggregation", False):
                return

            # Добавляем заголовок для ручных сканеров
            header_label_7 = tk.Label(
                settings_window,
                text="Ручные сканеры",
                font=("Helvetica", header_font_size)
            )
            header_label_7.grid(row=8, column=0, columnspan=8, padx=10, pady=10)

            # Получаем список доступных COM-портов
            def get_available_com_ports():
                """
                Возвращает список доступных COM-портов.
                """
                try:
                    ports = list_ports.comports()
                    return [port.device for port in ports]
                except Exception as e:
                    log_error_once(f"Ошибка при получении списка COM-портов: {e}")
                    return []

            available_com_ports = get_available_com_ports()

            # Создаем новые элементы интерфейса для сканеров
            scanner_count = int(scanner_count_var.get())
            for i in range(scanner_count):
                # Создаем фрейм для сканера
                frame = tk.Frame(settings_window, borderwidth=2, relief="groove")
                frame.grid(row=i + 9, column=0, columnspan=4, padx=10, pady=5, sticky="ew")
                scanner_frames.append(frame)

                # Метка для COM-порта
                com_label_scan = tk.Label(
                    frame,
                    text=f"COM-порт сканера {i + 1}:",
                    font=("Helvetica", base_font_size)
                )
                com_label_scan.grid(row=0, column=0, padx=10, pady=5, sticky=tk.E)
                com_labels_s.append(com_label_scan)

                # Выпадающий список для выбора COM-порта
                com_combobox_scan = ttk.Combobox(
                    frame,
                    font=("Helvetica", base_font_size),
                    width=text_width2
                )
                com_combobox_scan['values'] = available_com_ports  # Устанавливаем доступные COM-порты
                com_combobox_scan.grid(row=0, column=1, padx=10, pady=5, sticky="e")

                # Устанавливаем значение по умолчанию (если есть сохраненные настройки)
                if i < len(scanners) and "com_port" in scanners[i]:
                    com_combobox_scan.set(scanners[i]["com_port"])
                elif available_com_ports:
                    com_combobox_scan.set(available_com_ports[0])  # Первый доступный порт по умолчанию

                com_comboboxes_s.append(com_combobox_scan)  # Сохраняем ссылку на Combobox

            # Обновляем позицию кнопки сохранения настроек
        except Exception as e:
            log_error_once(f"Ошибка при обновлении интерфейса для ручных сканеров: {e}")

    header_label_6 = tk.Label(settings_window, text="Групповая агрегация", font=("Helvetica", header_font_size))
    header_label_6.grid(row=4, column=0, columnspan=8, padx=10, pady=10)

    def update_settings_button_position(button):
        button.grid_forget()
        max_entries = len(ip_entries) * 2
        button.grid(row=(max_entries + 8), column=0, columnspan=8, sticky=tk.EW)

    def open_reg_window():
        reg_window = tk.Toplevel(settings_window)
        reg_window.resizable(False, False)
        reg_window.title("Настройка регистров")

        # Значения по умолчанию
        default_registers = {
            "bottles": [900, 900, 900],
            "packs": [902, 902, 902],
            "pallets": [904, 904, 904],
            "total_packs": [934, 934, 934],
            "total_bottles": [936, 936, 936],
            "bottles_ser": [930, 930, 930],
            "new_pallet": [918, 918, 918],
            "new_file": [922, 922, 922],
            "green_column": [908, 908, 908],
            "red_column": [938, 938, 938],
            "group_cam": [912, 912, 912],
            "scanner": [914, 914, 914],
            "controller": [932, 932, 932],
            "pal_open": [906, 906, 906],
            "all_ser": [411, 411, 411],
            "suc_ser": [412, 412, 412],
            "def_ser": [413, 413, 413],
            "serv_work": [415, 415, 415]
        }

        # Переменные для хранения значений из полей ввода
        register_vars = {
            "bottles": [],
            "packs": [],
            "pallets": [],
            "total_packs": [],
            "total_bottles": [],
            "bottles_ser": [],
            "new_pallet": [],
            "new_file": [],
            "green_column": [],
            "red_column": [],
            "group_cam": [],
            "scanner": [],
            "controller": [],
            "pal_open": [],
            "all_ser": [],
            "suc_ser": [],
            "def_ser": [],
            "serv_work": []
        }

        try:
            with open("settings.json", "r") as f:
                settings_data = json.load(f)
            register_data = settings_data.get("registers", {})
        except FileNotFoundError:
            log_error_once("Файл настроек не найден.")
            register_data = {}

        # Заголовки колонок (линии)
        for col_idx in range(3):
            tk.Label(reg_window, text=f"Линия {col_idx + 1}").grid(row=1, column=col_idx + 1, padx=5, pady=2)

        # Строки с типами регистров
        register_types = [
            ("Счётчик бутылок", "bottles"),
            ("Счётчик упаковок", "packs"),
            ("Счётчик паллет", "pallets"),
            ("Cчётчик упаковок общий", "total_packs"),
            ("Счётчик бутылок общий", "total_bottles"),
            ("Счётчик бутылок сериализации", "bottles_ser"),
            ("Новый файл", "new_file"),
            ("Новый паллет", "new_pallet"),
            ("Колонна зелёный", "green_column"),
            ("Колонна красный", "red_column"),
            ("Групповая камера", "group_cam"),
            ("Сканер", "scanner"),
            ("Контроллер", "controller"),
            ("Открыть палет", "pal_open"),
            ("Сериализация всего", "all_ser"),
            ("Сериализация удачные", "suc_ser"),
            ("Сериализация брак", "def_ser"),
            ("Работа сервера", "serv_work")
        ]

        for row_idx, (label_text, key) in enumerate(register_types):
            tk.Label(reg_window, text=label_text).grid(row=row_idx + 2, column=0, padx=5, pady=2)

            for col_idx in range(3):  # 3 линии
                # Получаем значение из файла или по умолчанию
                value_source = register_data.get(key, default_registers[key])

                if isinstance(value_source, list) and len(value_source) > col_idx:
                    val = value_source[col_idx]
                elif isinstance(value_source, dict):
                    val = value_source.get(str(col_idx), default_registers[key][col_idx])
                else:
                    val = default_registers[key][col_idx]

                var = tk.IntVar(value=val)
                entry = tk.Entry(reg_window, textvariable=var, width=10)
                entry.grid(row=row_idx + 2, column=col_idx + 1, padx=5, pady=2)

                register_vars[key].append(var)

        def save_register_settings():
            # Загружаем текущие настройки
            try:
                with open("settings.json", "r") as f:
                    settings_data = json.load(f)
            except FileNotFoundError:
                settings_data = {}

            # Обновляем раздел registers
            settings_data["registers"] = {
                "bottles": [var.get() for var in register_vars["bottles"]],
                "packs": [var.get() for var in register_vars["packs"]],
                "pallets": [var.get() for var in register_vars["pallets"]],
                "total_packs": [var.get() for var in register_vars["total_packs"]],
                "total_bottles": [var.get() for var in register_vars["total_bottles"]],
                "bottles_ser": [var.get() for var in register_vars["bottles_ser"]],
                "new_pallet": [var.get() for var in register_vars["new_pallet"]],
                "new_file": [var.get() for var in register_vars["new_file"]],
                "green_column": [var.get() for var in register_vars["green_column"]],
                "red_column": [var.get() for var in register_vars["red_column"]],
                "group_cam": [var.get() for var in register_vars["group_cam"]],
                "scanner": [var.get() for var in register_vars["scanner"]],
                "controller": [var.get() for var in register_vars["controller"]],
                "pal_open": [var.get() for var in register_vars["pal_open"]],
                "all_ser": [var.get() for var in register_vars["all_ser"]],
                "suc_ser": [var.get() for var in register_vars["suc_ser"]],
                "def_ser": [var.get() for var in register_vars["def_ser"]],
                "serv_work": [var.get() for var in register_vars["serv_work"]],
            }

            # Сохраняем обновлённые настройки в файл
            with open("settings.json", "w") as f:
                json.dump(settings_data, f, indent=4)

            reg_window.destroy()
            log_info_once("Настройки регистров сохранены")
            restart_application()

        # Кнопка сохранить
        save_button = create_styled_button(reg_window, text="Сохранить", command=save_register_settings)
        save_button.grid(row=20, column=0, columnspan=4, pady=10, ipadx=20, ipady=10)

    def open_aggr_window():
        aggr_window = tk.Toplevel(settings_window)
        aggr_window.resizable(False, False)
        aggr_window.title("Настройка групповой агрегации")

        # Переменные для хранения введённых данных
        len_gs1_var = tk.IntVar()
        smesch_var = tk.IntVar()
        delay_pack_var = tk.IntVar()
        bottles_per_pack_var = []
        packs_per_pallet_var = tk.IntVar()
        use_aggregation_var = tk.BooleanVar()
        use_serial_var = tk.BooleanVar()
        delay_vars = []  # Новый список для хранения задержек по линиям
        global aggregation_mode_var  # Глобальная переменная для хранения выбранного режима агрегации
        aggregation_mode_var = tk.StringVar(value="mode1")  # По умолчанию выбран первый режим

        # Загрузка текущих настроек, если файл существует
        try:
            with open("settings.json", "r") as file:
                settings_data = json.load(file)
                len_gs1_var.set(settings_data.get("len_gs1", 0))
                smesch_var.set(settings_data.get("smeschenie", 0))
                delay_pack_var.set(settings_data.get("delay_pack", 0))
                bottles_per_pack_by_line = settings_data.get("bottles_per_pack_by_line", {})
                delays_by_line = settings_data.get("delays_by_line", {})  # Загружаем задержки для линий
                for line in range(1, 4):  # Предполагаем максимум 3 линии
                    var_bottle = tk.IntVar(value=bottles_per_pack_by_line.get(str(line), 0))
                    bottles_per_pack_var.append(var_bottle)
                    var_delay = tk.IntVar(value=delays_by_line.get(str(line), 0))  # Загружаем задержку для линии
                    delay_vars.append(var_delay)
                packs_per_pallet_var.set(settings_data.get("packs_per_pallet", 0))
                use_aggregation_var.set(settings_data.get("use_pall_aggregation", False))
                use_serial_var.set(settings_data.get("use_serial", False))
                aggregation_mode_var.set(settings_data.get("aggregation_mode", "mode1"))  # Загрузка сохранённого режима
        except FileNotFoundError:
            log_error_once("Файл настроек не найден. Используются значения по умолчанию.")

        # Функция для создания полей ввода количества бутылок
        def create_bottle_input(row, line_number):
            label = tk.Label(aggr_window, text=f"Бутылок в пакке (Линия {line_number}):")
            label.grid(row=row, column=0, padx=5, pady=5, sticky=tk.E)
            entry = tk.Entry(aggr_window, textvariable=bottles_per_pack_var[line_number - 1])
            entry.grid(row=row, column=1, padx=5, pady=5)

        # Функция для создания полей ввода задержек
        def create_delay_input(row, line_number):
            label = tk.Label(aggr_window, text=f"Задержка (Линия {line_number}):")
            label.grid(row=row, column=0, padx=5, pady=5, sticky=tk.E)
            entry = tk.Entry(aggr_window, textvariable=delay_vars[line_number - 1])
            entry.grid(row=row, column=1, padx=5, pady=5)

        # Создаем поля ввода для каждой линии
        for line_number in range(1, 4):  # Предполагаем максимум 3 линии
            create_bottle_input(line_number - 1, line_number)
            create_delay_input(line_number + 2, line_number)  # Смещаем строки для задержек

        # Остальные элементы интерфейса
        len_gs1_label = tk.Label(aggr_window, text="Длина группового кода:")
        len_gs1_label.grid(row=6, column=0, padx=5, pady=5, sticky=tk.E)
        len_gs1_entry = tk.Entry(aggr_window, textvariable=len_gs1_var)
        len_gs1_entry.grid(row=6, column=1, padx=5, pady=5)

        smesch_label = tk.Label(aggr_window, text="Смещение регистров:")
        smesch_label.grid(row=7, column=0, padx=5, pady=5, sticky=tk.E)
        smesch_entry = tk.Entry(aggr_window, textvariable=smesch_var)
        smesch_entry.grid(row=7, column=1, padx=5, pady=5)

        use_aggregation_check = tk.Checkbutton(aggr_window, text="Использовать агрегацию второго уровня",
                                               variable=use_aggregation_var)
        use_aggregation_check.grid(row=8, column=0, columnspan=2, padx=5, pady=5)

        serial_check = tk.Checkbutton(aggr_window, text="Использовать сериализацию с групповой",
                                      variable=use_serial_var)
        serial_check.grid(row=9, column=0, columnspan=2, padx=5, pady=5)

        # Радиокнопки для выбора режима агрегации
        mode1_rb = tk.Radiobutton(aggr_window, text="Одна камера", variable=aggregation_mode_var, value="mode1")
        mode1_rb.grid(row=10, column=0, padx=5, pady=5, sticky=tk.W)
        mode2_rb = tk.Radiobutton(aggr_window, text="Камера + сканер", variable=aggregation_mode_var, value="mode2")
        mode2_rb.grid(row=11, column=0, padx=5, pady=5, sticky=tk.W)

        # Функция для сохранения настроек
        def save_aggregation_settings():
            # Загружаем текущие настройки из файла
            try:
                with open("settings.json", "r") as file:
                    settings_data = json.load(file)
            except FileNotFoundError:
                settings_data = {}  # Если файл не найден, создаём пустой словарь

            # Обновляем только те поля, которые относятся к агрегации
            settings_data.update({
                "len_gs1": len_gs1_var.get(),
                "smeschenie": smesch_var.get(),
                "delay_pack": delay_pack_var.get(),
                "bottles_per_pack_by_line": {
                    str(line): var.get() for line, var in enumerate(bottles_per_pack_var, start=1)
                },
                "delays_by_line": {
                    str(line): var.get() for line, var in enumerate(delay_vars, start=1)  # Сохраняем задержки
                },
                "packs_per_pallet": packs_per_pallet_var.get(),
                "use_pall_aggregation": use_aggregation_var.get(),
                "use_serial": use_serial_var.get(),
                "aggregation_mode": aggregation_mode_var.get()  # Сохраняем выбранный режим
            })

            # Сохраняем обновлённые настройки в файл
            with open("settings.json", "w") as file:
                json.dump(settings_data, file, indent=4)

            # Закрываем окно настроек
            aggr_window.destroy()

            # Логируем успешное сохранение
            log_info_once("Настройки изменены")

            restart_application()
            settings_window.destroy()

        # Кнопка сохранения
        save_button = create_styled_button(aggr_window, text="Сохранить", command=save_aggregation_settings)
        save_button.grid(row=12, column=0, columnspan=2, sticky=tk.EW, padx=20, pady=10, ipadx=20, ipady=10)

    settings_data = get_settings_data()
    path_label = settings_data.get("folder_path", "")

    try:
        settings_data = get_settings_data()
    except FileNotFoundError:
        settings_data = {
            "camera_count": 1,
            "controller_count": 1,
            "camera_g_count": 1,
            "scanner_count": 1,
            "cameras": [{"ip": "192.168.1.1", "port": "5000", "line": "1"}],
            "controllers": [{"ip": "192.168.1.1", "port": "502", "delay1": 0, "delay2": 0, "delay3": 0, "delay4": 0}],
            "cameras_g": [{"ip": "192.168.1.1", "port": "5000", "line": "1"}],
            "cameras_gs1": [{"ip": "192.168.1.1", "port": "5000", "line": "1"}],
            "folder_path": os.getcwd(),
            "left": 30,
            "right": 60,
            "len_gs1": 21,
            "delay_pack": 5,
            "bottles_per_pack": 6,
            "packs_per_pallet": 10,
            "use_pall_aggregation": False,
            "aggregation_mode": "mode1"
        }

    product_output = tk.Listbox(settings_window, width=text_width, height=text_height)
    product_output.grid(row=1, rowspan=5, column=8, columnspan=4, padx=10, pady=5)
    product_output_scrollbar = tk.Scrollbar(settings_window, command=product_output.yview)
    product_output_scrollbar.grid(row=1, column=12, rowspan=5, sticky="nsw")
    product_output.config(yscrollcommand=product_output_scrollbar.set)

    product_info = []
    try:
        with open('Product_map.json', 'r') as f:
            product_map = json.load(f)
    except FileNotFoundError:
        product_map = {}

    # Добавление информации о продуктах в список
    for code, name in product_map.items():
        product_info.append(f"Код: {code}, Название: {name} ")

    for item in product_info:
        product_output.insert(tk.END, item)

    label7 = tk.Label(settings_window, text="Место сохранения файлов:", font=("Helvetica", base_font_size))
    label7.grid(row=10, column=8, columnspan=4, padx=10, pady=5, sticky="w")

    folder_path_label = tk.Label(settings_window, text=path_label, font=("Helvetica", base_font_size))
    folder_path_label.grid(row=11, column=9, columnspan=4, padx=10, pady=5, sticky="w")

    choose_folder_button = create_styled_button(settings_window, text="Выбрать папку",
                                                command=(lambda: choose_folder(folder_path_label)), width=10, height=1,
                                                text_height=8)
    choose_folder_button.grid(row=11, column=8, columnspan=2, padx=10, pady=5, sticky="w")

    header_label_4 = tk.Label(settings_window, text="Словарь", font=("Helvetica", header_font_size))
    header_label_4.grid(row=0, column=9, columnspan=2, padx=10, pady=10)

    add_button = create_styled_button(settings_window, text="Добавить продукт",
                                      command=lambda: add_product(product_output), width=15, height=1, text_height=8)
    add_button.grid(row=6, column=8, padx=5, pady=5)

    delete_button = create_styled_button(settings_window, text="Удалить продукт",
                                         command=lambda: delete_product(product_output), width=15, height=1,
                                         text_height=8)
    delete_button.grid(row=6, column=10, padx=10, pady=10, sticky="e")
    text_width3 = int(0.4 * text_width2)
    left_border_label = tk.Label(settings_window, text="Левая граница:", font=("Helvetica", base_font_size))
    left_border_label.grid(padx=10, pady=5, row=8, column=8, sticky="w")
    left_border_entry = tk.Entry(settings_window, font=("Helvetica", base_font_size), width=text_width3)
    left_border_entry.grid(padx=10, pady=5, row=8, column=9)

    right_border_label = tk.Label(settings_window, text="Правая граница:", font=("Helvetica", base_font_size))
    right_border_label.grid(padx=10, pady=5, row=8, column=10, sticky="w")
    right_border_entry = tk.Entry(settings_window, font=("Helvetica", base_font_size), width=text_width3)
    right_border_entry.grid(padx=10, pady=5, row=8, column=11)

    left_range = settings_data.get("left", 30)
    right_range = settings_data.get("right", 60)
    left_border_entry.insert(0, str(left_range))
    right_border_entry.insert(0, str(right_range))

    camera_count_var = tk.StringVar()
    camera_count_var.set(str(settings_data["camera_count"]))
    camera_count_var.trace_add("write", update_camera_count)

    camera_g_count_var = camera_count_var
    camera_g_count_var.set(str(settings_data["camera_g_count"]))
    camera_g_count_var.trace_add("write", update_camera_g_count)

    scanner_count_var = camera_count_var
    scanner_count_var.set(str(settings_data["scanner_count"]))
    scanner_count_var.trace_add("write", update_scaner_count)

    frame1 = tk.Frame(settings_window, borderwidth=2, relief="groove")
    frame1.grid(row=0, column=0, columnspan=4, padx=10, pady=5, sticky="w")
    tk.Label(settings_window, text="Линии:", font=("Helvetica", base_font_size)).grid(padx=10, pady=5, row=7,
                                                                                      column=8, sticky="w")
    tk.OptionMenu(settings_window, camera_count_var, *range(1, 4)).grid(padx=10, pady=5, row=7, column=9, sticky="w")

    controller_count_var = camera_count_var
    controller_count_var.set(str(settings_data["controller_count"]))
    controller_count_var.trace_add("write", update_controller_count)

    local_ip_list = get_local_ips()

    # Если список пуст — добавляем placeholder
    if not local_ip_list:
        local_ip_list = ["Нет активных сетевых интерфейсов"]

    # Создаем Combobox для выбора IP
    selected_ip_var = tk.StringVar()
    ip_combobox_label = tk.Label(settings_window, text="IP для сервера:", font=("Helvetica", base_font_size))
    ip_combobox_label.grid(row=9, column=8, padx=10, pady=5, sticky="w")

    ip_combobox = ttk.Combobox(
        settings_window,
        textvariable=selected_ip_var,
        values=local_ip_list,
        state="readonly",
        width=text_width2
    )
    ip_combobox.grid(row=9, column=9, padx=10, pady=5)

    # Загружаем сохранённый IP из настроек
    saved_ip = settings_data.get("server_ip", "")
    if saved_ip and saved_ip in local_ip_list:
        ip_combobox.set(saved_ip)
    else:
        ip_combobox.set(local_ip_list[0] if local_ip_list else "Выберите IP")

    scanner_frames = []
    ip_entries = []
    port_entries = []
    ip_labels = []
    port_labels = []
    ip_entries_g = []
    port_entries_g = []
    ip_labels_g = []
    port_labels_g = []
    controller_ip_entries = []
    controller_port_entries = []
    controller_ip_labels = []
    controller_port_labels = []
    base_font_size = int(screen_height * 0.01)  # 1.5% от высоты экрана
    settings_button2 = create_styled_button(settings_window, text="Сохранить",
                                            command=lambda: save_settings(settings_window, camera_count_var,
                                                                          controller_count_var,
                                                                          camera_g_count_var, scanner_count_var,
                                                                          folder_path_label, ip_entries, port_entries,
                                                                          controller_ip_entries,
                                                                          controller_port_entries,
                                                                          ip_entries_g, port_entries_g,
                                                                          com_comboboxes_s,
                                                                          left_border_entry.get(),
                                                                          right_border_entry.get(), selected_ip_var),
                                            width=40, height=2)
    settings_button2.grid(row=15, column=10, columnspan=2, padx=10, pady=5, sticky='nsew')

    # Рассчитываем размеры шрифтов и виджетов
    button_width = int(screen_width * 0.03)
    button_height = int(screen_height * 0.004)

    update_camera_count()  # Вызываем функции для создания начальных виджетов
    update_controller_count()
    update_camera_g_count()
    update_scaner_count()

    delay_button = create_styled_button(settings_window, text="Настройка регистров", command=open_reg_window, height=1,
                                        width=20, text_height=10)
    delay_button.grid(row=13, column=8, columnspan=4, sticky="ew", pady=10, ipadx=20, ipady=10)

    aggr_button = create_styled_button(settings_window, text="Настройка агрегации", command=open_aggr_window, height=1,
                                       width=20, text_height=10)
    aggr_button.grid(row=12, column=8, columnspan=4, sticky="ew", pady=10, ipadx=20, ipady=10)

    settings_window.mainloop()


DEFAULT_REGISTER_VALUES = {
    "bottles": [900, 900, 900],
    "packs": [902, 902, 902],
    "pallets": [904, 904, 904],
    "total_packs": [934, 934, 934],
    "total_bottles": [936, 936, 936],
    "bottles_ser": [930, 930, 930],
    "new_pallet": [918, 918, 918],
    "new_file": [922, 922, 922],
    "green_column": [908, 908, 908],
    "red_column": [938, 938, 938],
    "group_cam": [912, 912, 912],
    "scanner": [914, 914, 914],
    "controller": [932, 932, 932],
    "pal_open": [906, 906, 906],
    "all_ser": [411, 411, 411],
    "suc_ser": [412, 412, 412],
    "def_ser": [413, 413, 413],
    "serv_work": [415, 415, 415]
}


def load_register_addresses():
    global register_addresses
    if os.path.exists("settings.json"):
        try:
            with open("settings.json", "r", encoding="utf-8") as f:
                settings_data = json.load(f)

            # Загружаем регистры
            for key in DEFAULT_REGISTER_VALUES:
                values = settings_data.get("registers", {}).get(key)
                if isinstance(values, list) and len(values) == 3:
                    register_addresses[key] = values
                else:
                    register_addresses[key] = DEFAULT_REGISTER_VALUES[key]
        except Exception as e:
            print(f"Ошибка при чтении settings.json: {e}")
            # Используем дефолты, если файл сломан
            register_addresses = DEFAULT_REGISTER_VALUES.copy()
    else:
        # Если файл не существует — используем дефолты
        register_addresses = DEFAULT_REGISTER_VALUES.copy()

    return register_addresses


register_addresses = {}


def setup_logging(log_output):
    date_log = get_current_date()
    logs_folder = "logs"  # Имя папки для логов
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)
    log_file_path = os.path.join(logs_folder, f"{date_log}.log")
    logging.basicConfig(
        filename=log_file_path,  # Указываем путь к файлу лога
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s")

    class TerminalHandler(logging.Handler):
        def __init__(self, log_output):
            super().__init__()
            if isinstance(log_output, list):
                self.log_output = log_output
            else:
                self.log_output = [log_output]

        def emit(self, record):
            log_entry = self.format(record)
            for output in self.log_output:
                append_log(output, log_entry, record.levelname)  # Передаём уровень логирования

    terminal_handler = TerminalHandler(log_output)
    terminal_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(terminal_handler)
    terminal_thread = threading.Thread(target=logging.shutdown, daemon=True, name="logging")
    threads_all.append(terminal_thread)
    terminal_thread.start()


exit_event = threading.Event()
sockets = []
sockets_g = []
sockets_s = []
sockets_controller = []


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
    """Возвращает список всех локальных IPv4-адресов"""
    ips = []
    for interface, addrs in psutil.net_if_addrs().items():
        for addr in addrs:
            if addr.family == socket.AF_INET and not addr.address.startswith("127."):
                ips.append(addr.address)
    return ips


def start_button_command(root, ip_entries, port_entries, ip_entries_g, port_entries_g, ip_entries_s, port_entries_s,
                         ip_entries_controllers, line, line_g,
                         folder_path_label, start_button, exit_event, com_ports):
    def start_server_async():
        try:
            start_server(ip_entries, port_entries, ip_entries_g, port_entries_g, ip_entries_s, port_entries_s,
                         ip_entries_controllers, line, line_g,
                         folder_path_label, start_button, exit_event, com_ports)
        except Exception as e:
            log_error_once(f"Ошибка при запуске сервера: {e}")

    # Запускаем сервер в отдельном потоке
    thread = threading.Thread(target=start_server_async)
    threads_all.append(thread)
    thread.start()


def start_server(ip_entries, port_entries, ip_entries_g, port_entries_g, ip_entries_s, port_entries_s,
                 ip_entries_controllers, line, line_g,
                 folder_path_label, start_button, exit_event, com_ports):
    settings_data = get_settings_data()
    local_ip = settings_data.get("server_ip", '')
    if local_ip == '':
        local_ip = get_local_ip()
    if not local_ip:
        log_error_once("Не удалось получить локальный IP-адрес")
        return
    # Запуск серверов для обычных камер
    for i in range(len(ip_entries)):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(10)
        sockets.append(server_socket)

    threads = []
    for server_socket, ip_entry, port_entry, line_num in zip(sockets, ip_entries, port_entries, line):
        thread = threading.Thread(target=run_server,
                                  name="run_server",
                                  args=(server_socket, port_entry, line_num, folder_path_label, exit_event, "default"))
        threads.append(thread)
        threads_all.append(thread)
        thread.start()

    # Запуск серверов для групповых камер (ip_entries_g)
    for i in range(len(ip_entries_g)):
        server_socket_g = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket_g.settimeout(10)
        sockets_g.append(server_socket_g)

    for server_socket_g, ip_entry_g, port_entry_g, line_num in zip(sockets_g, ip_entries_g, port_entries_g, line_g):
        thread = threading.Thread(target=run_server,
                                  name="run_server_grouped",
                                  args=(
                                      server_socket_g, port_entry_g, line_num, folder_path_label, exit_event,
                                      "grouped"))
        threads.append(thread)
        threads_all.append(thread)
        thread.start()

    for i in range(len(ip_entries_s)):
        server_socket_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket_s.settimeout(10)
        sockets_s.append(server_socket_s)

    for com_port, line_num in com_ports:
        baud_rate = 9600  # Установите нужную скорость передачи данных
        thread = threading.Thread(
            target=receive_data_and_save_upr,
            args=(com_port, folder_path_label, line_num, baud_rate, exit_event)
        )
        threads.append(thread)
        threads_all.append(thread)
        thread.start()

    # Запуск контроллеров
    for i in range(len(ip_entries_controllers)):
        server_socket_controller = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket_controller.settimeout(5)
        sockets_controller.append(server_socket_controller)

    start_button.config(state=tk.DISABLED)  # Отключаем кнопку "Старт"


def run_server(server_socket, port, line, folder_path_label, exit_event,
               server_type):
    try:
        # Проверяем, что порт является корректным числом
        try:
            port_int = int(port)
            if not (0 <= port_int <= 65535):
                raise ValueError("Порт должен быть в диапазоне от 0 до 65535")
        except ValueError as e:
            log_error_once(f"Некорректный порт: {e}")
            return

        # Получаем локальный IP-адрес
        settings_data = get_settings_data()
        local_ip = settings_data.get("server_ip", '')
        if local_ip == '':
            local_ip = get_local_ip()
        if not local_ip:
            log_error_once("Не удалось получить локальный IP-адрес")
            return

        # Основной цикл сервера
        while not exit_event.is_set():
            try:
                if server_type == "grouped":
                    server_socket_grooped = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    server_socket_grooped.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server_socket_grooped.settimeout(5)  # Устанавливаем тайм-аут для приема
                    server_socket_grooped.bind((local_ip, port_int))
                    server_socket_grooped.listen(5)  # Очередь подключений
                    sockets.append(server_socket_grooped)
                    while not exit_event.is_set():
                        try:
                            receive_data_and_save_grouped(server_socket_grooped, folder_path_label, line, exit_event)
                        except Exception as e:
                            log_error_once(f"Ошибка в цикле группового сервера: {e}")
                            break
                else:
                    client_socket = None
                    try:
                        # Логика для обычных камер
                        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client_socket.settimeout(10)  # Устанавливаем тайм-аут для подключения
                        client_socket = reconnect_to_camera(local_ip, port_int, exit_event)
                        sockets.append(client_socket)
                        receive_data_and_save(client_socket, folder_path_label, line,
                                              exit_event)
                    except Exception as e:
                        # Логируем другие ошибки
                        print(f"Ошибка подключения к серверу {local_ip}:{port_int}: {e}")
                        continue
                    finally:
                        # Закрываем сокет при выходе из блока
                        if client_socket:
                            client_socket.close()
                            print(f"Соединение с сервером {local_ip}:{port_int} закрыто.")
            except socket.timeout:
                continue
            except Exception as e:
                log_error_once(f"Ошибка в основном цикле сервера: {e}")
                break

    except Exception as e:
        log_error_once(f"Критическая ошибка запуска сервера камеры: {e}")
    finally:
        # Закрываем сокет и удаляем его из списка
        if server_socket:
            server_socket.close()
            if server_socket in sockets:
                sockets.remove(server_socket)
        log_info_once(f"Сервер камеры {server_type} остановлен.")


data_buffer = queue.Queue()
file_write_lock = threading.Lock()


def reconnect_to_camera(ip, port, exit_event, timeout=10):
    """Пытается подключиться к камере до тех пор, пока не станет доступной."""
    while not exit_event.is_set():
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(timeout)
            client_socket.connect((ip, int(port)))
            print(f"[OK] Восстановлено соединение с камерой {ip}:{port}")
            return client_socket
        except (socket.timeout, ConnectionRefusedError) as e:
            time.sleep(5)
        except Exception as e:
            log_error_once(f"Ошибка при подключении к камере {ip}:{port}: {e}")
            time.sleep(5)
    return None


def reconnect_client(server_socket, line, exit_event):
    while not exit_event.is_set():
        try:
            client_socket, addr = server_socket.accept()
            return client_socket
        except socket.timeout:
            continue
        except Exception as e:
            logging.error(f"[LINE {line + 1}] Ошибка при ожидании клиента: {e}")
            time.sleep(5)
    return None


def buffer_worker(folder_path_for_date, line, exit_event):
    """Функция для фоновой обработки данных из буфера."""
    while not exit_event.is_set():
        try:
            # Получаем данные из буфера
            data, line, product_name = data_buffer.get(timeout=1)  # Тайм-аут для проверки exit_event
            with line_datetime_lock:
                datetime_prefix = line_datetime.get(line, "unknown_datetime")
            file_path = os.path.join(folder_path_for_date,
                                     f"Stage0_{product_name}_{datetime_prefix}_line_{line + 1}.txt")

            # Проверяем, является ли код дубликатом
            if is_duplicate(data, file_path):
                continue  # Пропускаем запись дубликата

            # Используем блокировку для синхронизации записи в файл
            with file_write_lock:
                # Создание файла, если он не существует
                if not os.path.exists(file_path):
                    try:
                        with open(file_path, 'w') as file:
                            file.write("")  # Создаем пустой файл
                    except IOError as e:
                        log_error_once(f"Ошибка при создании файла: {e}")
                    log_info_once(f"Создан новый файл: Stage0_{product_name}_{datetime_prefix}_line_{line + 1}.txt")

                # Запись данных в файл
                try:
                    with open(file_path, 'a') as file:
                        append_to_widget(recorded_codes_text, line, data, recorded_codes_counters, file_path=file_path,
                                         use_file_counter=True)
                        file.write(data + '\n')
                        file.flush()  # Принудительная запись данных на диск
                        os.fsync(file.fileno())  # Синхронизация файловой системы
                except IOError as e:
                    log_error_once(f"Ошибка записи файла: {e}")

        except queue.Empty:
            continue  # Если буфер пуст, продолжаем цикл
        except Exception as e:
            log_error_once(f"Ошибка при обработке данных из буфера: {e}")


def receive_data_and_save(client_socket, folder_path_label, line, exit_event):
    try:
        # Загрузка product_mapping из файла Product_map.json
        try:
            with open('Product_map.json', 'r') as f:
                product_mapping = json.load(f)
        except FileNotFoundError:
            product_mapping = {}
            log_error_once("Файл Product_map.json не найден.")

        # Загрузка настроек
        settings_data = get_settings_data()
        left_range = settings_data.get("left", 0)
        right_range = settings_data.get("right", float('inf'))

        # Создание папки для текущей даты
        current_date = get_current_date()
        folder_path_for_date = os.path.join(folder_path_label, current_date)
        os.makedirs(folder_path_for_date, exist_ok=True)

        # Запуск фонового потока для обработки буфера
        buffer_thread = threading.Thread(
            target=buffer_worker,
            args=(folder_path_for_date, line, exit_event)
        )
        buffer_thread.start()
        threads_all.append(buffer_thread)

        # Основной цикл обработки данных
        while not exit_event.is_set():
            try:
                # Чтение данных от клиента (без декодирования)
                client_socket.settimeout(5.0)
                try:
                    raw_data = client_socket.recv(2048)
                except socket.timeout:
                    continue
                if not raw_data:
                    break  # Если данные не пришли, завершаем цикл

                # Декодируем данные с использованием определенной кодировки
                data = raw_data.decode('utf-8', errors='ignore').strip()

                if not data:
                    break  # Если данные не пришли, завершаем цикл

                # Извлечение типа продукта из данных
                if len(data) >= 16:
                    product_type = data[2:16].strip()
                else:
                    log_error_once("Некорректная длина данных")
                    continue
                product_name = product_mapping.get(product_type, product_type)

                # Формируем имя файла с датой из настроек
                with line_datetime_lock:
                    datetime_prefix = line_datetime.get(line, "unknown_datetime")
                file_path = os.path.join(folder_path_for_date,
                                         f"Stage0_{product_name}_{datetime_prefix}_line_{line + 1}.txt")
                append_to_widget(all_codes_text, line, data, all_codes_counters, file_path=file_path,
                                 use_file_counter=False)
                if not os.path.exists(file_path):
                    with open(file_path, 'w'):
                        pass
                    log_info_once(f"Создан новый файл: Stage0_{product_name}_{datetime_prefix}_line_{line + 1}.txt")

                # Проверка данных на соответствие диапазону и отсутствие дубликатов
                if left_range <= len(data) <= right_range and not is_duplicate(data, file_path):
                    # Помещаем данные в буфер
                    data_buffer.put((data, line, product_name))
                else:
                    continue
            except socket.timeout:
                # Если тайм-аут, продолжаем цикл
                continue
            except ConnectionResetError as e:
                # Если соединение разорвано, завершаем цикл
                print(f"Разрыв соединения с камерой линии {line + 1}")
                break
            except Exception as e:
                # Логирование других ошибок
                log_error_once(f"Ошибка получения кодов линии {line + 1}: {e}")
                break
    except socket.timeout:
        pass
    except Exception as e:
        # Логирование критических ошибок
        log_error_once(f"Критическая ошибка в receive_data_and_save: {e}")


# Глобальный буфер для хранения всех кодов
buffer_codes_by_line = {}
buffer_lock = threading.Lock()  # Для потокобезопасности

# Словарь для хранения текущих палетных кодов для каждой линии
current_pallet_codes_by_line = {}
pallet_lock = threading.Lock()


def receive_data_and_save_upr(com_port, folder_path_label, line_num, baud_rate, exit_event):
    global current_pallet_codes_by_line  # Используем глобальный словарь
    ip_entries_controllers = []
    port_entries_controllers = []
    settings_data = get_settings_data()
    controllers = settings_data.get("controllers", [])
    for controller in controllers:
        ip_entries_controllers.append(controller["ip"])
        port_entries_controllers.append(controller["port"])
    connection = ModbusConnection(ip=ip_entries_controllers[line_num], port=502)

    try:
        while not exit_event.is_set():  # Основной цикл работы программы
            try:
                # Попытка открыть COM-порт
                ser = serial.Serial(com_port, baud_rate, timeout=0.1)
                log_info_once(f"COM-порт {com_port} успешно открыт.")

                # Цикл обработки данных из COM-порта
                while not exit_event.is_set():
                    try:
                        # Прием данных от сканера
                        raw_data = ser.readline().decode('utf-8').strip()
                        if not raw_data:
                            continue  # Пропускаем пустые строки

                        append_to_widget(all_grouped_codes_text, line_num, "Палетный " + raw_data,
                                         all_palet_codes_counters,
                                         file_path=0,
                                         use_file_counter=False)

                        current_date = get_current_date()
                        folder_path_for_date = os.path.join(folder_path_label, current_date)
                        os.makedirs(folder_path_for_date, exist_ok=True)
                        time_codes_file_path = os.path.join(folder_path_for_date,
                                                            "timecodes")
                        os.makedirs(time_codes_file_path, exist_ok=True)
                        timestamps_file_path = os.path.join(time_codes_file_path,
                                                            f"time_codes_line_{line_num + 1}.txt")
                        file_existed = os.path.exists(timestamps_file_path)
                        current_time = datetime.datetime.now()
                        if not file_existed:
                            log_info_once(f"Создан новый файл: time_codes_line_{line_num + 1}.txt")
                        try:
                            with open(timestamps_file_path, "a", encoding="utf-8") as f:
                                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
                                f.write(f"Палетный  {timestamp}  {raw_data}\n")
                        except IOError as e:
                            log_error_once(f"Ошибка записи файла: {e}")
                        with pallet_lock:  # Защита доступа к глобальному словарю
                            current_pallet_code = current_pallet_codes_by_line.get(line_num, "")

                        if raw_data and current_pallet_code and raw_data == current_pallet_code:
                            # Если повторно сканируется тот же палетный код
                            log_info_once(
                                f"Закрыт палет: {raw_data}. Линия {line_num + 1}.")
                            with pallet_lock:
                                current_pallet_codes_by_line[line_num] = ''  # Сбрасываем палетный код
                        else:
                            if current_pallet_code:  # Проверяем, что текущий паллетный код не пустой
                                log_info_once(
                                    f"Закрыт палет: {current_pallet_code}. Линия {line_num + 1}")
                            log_info_once(
                                f"Открыт палет: {raw_data}. Линия {line_num + 1}")
                            with pallet_lock:
                                current_pallet_codes_by_line[line_num] = raw_data  # Записываем новый палетный код
                            error_logged[line_num] = False

                    except serial.SerialTimeoutException:
                        continue
                    except Exception as e:
                        log_error_once(f"Ошибка при обработке данных с COM-порта {com_port}: {e}")
                        break

            except serial.SerialException as e:
                if not error_logged.get(line_num, False):
                    log_error_once(f"Не удалось открыть COM-порт {com_port}: {e}")
                    error_logged[line_num] = True
                time.sleep(5)  # Задержка перед повторной попыткой
                continue  # Переходим к следующей итерации для повторной попытки открытия порта

            finally:
                # Закрываем порт, если он был открыт
                if 'ser' in locals() and ser.is_open:
                    ser.close()
                    log_info_once(f"COM-порт {com_port} закрыт.")

    except Exception as e:
        log_error_once(f"Критическая ошибка в работе сервера ручного сканера: {e}")
    finally:
        # Логируем отключение сканера
        log_info_once(f"Ручной сканер линии {line_num + 1} отключен.")


error_logged = {}
last_connection_time_by_line = {}
active_buffer_threads = {}
modbus_connection_lock = threading.Lock()


def receive_data_and_save_grouped(server_socket, folder_path_label, line, exit_event):
    """
    Функция для приема данных от клиентов (камер) и сохранения их в буфере.
    Обрабатывает групповые коды и паллетные данные, обеспечивая потокобезопасность.
    """
    client_socket = None
    try:
        # Принимаем подключение от клиента
        current_time = datetime.datetime.now()
        last_connection_time = last_connection_time_by_line.get(line, current_time - datetime.timedelta(seconds=60))
        client_socket = reconnect_client(server_socket, line, exit_event)
        if not client_socket:
            return  # или continue, в зависимости от контекста

        settings_data = get_settings_data()
        left_range = settings_data.get("left", 0)
        right_range = settings_data.get("right", float('inf'))
        # Проверка времени последнего подключения
        if (current_time - last_connection_time).total_seconds() >= 30:
            last_connection_time_by_line[line] = current_time

        # Настройка клиентского сокета
        client_socket.settimeout(5)
        sockets.append(client_socket)

        # Создание папки для текущей даты
        current_date = get_current_date()
        folder_path_for_date = os.path.join(folder_path_label, current_date)
        os.makedirs(folder_path_for_date, exist_ok=True)

        # Запуск фонового потока для обработки буфера
        if line not in active_buffer_threads or not active_buffer_threads[line].is_alive():
            buffer_thread = threading.Thread(
                target=buffer_worker_grooped,
                args=(folder_path_for_date, line, exit_event)
            )
            buffer_thread.start()
            threads_all.append(buffer_thread)
            active_buffer_threads[line] = buffer_thread

        # Основной цикл обработки данных
        modbus_connections = get_modbus_connections()
        modbus_connectionss = {i: connection for i, connection in enumerate(modbus_connections)}
        if line in modbus_connectionss:
            connection = modbus_connections[line]
        settings_data = get_settings_data()
        use_serial = settings_data.get("use_serial", False)

        while not exit_event.is_set():
            try:
                raw_data = client_socket.recv(4096)
                data = raw_data.decode('utf-8', errors='ignore').strip()
                if not data:
                    break  # Если данные не пришли, завершаем цикл
                if connection is None:
                    pass
                else:
                    if connection.connected and not use_serial:
                        with write_lock:
                            pallet_code_register_value = connection.read_reg(register_addresses["new_pallet"][line])
                        if pallet_code_register_value == 1:
                            log_info_once(f"Нажата кнопка Новый палет линия {line + 1}")
                            handle_pallet_code(client_socket, line, folder_path_for_date, connection)
                            with write_lock:
                                connection.write_reg(register_addresses["new_pallet"][line], 0)
                    # Чтение данных от клиента
                    else:
                        return

                # Добавление данных в буфер
                if left_range <= len(data) <= right_range:
                    # Помещаем данные в буфер
                    with buffer_codes_lock:
                        buffer_codes_by_line.setdefault(line, []).append(data)

                # Обновление виджета и времени последнего обновления
                with widget_lock:
                    append_to_widget(
                        all_grouped_codes_text,
                        line,
                        data,
                        all_grouped_codes_counters,
                        file_path=0,
                        use_file_counter=False
                    )
                last_update_time_by_line[line] = datetime.datetime.now()

            except socket.timeout:
                continue
            except (ConnectionResetError, UnicodeDecodeError) as e:
                log_error_once(f"Ошибка при работе с клиентом линии {line + 1}: {e}")
                break
            except Exception as e:
                log_error_once(f"Неожиданная ошибка линии {line + 1}: {e}")
    except socket.timeout:
        pass
    except Exception as e:
        log_error_once(f"Критическая ошибка в receive_data_and_save_grouped: {e}")
    finally:
        # Закрытие клиентского сокета
        if client_socket:
            client_socket.close()
            if client_socket in sockets:
                sockets.remove(client_socket)


def handle_pallet_code(client_socket, line, folder_path_for_date, connection):
    """
    Обрабатывает паллетный код: считывает данные, записывает их в файл и обновляет глобальные переменные.
    """
    try:
        raw_data = client_socket.recv(4096).decode('utf-8')
        if raw_data:
            # Логирование и добавление данных в виджет
            append_to_widget(
                all_grouped_codes_text,
                line,
                f"Палетный {raw_data}",
                all_palet_codes_counters,
                file_path=0,
                use_file_counter=False
            )

            # Запись данных в файл
            time_codes_file_path = os.path.join(folder_path_for_date, "timecodes")
            os.makedirs(time_codes_file_path, exist_ok=True)
            timestamps_file_path = os.path.join(time_codes_file_path, f"time_codes_line_{line + 1}.txt")
            try:
                with open(timestamps_file_path, "a", encoding="utf-8") as f:
                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"Палетный  {timestamp}  {raw_data}\n")
            except IOError as e:
                log_error_once(f"Ошибка записи файла: {e}")

            # Обновление текущего паллетного кода
            with pallet_lock:
                current_pallet_code = current_pallet_codes_by_line.get(line, "")
            if raw_data and current_pallet_code and raw_data == current_pallet_code:
                log_info_once(f"Закрыт палет: {raw_data}. Линия {line + 1}.")
                with pallet_lock:
                    current_pallet_codes_by_line[line] = ""
            else:

                if current_pallet_code:
                    log_info_once(f"Закрыт палет: {current_pallet_code}. Линия {line + 1}")
                log_info_once(f"Открыт палет: {raw_data}. Линия {line + 1}")
                with pallet_lock:
                    current_pallet_codes_by_line[line] = raw_data

    except Exception as e:
        log_error_once(f"Ошибка обработки паллетного кода линии {line + 1}: {e}")
    except socket.timeout:
        pass
    finally:
        # Закрытие клиентского сокета
        if client_socket:
            try:
                client_socket.close()
            except Exception as close_error:
                log_error_once(f"Ошибка при закрытии сокета в handle_pallet_code: {close_error}")


widget_lock = threading.Lock()


def clear_widget(widget, line):
    """
    Очищает строки в виджете, связанные с указанной линией.
    Удаляет строки целиком, если они начинаются с "Линия X:", "Палетный" или находятся в блоке строк для указанной линии.
    """
    try:
        if not widget or not isinstance(widget, tk.Text):
            log_error_once("Ошибка в clear_widget: виджет не существует или имеет неверный тип.")
            return

        # Разблокируем виджет (если используется блокировка)
        widget.config(state=tk.NORMAL)

        # Получаем весь текст из виджета
        all_text = widget.get("1.0", "end").strip()  # Убираем лишние пробелы и пустые строки в конце
        lines_to_keep = []
        skip_block = False  # Флаг для пропуска блока строк, связанных с указанной линией

        # Разделяем текст на строки
        for line_content in all_text.split("\n"):
            line_content = line_content.strip()  # Убираем пробелы в начале и конце строки

            # Пропускаем пустые строки
            if not line_content:
                continue

            # Проверяем, начинается ли строка с "Линия"
            if line_content.startswith("Линия"):
                try:
                    # Извлекаем номер линии из строки
                    file_line_number = int(line_content.split("Линия")[1].split(":")[0].strip())
                    if file_line_number == line + 1:
                        # Начинаем пропускать блок строк для указанной линии
                        skip_block = True
                    else:
                        # Заканчиваем пропуск блока строк
                        skip_block = False
                        lines_to_keep.append(line_content)
                except ValueError:
                    # Если формат строки некорректен, логируем ошибку и оставляем строку
                    log_error_once(f"Некорректный формат строки в виджете: {line_content}")
                    lines_to_keep.append(line_content)
            elif line_content.startswith("Палетный"):
                # Пропускаем строки, начинающиеся с "Палетный"
                continue
            else:
                # Если строка не начинается с "Линия" или "Палетный", проверяем флаг skip_block
                if skip_block:
                    # Пропускаем строку, если она находится в блоке для указанной линии
                    continue
                else:
                    # Оставляем строку, если она не связана с указанной линией
                    lines_to_keep.append(line_content)

        # Очищаем виджет и записываем обратно только нужные строки
        widget.delete("1.0", "end")
        widget.insert("1.0", "\n".join(lines_to_keep) + "\n")  # Добавляем символ новой строки в конецg

        # Блокируем виджет снова (если требуется)
        widget.config(state=tk.DISABLED)

    except Exception as e:
        log_error_once(f"Ошибка в clear_widget: {e}")


last_update_time_by_line = {}  # Время последнего обновления буфера для каждой линии


def buffer_worker_grooped(folder_path_label, line, exit_event):
    """
    Рабочий поток для обработки буфера кодов.
    :param folder_path_label: Путь к папке для сохранения файлов.
    :param line: Номер линии.
    :param exit_event: Событие для выхода из потока.
    """
    try:
        # Загрузка product_mapping из файла Product_map.json (выполняется один раз)
        try:
            with open('Product_map.json', 'r') as f:
                product_mapping = json.load(f)
        except FileNotFoundError:
            product_mapping = {}
            log_error_once("Файл Product_map.json не найден. Используется пустой словарь.")

        # Основной цикл обработки буфера
        while not exit_event.is_set():
            try:
                # Копируем данные из буфера
                with buffer_codes_lock:
                    buffer = list(buffer_codes_by_line.get(line, []))

                if buffer:
                    # Обрабатываем буфер в потоке
                    thread = threading.Thread(
                        target=process_buffer_codes,
                        args=(line, folder_path_label, product_mapping),
                        daemon=True
                    )
                    thread.start()
                    threads_all.append(thread)

                time.sleep(0.1)  # Не нагружаем процессор
            except Exception as e:
                log_error_once(f"Критическая ошибка в потоке линии {line + 1}: {e}")

    except Exception as e:
        log_error_once(f"Критическая ошибка в buffer_worker для линии {line + 1}: {e}")


def is_gs1_datamatrix(code):
    """
    Проверяет, является ли код GS1 DataMatrix.
    :param code: Код для проверки.
    :return: True, если код соответствует GS1 DataMatrix, иначе False.
    """
    settings_data = get_settings_data()
    len_gs1 = settings_data.get("len_gs1", 18)  # Значение по умолчанию 18, если ключ отсутствует

    if len(code) == len_gs1:
        return True
    return False


# Словарь для хранения времени поступления первого кода в буфер для каждой линии
first_code_time_by_line = {}
first_code_lock = threading.Lock()


def get_bottles_per_pack_for_lines(settings_data, lines):
    """
    Получает количество бутылок в пакете для нескольких линий с учетом смещения.
    :param settings_data: Данные настроек.
    :param lines: Список номеров линий (чисел).
    :return: Список количества бутылок для каждой линии с учетом смещения.
    """
    # Проверяем, что lines — это список
    if not isinstance(lines, list):
        raise ValueError("Параметр lines должен быть списком.")

    # Для каждой линии получаем количество бутылок с учетом смещения
    bottles_per_pack_list = []
    for line in lines:
        # Смещаем номер линии на 1 вправо
        shifted_line = line + 1

        # Используем существующую функцию для одной линии
        bottles_per_pack = get_bottles_per_pack_for_line(settings_data, shifted_line)
        bottles_per_pack_list.append(bottles_per_pack)

    return bottles_per_pack_list


def get_bottles_per_pack_for_line(settings_data, line):
    """
    Получает количество бутылок в пакете для указанной линии.
    :param settings_data: Данные настроек.
    :param line: Номер линии (число или список с одним элементом).
    :return: Количество бутылок в пакете для линии (по умолчанию 6).
    """
    # Проверяем, что line — это число
    if isinstance(line, list) and len(line) == 1:
        line = line[0]  # Извлекаем первый элемент из списка
    if not isinstance(line, int):
        raise ValueError("Номер линии должен быть числом.")

    # Извлекаем данные о количестве бутылок для всех линий
    bottles_per_pack_by_line = settings_data.get("bottles_per_pack_by_line", {})

    # Проверяем, что данные имеют правильный формат
    if not isinstance(bottles_per_pack_by_line, dict):
        raise ValueError("bottles_per_pack_by_line должен быть словарем.")

    # Преобразуем номер линии в строку для поиска
    line_key = str(line)

    # Ищем значение для указанной линии
    bottles_per_pack = bottles_per_pack_by_line.get(line_key, 6)

    return int(bottles_per_pack)


def get_delays_per_line(settings_data, line):
    """
    Получает количество бутылок в пакете для указанной линии.
    :param settings_data: Данные настроек.
    :param line: Номер линии.
    :return: Количество бутылок в пакете для линии (по умолчанию 6).
    """
    delay_by_line = settings_data.get("delays_by_line", {})
    return int(delay_by_line.get(str(line), 6))  # Значение по умолчанию: 6


def normalize_code(code):
    """
    Нормализует код, удаляя всё, начиная с символа \x1d (групповой разделитель).
    :param code: Исходный код.
    :return: Нормализованный код.
    """
    separator = "\x1d"
    if separator in code:
        return code.split(separator)[0]
    return code


line_datetime = {}  # Словарь для хранения дат для каждой линии
line_datetime_lock = threading.Lock()  # Блокировка для синхронизации доступа к словарю
last_modified_time = None


def load_line_datetime():
    """
    Загружает даты для всех линий из файла line_datetimes.json
    и сохраняет их в словарь line_datetime.
    """
    global line_datetime, last_modified_time
    try:
        # Получаем время последнего изменения файла
        current_mtime = os.path.getmtime('line_datetimes.json')
        if last_modified_time is None or current_mtime != last_modified_time:
            last_modified_time = current_mtime  # Обновляем время последнего изменения
            with open('line_datetimes.json', 'r', encoding='utf-8') as f:
                datetime_dict = json.load(f)
            with line_datetime_lock:
                line_datetime.clear()  # Очищаем текущий словарь
            for line, datetime_value in datetime_dict.items():
                try:
                    line_number = int(line)  # Преобразуем ключ (номер линии) в целое число
                    with line_datetime_lock:
                        line_datetime[line_number] = datetime_value
                except ValueError:
                    print(f"Некорректный ключ в поле datetime: {line}")
    except FileNotFoundError:
        print("Файл line_datetimes.json не найден. Создан новый файл с настройками.")
        with line_datetime_lock:
            line_datetime = {}
        # Создаём новый файл с пустыми данными
        with open('line_datetimes.json', 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=4)
    except json.JSONDecodeError:
        print("Ошибка чтения файла line_datetimes.json. Проверьте его формат.")


def update_line_datetime_in_file(line, new_datetime):
    """
    Обновляет только поле datetime для указанной линии в файле line_datetimes.json.
    :param line: Номер линии.
    :param new_datetime: Новое значение даты и времени.
    """
    try:
        # Загружаем текущие настройки из файла
        try:
            with open('line_datetimes.json', 'r', encoding='utf-8') as f:
                datetime_dict = json.load(f)
        except FileNotFoundError:
            datetime_dict = {}

        # Обновляем значение для указанной линии
        datetime_dict[str(line)] = new_datetime

        # Сохраняем обновлённые данные обратно в файл
        with open('line_datetimes.json', 'w', encoding='utf-8') as f:
            json.dump(datetime_dict, f, ensure_ascii=False, indent=4)

        load_line_datetime()

        log_info_once(f"Дата для линии {line + 1} успешно обновлена.")
    except Exception as e:
        print(f"Ошибка при обновлении даты для линии {line}: {e}")


def check_and_update_line_datetime(line_num):
    """
    Проверяет значение регистра 1022 (или 1022 + 100 для линии 2) и обновляет дату линии,
    если значение регистра равно 1.
    :param line_num: Номер линии.
    """
    modbus_connections = get_modbus_connections()
    modbus_connectionss = {i: connection for i, connection in enumerate(modbus_connections)}
    if line_num in modbus_connectionss:
        connection = modbus_connections[line_num]
        if connection is None:
            return
        else:
            if not connection.connected:
                print(f"Контроллер для линии {line_num + 1} не подключен.")
                return
            try:
                with write_lock:
                    if connection is not None:
                        try:
                            trigger_register_value = connection.read_reg(register_addresses["new_file"][line_num])
                        except Exception as read_error:
                            print(
                                f"Ошибка чтения регистра {register_addresses["new_file"][line_num]} у контроллера {connection.address}: {read_error}")
                            return
                    else:
                        return
                # Если значение регистра равно 1, обновляем дату и сбрасываем регистр
                if trigger_register_value == 1:
                    new_datetime = datetime.datetime.now().strftime("%H-%M-%S")
                    update_line_datetime_in_file(line_num, new_datetime)  # Обновляем дату для линии
                    try:
                        with write_lock:
                            connection.write_reg(register_addresses["new_file"][line_num], 0)  # Сбрасываем регистр
                    except Exception as e:
                        print(
                            f"Ошибка записи в регистр {register_addresses["new_file"][line_num]} у контроллера {connection.address}: {e}")

            except Exception as e:
                print(f"Ошибка при проверке и обновлении даты для линии {line_num + 1}: {e}")


def process_buffer_codes(line, folder_path_label, product_mapping):
    """
    Обрабатывает коды из буфера для конкретной линии, связывает GS1-коды с обычными кодами и записывает в файл.
    Ожидает количество кодов, указанное в bottles_per_pack, затем обрабатывает их и очищает буфер.
    Запись в файл осуществляется построчно в формате: обычный_код GS1_код.
    """
    try:
        settings_data = get_settings_data()
        use_serial = settings_data.get("use_serial", False)

        if use_serial:
            # Режим сериализации
            process_buffer_codes_serial(line, folder_path_label, product_mapping)
        else:
            # Режим групповой агрегации
            process_buffer_codes_grouped(line, folder_path_label, product_mapping)
    except Exception as e:
        print(f"Критическая ошибка обработки буфера кодов: {e}")


buffer_codes_lock = threading.Lock()


def process_buffer_codes_grouped(line, folder_path_label, product_mapping):
    modbus_connections = get_modbus_connections()
    settings_data = get_settings_data()
    len_gs1 = settings_data.get("len_gs1", 18)
    modbus_connectionss = {i: connection for i, connection in enumerate(modbus_connections)}
    if line not in modbus_connectionss:
        return
    with first_code_lock:
        first_code_time_by_line.pop(line, None)
    connection = modbus_connectionss[line]

    try:
        with buffer_codes_lock:
            buffer_codes = buffer_codes_by_line.get(line, [])

        for code in buffer_codes:
            if len(code.strip()) < len_gs1:
                with buffer_codes_lock:
                    buffer_codes_by_line[line] = []
                    print("Очистка буфера 1")
                with widget_lock:
                    clear_widget(all_grouped_codes_text, line)
                with first_code_lock:
                    first_code_time_by_line.pop(line, None)
                log_error_once(f"В буфере найден короткий код: '{code}'. Буфер очищен.")
                return
        with first_code_lock:
            if buffer_codes and line not in first_code_time_by_line:
                first_code_time_by_line[line] = datetime.datetime.now()

        with line_datetime_lock:
            datetime_prefix = line_datetime.get(line, "unknown_datetime")

        current_time = datetime.datetime.now()
        with first_code_lock:
            first_code_time = first_code_time_by_line.get(line, current_time)

        settings_data = get_settings_data()
        timeout_seconds = get_delays_per_line(settings_data, line + 1)
        bottles_per_pack = get_bottles_per_pack_for_line(settings_data, line + 1)

        if current_time - first_code_time > datetime.timedelta(seconds=timeout_seconds):
            if len(buffer_codes) == 0:
                with first_code_lock:
                    first_code_time_by_line.pop(line, None)
                return
            else:
                with buffer_codes_lock:
                    buffer_codes_by_line[line] = []
                    print("Очистка буфера 2")
                with widget_lock:
                    clear_widget(all_grouped_codes_text, line)
                red_column(line)
                return

        unique_codes = set(buffer_codes)
        if len(unique_codes) < bottles_per_pack + 1:
            return

        gs1_candidates = [code for code in buffer_codes if is_gs1_datamatrix(code)]

        if not gs1_candidates:
            # Нет ни одного GS1-кода → очищаем буфер
            with buffer_codes_lock:
                buffer_codes_by_line[line] = []
                print("Очистка буфера 3")
            with widget_lock:
                clear_widget(all_grouped_codes_text, line)
            red_column(line)
            with first_code_lock:
                first_code_time_by_line.pop(line, None)
            log_error_once(f"Не найдено GS1-кодов для линии {line + 1}")
            return

        if len(gs1_candidates) > 1:
            log_error_once(f"Обнаружено более одного GS1-кода на линии {line + 1}: {gs1_candidates}")
            with buffer_codes_lock:
                buffer_codes_by_line[line] = []
                print("Очистка буфера 3")
            with widget_lock:
                clear_widget(all_grouped_codes_text, line)
            red_column(line)
            with first_code_lock:
                first_code_time_by_line.pop(line, None)
            return

        # Используем первый найденный GS1-код
        gs1_code = gs1_candidates[0]

        # Удаляем все GS1-коды из буфера
        grouped_codes = [code.strip() for code in buffer_codes if not is_gs1_datamatrix(code)]
        grouped_codes = [code for code in grouped_codes if code]  # Убираем пустые

        # Получаем уникальные с сохранением порядка
        seen = set()
        unique_grouped_codes = []
        for code in grouped_codes:
            if code not in seen:
                seen.add(code)
                unique_grouped_codes.append(code)

        # Проверяем количество уникальных кодов
        if len(unique_grouped_codes) < bottles_per_pack:
            log_error_once(
                f"Недостаточно уникальных кодов: требуется {bottles_per_pack}, найдено {len(unique_grouped_codes)}")
            with buffer_codes_lock:
                buffer_codes_by_line[line] = []
                print("Очистка буфера 5")
            with widget_lock:
                clear_widget(all_grouped_codes_text, line)
            with first_code_lock:
                first_code_time_by_line.pop(line, None)
            return

        # Обрезаем до нужного количества
        grouped_codes_to_write = grouped_codes[:bottles_per_pack]

        with buffer_codes_lock:
            buffer_codes_by_line[line] = []
            print("Очистка буфера 5")
        with widget_lock:
            clear_widget(all_grouped_codes_text, line)
        with first_code_lock:
            first_code_time_by_line.pop(line, None)

        files_to_check_stage1 = [f for f in os.listdir(str(folder_path_label)) if
                                 "Stage1" in f and f.endswith(".txt") and datetime_prefix in f]

        files_to_check_stage2 = [f for f in os.listdir(str(folder_path_label)) if
                                 "Stage2" in f and f.endswith(".txt") and datetime_prefix in f]

        # Флаг для определения, нужно ли удалять старую пачку
        delete_old_pack = False

        # Список для хранения групповых кодов, которые нужно удалить из Stage2
        gs1_codes_to_delete = set()

        # Проверяем наличие совпадений в Stage1
        for file_name in files_to_check_stage1:
            file_path = os.path.join(str(folder_path_label), file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                for line_content in f:
                    parts = line_content.strip().split("\t", 1)
                    if len(parts) == 2:
                        individual_code, existing_gs1_code = parts[0].strip(), parts[1].strip()

                        # Если найден хотя бы один индивидуальный код или GS1_CODE, помечаем на удаление
                        if individual_code in grouped_codes_to_write or existing_gs1_code == gs1_code:
                            delete_old_pack = True
                            gs1_codes_to_delete.add(existing_gs1_code)  # Добавляем GS1_CODE для удаления из Stage2
                            log_info_once(f"Найдено совпадение в файле Stage1: {file_name}. Удаляем старую пачку.")
                            break
                if delete_old_pack:
                    break

        # Если найдено совпадение, удаляем старые строки из Stage1 и Stage2
        if delete_old_pack:
            for file_name in files_to_check_stage1 + files_to_check_stage2:
                file_path = os.path.join(str(folder_path_label), file_name)
                lines_to_keep = []

                with open(file_path, "r", encoding="utf-8") as f:
                    for line_content in f:
                        # Удаляем лишние пробелы и проверяем, не пустая ли строка
                        stripped_line = line_content.strip()
                        if not stripped_line:
                            continue  # Пропускаем пустые строки

                        # Определяем групповой код в зависимости от формата файла
                        if "Stage1" in file_name:
                            # Теперь разделитель — таб
                            parts = stripped_line.split("\t", 1)
                            existing_gs1_code = parts[1].strip() if len(parts) == 2 else None
                        elif "Stage2" in file_name:
                            existing_gs1_code = parts[0].strip()

                        # Проверяем, нужно ли удалить строку
                        if existing_gs1_code in gs1_codes_to_delete:
                            log_info_once(
                                f"Удаляем строку с групповым кодом: {existing_gs1_code} из файла: {file_name}")
                            continue

                        # Сохраняем строки, которые не связаны с текущими групповыми кодами
                        lines_to_keep.append(line_content)

                # Перезаписываем файл, исключая старые строки
                try:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.writelines(lines_to_keep)
                except IOError as e:
                    log_error_once(f"Ошибка записи файла: {e}")

        # Записываем новую пачку
        if len(grouped_codes_to_write[0]) >= 16:
            product_type = grouped_codes_to_write[0][2:16].strip()
        else:
            log_error_once("Некорректная длина данных")
            return

        product_name = product_mapping.get(product_type, f"({str(product_type)})")

        save_path_stage1 = os.path.join(str(folder_path_label),
                                        f"Stage1_{product_name}_{datetime_prefix}_line_{line + 1}.txt")
        os.makedirs(os.path.dirname(save_path_stage1), exist_ok=True)

        if not os.path.exists(save_path_stage1):
            log_info_once(f"Создан новый файл: Stage1_{product_name}_{datetime_prefix}_line_{line + 1}.txt")

        try:
            with open(save_path_stage1, "a", encoding="utf-8") as f:
                for grouped_code in grouped_codes_to_write:
                    f.write(f"{grouped_code}\t{gs1_code}\n")
                green_column(line)
        except IOError as e:
            log_error_once(f"Ошибка записи файла: {e}")

        for grouped_code in grouped_codes_to_write:
            append_to_widget(recorded_grouped_codes_text, line, f"{grouped_code}  {gs1_code}",
                             counters=recorded_grouped_codes_counters1, file_path=0, use_file_counter=False)

        # Логируем запись новой пачки
        log_info_once(f"Новая группа кодов с GS1_CODE: {gs1_code} успешно записана.")

        time_codes_file_path = os.path.join(folder_path_label,
                                            "timecodes")
        os.makedirs(time_codes_file_path, exist_ok=True)
        timestamps_file_path = os.path.join(time_codes_file_path, f"time_codes_line_{line + 1}.txt")
        if not os.path.exists(timestamps_file_path):
            log_info_once(f"Создан новый файл: time_codes_line_{line + 1}.txt")
        try:
            with open(timestamps_file_path, "a", encoding="utf-8") as f:
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"Групповой {timestamp}  {gs1_code}\n")
        except IOError as e:
            log_error_once(f"Ошибка записи файла: {e}")

        link_pallet_and_grouped_codes(gs1_code, product_name, line, folder_path_label)

    except Exception as e:
        log_error_once(f"Ошибка обработки буфера кодов: {e}")
    finally:
        with buffer_codes_lock:
            buffer_codes_by_line[line] = []
        with first_code_lock:
            first_code_time_by_line.pop(line, None)
        with widget_lock:
            clear_widget(all_grouped_codes_text, line)


# Глобальная переменная для отслеживания времени последнего вызова
last_run_time_by_line = {}


def process_buffer_codes_serial(line, folder_path_label, product_mapping):
    try:
        settings_data = get_settings_data()
        # Получаем коды для текущей линии
        with buffer_codes_lock:
            buffer_codes = buffer_codes_by_line.get(line, [])
        if not buffer_codes:
            return  # Если буфер пуст, выходим

        # Получаем настройки
        settings_data = get_settings_data()
        bottles_per_pack = get_bottles_per_pack_for_line(settings_data, line + 1)
        delay_seconds = get_delays_per_line(settings_data, line + 1)  # Задержка для линии

        # Инициализируем временную метку первого кода
        current_time = datetime.datetime.now()
        with first_code_lock:
            if buffer_codes and line not in first_code_time_by_line:
                first_code_time_by_line[line] = current_time

        # Получаем время поступления первого кода в буфер
        with first_code_lock:
            first_code_time = first_code_time_by_line.get(line, current_time)

        # Удаляем дубликаты внутри буфера
        unique_buffer_codes = list(set(buffer_codes))

        # Проверяем, истекло ли время ожидания
        if current_time - first_code_time > datetime.timedelta(seconds=delay_seconds):
            if len(unique_buffer_codes) < bottles_per_pack:
                # Очищаем буфер и виджет, если время истекло и недостаточно уникальных кодов
                with buffer_codes_lock:
                    buffer_codes_by_line[line] = []
                with widget_lock:
                    clear_widget(all_grouped_codes_text, line)
                red_column(line)
                with first_code_lock:
                    first_code_time_by_line.pop(line, None)  # Сбрасываем таймер
                return

        # Если уникальных кодов все еще недостаточно, продолжаем накопление
        if len(unique_buffer_codes) < bottles_per_pack:
            return

        # Если достигнуто необходимое количество уникальных кодов, обрабатываем их
        green_column(line)

        # Определяем путь к файлу для сохранения кодов
        if len(unique_buffer_codes[0]) >= 16:
            product_type = unique_buffer_codes[0][2:16].strip()
        else:
            log_error_once("Некорректная длина данных")
        product_name = product_mapping.get(product_type, f"({product_type})")
        with line_datetime_lock:
            datetime_prefix = line_datetime.get(line, "unknown_datetime")
        save_path = os.path.join(str(folder_path_label), f"Stage0_{product_name}_{datetime_prefix}_line_{line + 1}.txt")
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
        except Exception as e:
            log_error_once(f"Ошибка создания директории: {e}")

        # Загружаем существующие коды из файла
        existing_codes = set()
        if os.path.exists(save_path):
            with open(save_path, "r", encoding="utf-8") as f:
                existing_codes = {line.strip() for line in f}

        # Фильтруем новые коды (которых нет в файле)
        new_codes = [code for code in unique_buffer_codes if code not in existing_codes]

        if not new_codes:
            # Если новых кодов нет, очищаем буфер и виджет
            with buffer_codes_lock:
                buffer_codes_by_line[line] = []
            with widget_lock:
                clear_widget(all_grouped_codes_text, line)
            red_column(line)
            return

        # Записываем новые коды в файл
        try:
            with open(save_path, "a", encoding="utf-8") as f:
                for new_code in new_codes:
                    f.write(f"{new_code}\n")
        except IOError as e:
            log_error_once(f"Ошибка записи файла: {e}")

        # Добавляем новые коды в виджет
        for new_code in new_codes:
            with widget_lock:
                append_to_widget(
                    recorded_codes_text,
                    line,
                    new_code,
                    counters=recorded_grouped_codes_counters2,
                    file_path=0,
                    use_file_counter=False
                )

        # Очищаем виджет после успешной записи
        with widget_lock:
            clear_widget(all_grouped_codes_text, line)
        with buffer_codes_lock:
            # Очищаем буфер и сбрасываем таймер
            buffer_codes_by_line[line] = []
        with first_code_lock:
            first_code_time_by_line.pop(line, None)

    except Exception as e:
        log_error_once(f"Ошибка обработки буфера кодов в режиме сериализации: {e}")


def green_column(line):
    modbus_connections = get_modbus_connections()
    modbus_connectionss = {i: connection for i, connection in enumerate(modbus_connections)}
    if line in modbus_connectionss and modbus_connectionss[line] is not None:
        connection = modbus_connections[line]
        if connection.connected:
            with write_lock:
                connection.write_reg(register_addresses["green_column"][line], 1)
        else:
            log_error_once(f"Контроллер линии {line + 1} отключен. Невозможно зажечь зелёную колонну.")
    else:
        return


def red_column(line):
    modbus_connections = get_modbus_connections()
    modbus_connectionss = {i: connection for i, connection in enumerate(modbus_connections)}
    if line in modbus_connectionss:
        connection = modbus_connections[line]
        if connection.connected:
            with write_lock:
                connection.write_reg(register_addresses["red_column"][line], 1)
        else:
            log_error_once(f"Контроллер линии {line + 1} отключен. Невозможно зажечь зелёную колонну.")


def link_pallet_and_grouped_codes(gs1_code, product_name, line_num, folder_path_label):
    """
    Функция для связывания палетного кода с групповыми кодами и записи в файл.
    Если групповой код уже привязан к другому палету, старая привязка удаляется.
    :param gs1_code: Групповой код (GS1).
    :param line_num: Номер линии.
    :param folder_path_label: Путь к папке для сохранения данных.
    """
    with line_datetime_lock:
        datetime_prefix = line_datetime.get(line_num, "unknown_datetime")
    try:
        with pallet_lock:
            pallet_code = current_pallet_codes_by_line.get(line_num, '')
        if pallet_code:
            file_name = f"Stage2_{product_name}_{datetime_prefix}_line_{line_num + 1}.txt"
        else:
            file_name = f"Stage2_notCAM_{product_name}_{datetime_prefix}_line_{line_num + 1}.txt"
        save_path = os.path.join(folder_path_label, file_name)
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
        except Exception as e:
            log_error_once(f"Ошибка создания директории: {e}")

        # Проверяем, существует ли файл
        file_existed = os.path.exists(save_path)
        if not file_existed:
            log_info_once(f"Создан новый файл: {file_name}")

        # Список всех файлов Stage2 для проверки на дубликаты
        files_to_check = [
            f for f in os.listdir(folder_path_label)
            if f.startswith("Stage2") and f.endswith(".txt")
        ]

        # Удаляем старые привязки для данного GS1-кода
        for file_name in files_to_check:
            file_path = os.path.join(folder_path_label, file_name)
            lines_to_keep = []
            with open(file_path, "r", encoding="utf-8") as f:
                for line_content in f:
                    parts = line_content.strip().split("\t")
                    if len(parts) >= 1:
                        existing_gs1_code = parts[0]
                        if existing_gs1_code == gs1_code:
                            log_info_once(
                                f"Удалена старая привязка для кода {gs1_code} в файле {file_name}."
                            )
                        else:
                            lines_to_keep.append(line_content)
            # Перезаписываем файл, оставляя только нужные строки
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.writelines(lines_to_keep)
            except IOError as e:
                log_error_once(f"Ошибка записи файла: {e}")

        # Записываем новую привязку
        try:
            with open(save_path, "a", encoding="utf-8") as f:
                if pallet_code is None:
                    pallet_code = ''
                code = f"{gs1_code}\t{pallet_code}"
                f.write(f"{code}\n")
        except IOError as e:
            log_error_once(f"Ошибка записи файла: {e}")

    except Exception as e:
        log_error_once(f"Ошибка при связывании палетного и группового кодов: {e}")


def get_current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def get_previous_date():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def is_duplicate(data, file_path):
    with open(file_path, 'r') as file:
        if data in file.read():
            return True
    return False


def choose_folder(folder_path_label):
    chosen_folder = filedialog.askdirectory()
    if chosen_folder:
        folder_path_label.config(text=chosen_folder)
    else:
        chosen_folder = os.getcwd()
        folder_path_label.config(text=chosen_folder)
    try:
        with open("settings.json", "r+") as f:
            settings_data = get_settings_data()
            settings_data["folder_path"] = chosen_folder
            f.seek(0)
            json.dump(settings_data, f, indent=4)
            f.truncate()
    except FileNotFoundError:
        log_error_once("Файл настроек не найден.")

    restart_application()


all_codes_counters = {}
recorded_codes_counters = {}
all_grouped_codes_counters = {}
all_palet_codes_counters = {}
recorded_grouped_codes_counters1 = {}
recorded_grouped_codes_counters2 = {}


def append_to_widget(widget, line, data, counters, max_lines=25000, file_path=None, use_file_counter=False):
    """
    Универсальная функция для добавления строки в текстовый виджет.
    Скроллбар автоматически обновляется, если пользователь не прокручивает вручную.

    :param widget: Текстовый виджет, в который добавляется строка.
    :param line: Номер линии.
    :param data: Данные для добавления.
    :param counters: Словарь счетчиков строк для данного виджета.
    :param max_lines: Максимальное количество строк в виджете.
    :param file_path: Путь к файлу для подсчета строк (опционально).
    :param use_file_counter: Если True, счетчик берется из файла. Если False, используется внутренний счетчик.
    """
    # Инициализация счетчика для линии, если он еще не существует
    if line not in counters:
        counters[line] = 0
    counters[line] += 1

    # Подсчет строк в файле, если указан file_path и use_file_counter=True
    if use_file_counter and file_path:
        try:
            with open(file_path, 'r') as f:
                line_count = sum(1 for _ in f)
        except FileNotFoundError:
            line_count = 0
    else:
        line_count = counters[line]  # Используем внутренний счетчик

    # Форматирование строки
    new_line = f"Линия {str(line + 1).zfill(2)}: {str(line_count).zfill(4)}. {data}\n"

    # Проверка положения скроллбара
    widget.configure(state="normal")
    is_at_bottom = widget.yview()[1] == 1.0

    # Ограничение количества строк
    current_lines = widget.get("1.0", "end").splitlines()
    if len(current_lines) >= max_lines:
        widget.delete("1.0", f"{len(current_lines) - max_lines + 1}.0")

    # Добавление новой строки
    widget.insert("end", new_line)
    widget.configure(state="disabled")

    # Автоматическая прокрутка, если скроллбар внизу
    if is_at_bottom:
        widget.see("end")


def safe_append_log(log_output, log_entry):
    if log_output and log_output.winfo_exists():
        log_output.after(0, lambda: append_log(log_output, log_entry))
    else:
        print(f"Виджет log_output недоступен. Лог: {log_entry}")


def append_log(log_output, log_text, levelname):
    log_output.config(state=tk.NORMAL)  # Разрешаем редактирование

    # Определяем тег в зависимости от уровня логирования
    if levelname == "ERROR":
        log_output.insert(tk.END, log_text + "\n", "error")  # Применяем тег "error" для ошибок
    elif levelname == "INFO":
        log_output.insert(tk.END, log_text + "\n", "info")  # Применяем тег "info" для информационных сообщений

    log_output.config(state=tk.DISABLED)  # Запрещаем редактирование
    log_output.see(tk.END)  # Автопрокрутка до конца


def on_copy(event):
    event.widget.event_generate("<<Copy>>")


def on_cut(event):
    event.widget.event_generate("<<Cut>>")


def on_paste(event):
    event.widget.event_generate("<<Paste>>")


all_codes_text = None
recorded_codes_text = None
all_grouped_codes_text = None
recorded_grouped_codes_text = None


def create_hidden_window(root):
    global hidden_window
    hidden_window = tk.Toplevel(root)  # Создаем новое окно
    hidden_window.title("Сериализация")
    hidden_window.withdraw()  # Скрываем окно

    hidden_window.protocol("WM_DELETE_WINDOW", lambda: hide_hidden_window(hidden_window))
    settings_file = get_settings_data()
    ip_entries_controllers = [controller["ip"] for controller in settings_file.get("controllers", [])]
    port_entries_controllers = [controller["port"] for controller in settings_file.get("controllers", [])]
    port_entries_controllers = [controller["port"] for controller in settings_file.get("controllers", [])]
    modbus_connections = get_modbus_connections()
    interval = 1
    update_registers_periodically(hidden_window, ip_entries_controllers, port_entries_controllers, interval,
                                  modbus_connections)

    # Вычисляем размеры шрифтов и виджетов для нового окна
    screen_width = hidden_window.winfo_screenwidth()
    screen_height = hidden_window.winfo_screenheight()
    base_font_size = int(screen_height * 0.013)
    text_width = int(screen_width * 0.04)
    text_height = int(screen_height * 0.04)

    # Счётчики
    counter_labels = [
        ("Всего", 0, 2),
        ("Считано", 0, 3),
        ("Брак", 0, 4),
        ("Линия", 0, 1),
    ]
    for text, row, col in counter_labels:
        tk.Label(hidden_window, text=text, font=("Helvetica", base_font_size)).grid(row=row, column=col, padx=10,
                                                                                    pady=5)
    global all_codes_text, recorded_codes_text
    # Поля вывода для всех кодов и записанных кодов
    all_codes_text = create_text_widget(hidden_window, row=1, col=6, rowspan=13, colspan=2, text_width=text_width,
                                        text_height=text_height * 0.9, scrollbar_col=8)
    recorded_codes_text = create_text_widget(hidden_window, row=1, col=10, rowspan=13, colspan=2, text_width=text_width,
                                             text_height=text_height * 0.9, scrollbar_col=12)

    # Заголовки для полей вывода
    tk.Label(hidden_window, text="Все коды", font=("Helvetica", base_font_size)).grid(row=0, column=6, columnspan=2,
                                                                                      padx=5, pady=10)
    tk.Label(hidden_window, text="Записанные коды", font=("Helvetica", base_font_size)).grid(row=0, column=10,
                                                                                             columnspan=2, padx=5,
                                                                                             pady=10)

    return hidden_window


def show_hidden_window(hidden_window):
    hidden_window.deiconify()  # Показываем окно


def hide_hidden_window(hidden_window):
    hidden_window.withdraw()  # Скрываем окно


def toggle_window(hidden_window):
    if hidden_window.state() == "withdrawn":
        show_hidden_window(hidden_window)
    else:
        hide_hidden_window(hidden_window)


# Глобальные переменные для отслеживания состояния устройств
device_status = {}  # Текущий статус устройств
previous_device_status = {}  # Предыдущий статус устройств
device_status_attempts = {}  # Счетчик попыток подтверждения изменения состояния

ping_buffer = {}
update_interval = 1  # Интервал обновления буфера в секундах


def update_ping_buffer():
    """
    Функция для периодического обновления буфера состояния ping.
    """
    global ping_buffer
    while not exit_event.is_set():
        try:
            # Получаем список IP-адресов для проверки
            with buffer_lock:
                ips_to_check = list(ping_buffer.keys())

            # Выполняем ping для каждого IP-адреса
            for ip in ips_to_check:
                try:
                    success_count = 0  # Счетчик успешных попыток
                    for attempt in range(3):  # Пять попыток
                        result = subprocess.run(
                            ["ping", "-n", "1", "-w", "1000", ip],  # Для Windows
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            creationflags=0x08000000  # CREATE_NO_WINDOW
                        )
                        logging.debug(f"Попытка {attempt + 1} для {ip}: returncode={result.returncode}")
                        if result.returncode == 0:
                            success_count += 1

                    # Устройство доступно, если хотя бы 3 из 5 попыток успешны
                    is_reachable = success_count >= 2

                except Exception as e:
                    is_reachable = False
                    log_error_once(f"Ошибка при выполнении ping для {ip}: {e}")

                # Обновляем буфер
                with buffer_lock:
                    ping_buffer[ip] = is_reachable

            # Ждем перед следующим обновлением
            time.sleep(update_interval)

        except Exception as e:
            log_error_once(f"Критическая ошибка в потоке обновления буфера ping: {e}")


def check_ping(ip):
    """
    Возвращает состояние доступности устройства из буфера.
    Если устройство еще не было добавлено в буфер, добавляет его со статусом False.
    """
    global ping_buffer

    # Проверяем, есть ли устройство в буфере
    with buffer_lock:
        if ip not in ping_buffer:
            # Если устройство отсутствует в буфере, добавляем его со статусом False
            ping_buffer[ip] = False

        # Возвращаем состояние из буфера
        return ping_buffer[ip]


def check_com_port(com_port):
    """
    Проверяет доступность COM-порта без открытия нового подключения.
    """
    import serial.tools.list_ports

    try:
        # Получаем список доступных COM-портов
        available_ports = [port.device for port in serial.tools.list_ports.comports()]

        if com_port in available_ports:
            return True
        else:
            return False

    except Exception as e:
        log_error_once(f"Ошибка при проверке доступности COM-порта {com_port}: {e}")
        return False


def check_device_status(devices_by_lines, ip_addresses_by_lines):
    """
    Проверяет доступность устройств через ping или COM-порт и обновляет их статус.
    """
    global device_status, previous_device_status
    for line, devices in devices_by_lines.items():
        ip_addresses = ip_addresses_by_lines.get(line, [])
        for device, ip in zip(devices, ip_addresses):
            try:
                if "Сканер" in device:
                    com_port = device.split(" ")[1]
                    status = check_com_port(com_port)
                elif ip is not None:
                    status = check_ping(ip)
                else:
                    status = False
            except Exception as e:
                status = False
                log_error_once(f"Ошибка при проверке устройства {device} ({ip}): {e}")

            unique_device_key = f"{device}_{ip}" if ip else device
            previous_status = previous_device_status.get(unique_device_key, None)
            device_status[unique_device_key] = status

            # Выводим изменения состояния
            if previous_status != status:
                if status:
                    log_info_once(f"Устройство {device} линия {line + 1} подключено.")
                else:
                    log_info_once(f"Устройство {device} линия {line + 1} отключено.")
                previous_device_status[unique_device_key] = status


def update_indicators(indicator_widgets, devices_by_lines, ip_addresses_by_lines):
    """
    Обновляет состояние индикаторов на основе текущих данных о подключении устройств.
    :param indicator_widgets: Словарь с виджетами индикаторов, разделенными по линиям.
    :param devices_by_lines: Словарь, где ключи — номера линий, а значения — списки устройств.
    :param ip_addresses_by_lines: Словарь, где ключи — номера линий, а значения — списки IP-адресов.
    """
    # Получаем соединения Modbu
    modbus_connections = get_modbus_connections()

    # Блокировка для потокобезопасного доступа к общим ресурсам
    for line, devices in devices_by_lines.items():
        if line not in indicator_widgets:
            continue  # Пропускаем линии, которых нет в словаре виджетов

        # Получаем список IP-адресов для текущей линии
        ip_addresses = ip_addresses_by_lines.get(line, [])

        # Проходим по всем устройствам и их IP-адресам
        for device, ip in zip(devices, ip_addresses):
            if device not in indicator_widgets[line]:
                continue  # Пропускаем устройства, которых нет в словаре виджетов

            # Получаем виджет Canvas для устройства
            canvas = indicator_widgets[line][device].get("canvas")
            if canvas is None:
                log_error_once(f"Canvas для устройства {device} на линии {line} равен None.")
                continue
            if not canvas.winfo_exists():
                log_error_once(f"Виджет для устройства {device} на линии {line} не существует.")
                continue  # Пропускаем, если виджет удален или не существует

            # Определяем уникальный ключ устройства
            unique_device_key = f"{device}_{ip}" if ip else device
            status = device_status.get(unique_device_key, False)  # Получаем статус устройства

            # Обновляем цвет индикатора
            color = "green" if status else "red"
            try:
                canvas.itemconfig(1, fill=color, outline=color)
            except Exception as e:
                log_error_once(f"Ошибка обновления индикатора для устройства {device}: {e}")
                continue

            # Записываем статус устройства в регистр контроллера
            try:
                if line < len(modbus_connections) and modbus_connections[line] is not None:
                    connection = modbus_connections[line]
                    if connection.connected:
                        # Определяем offset регистра в зависимости от типа устройства
                        if "Групповая камера" in device:
                            register_offset = register_addresses["group_cam"][line]
                        elif "Групповой сканер" in device:
                            register_offset = register_addresses["scanner"][line]
                        elif "Контроллер" in device:
                            register_offset = register_addresses["controller"][line]
                        else:
                            register_offset = 910

                        # Определяем значение для записи (1 — активно, 0 — неактивно)
                        value = 1 if status else 0

                        # Потокобезопасная запись в регистр
                        with write_lock:
                            connection.write_reg(register_offset, value)
            except Exception as e:
                log_error_once(f"Ошибка записи статуса устройства {device} в регистр контроллера: {e}")


def create_indicators_frame(root, devices_by_lines):
    """
    Создает фрейм с индикаторами для устройств, разделенных по линиям.
    :param root: Главное окно Tkinter.
    :param devices_by_lines: Словарь, где ключи — номера линий, а значения — списки устройств.
    :return: Фрейм с индикаторами и словарь виджетов.
    """
    indicators_frame = tk.Frame(root, bg="white", padx=10, pady=10)
    indicators_frame.grid(row=0, column=0, columnspan=4, sticky="nw")  # Размещаем в левом верхнем углу

    # Создаем словарь для хранения виджетов индикаторов
    indicator_widgets = {}

    # Переменная для отслеживания максимального количества строк (устройств) в линиях
    max_rows = max(len(devices) for devices in devices_by_lines.values())

    # Создаем индикаторы для каждой линии
    for col, (line, devices) in enumerate(devices_by_lines.items()):
        # Добавляем заголовок для линии
        tk.Label(indicators_frame, text=f"Линия {line + 1}", font=("Arial", 12), bg="white").grid(
            row=0, column=col, padx=10, pady=5, sticky="n"
        )

        # Создаем индикаторы для каждого устройства в линии
        for row, device in enumerate(devices, start=1):  # Начинаем с 1, чтобы оставить место для заголовка
            # Создаем фрейм для одного устройства
            device_frame = tk.Frame(indicators_frame, bg="white")
            device_frame.grid(row=row, column=col, sticky="w", padx=5, pady=2)

            # Добавляем цветной кружок (индикатор)
            canvas = tk.Canvas(device_frame, width=15, height=15, bg="white", highlightthickness=0)
            canvas.create_oval(2, 2, 14, 14, fill="red", outline="red")  # Начальное состояние — красный
            canvas.pack(side="left")

            # Добавляем название устройства
            label = tk.Label(device_frame, text=device, font=("Arial", 10), bg="white")
            label.pack(side="left")

            # Сохраняем виджеты для последующего обновления
            if line not in indicator_widgets:
                indicator_widgets[line] = {}
            indicator_widgets[line][device] = {"canvas": canvas, "label": label}

    return indicators_frame, indicator_widgets


def update_ui(root, modbus_connections):
    try:
        settings_data = get_settings_data()
    except FileNotFoundError:
        create_settings_file()
        log_error_once("Файл настроек не найден.")
        return
    ip_entries = []
    port_entries = []
    ip_entries_g = []
    port_entries_g = []
    line = []
    line_g = []

    # Очищаем интерфейс
    for widget in root.winfo_children():
        widget.destroy()

    # Получаем данные из файла настроек
    cameras = settings_data.get("cameras", [])
    cameras_g = settings_data.get("cameras_g", [])
    mode_aggr = settings_data.get("aggregation_mode", "mode1")
    scanners = settings_data.get("scanners", [])
    controllers = settings_data.get("controllers", [])
    folder_path_label = settings_data.get("folder_path", "")
    current_date = get_current_date()
    folder_path = os.path.join(folder_path_label, current_date)

    com_ports = [(scanner["com_port"], scanner["line"]) for scanner in settings_data.get("scanners", [])]
    # Формируем словари devices_by_lines и ip_addresses_by_lines из данных настроек
    devices_by_lines = {}
    ip_addresses_by_lines = {}

    # Обрабатываем обычные камеры
    for camera in cameras:
        line_number = camera["line"]
        ip_entries.append(camera["ip"])
        port_entries.append(camera["port"])
        line.append(camera["line"])
        if line_number not in devices_by_lines:
            devices_by_lines[line_number] = []
            ip_addresses_by_lines[line_number] = []
        devices_by_lines[line_number].append(f"Камера отбраковочная")
        ip_addresses_by_lines[line_number].append(camera["ip"])

    scanner_ips = []
    scanner_ports = []

    # Обрабатываем групповые камеры
    for camera in cameras_g:
        line_number = camera["line"]
        ip_entries_g.append(camera["ip"])
        port_entries_g.append(camera["port"])
        line_g.append(camera["line"])
        if line_number not in devices_by_lines:
            devices_by_lines[line_number] = []
            ip_addresses_by_lines[line_number] = []
        devices_by_lines[line_number].append(f"Групповая камера")
        ip_addresses_by_lines[line_number].append(camera["ip"])

        # Добавляем данные для сканера, если режим mode2
        if mode_aggr == "mode2":
            scanner_ip = camera.get("scanner_ip", "0.0.0.0")  # Значение по умолчанию
            scanner_port = camera.get("scanner_port", "0")  # Значение по умолчанию
            devices_by_lines[line_number].append(f"Групповой сканер")
            ip_addresses_by_lines[line_number].append(scanner_ip)
            ip_entries_g.append(scanner_ip)
            port_entries_g.append(scanner_port)
            line_g.append(camera["line"])

    # Обрабатываем сканеры
    for scanner in scanners:
        line_number = scanner["line"]
        if line_number not in devices_by_lines:
            devices_by_lines[line_number] = []
            ip_addresses_by_lines[line_number] = []
        devices_by_lines[line_number].append(f"Сканер {scanner['com_port']}")
        ip_addresses_by_lines[line_number].append(None)  # COM-порты не имеют IP

    for controller in controllers:
        line_number = controller["line"]
        if line_number not in devices_by_lines:
            devices_by_lines[line_number] = []
            ip_addresses_by_lines[line_number] = []
        devices_by_lines[line_number].append(f"Контроллер")
        ip_addresses_by_lines[line_number].append(controller["ip"])

    # Вычисляем размеры шрифтов и виджетов
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    base_font_size = int(screen_height * 0.013)
    header_font_size = int(screen_height * 0.018)
    text_width = int(screen_width * 0.038)
    text_height = int(screen_height * 0.04)
    button_width = int(screen_width * 0.01)
    button_height = int(screen_height * 0.05)

    # Создаем фрейм с индикаторами
    indicators_frame, indicator_widgets = create_indicators_frame(root, devices_by_lines)

    # Проверяем статус устройств и запускаем периодическую проверку
    periodic_ping(root, devices_by_lines, ip_addresses_by_lines, indicator_widgets)

    # Создаем скрытое окно и кнопки
    hidden_window = create_hidden_window(root)
    toggle_window_button = create_styled_button(
        root, text="Сериализация", command=lambda: toggle_window(hidden_window))
    toggle_window_button.grid(row=15, column=5, columnspan=2, padx=10, pady=5, sticky='nsew')
    use_pal = settings_data.get("use_pall_aggregation", False)
    if use_pal:
        header_labels = [
            ("Журнал", 1, 0, 4),
            ("Буфер", 0, 5, 2),
            ("Записанные коды", 0, 8, 2),
            ("Паллеты", 7, 0, 4),
        ]
    else:
        header_labels = [
            ("Журнал", 1, 0, 4),
            ("Буфер", 0, 5, 2),
            ("Записанные коды", 0, 8, 2),
            ("Счётчики", 7, 0, 4),
        ]
    for text, row, col, colspan in header_labels:
        tk.Label(root, text=text, font=("Helvetica", header_font_size)).grid(
            row=row, column=col, columnspan=colspan, padx=5, pady=10
        )

    separator = ttk.Separator(root, orient='horizontal')
    separator.grid(row=14, column=0, columnspan=13, sticky='ew', pady=10)

    global all_grouped_codes_text, recorded_grouped_codes_text
    all_grouped_codes_text = create_text_widget(
        root, row=1, col=5, rowspan=13, colspan=2, text_width=text_width,
        text_height=text_height * 0.7, scrollbar_col=7
    )
    recorded_grouped_codes_text = create_text_widget(
        root, row=1, col=8, rowspan=13, colspan=2, text_width=text_width,
        text_height=text_height * 0.7, scrollbar_col=10
    )
    log_output = create_text_widget(
        root, row=2, col=0, rowspan=1, colspan=4, text_width=text_width,
        text_height=text_height * 0.4, scrollbar_col=4
    )
    setup_logging(log_output)

    # Кнопка "Старт"
    start_button = tk.Button(
        root, text="Старт", font=("Helvetica", base_font_size), width=button_width, height=button_height
    )
    start_button.grid(row=16, column=5, padx=10, pady=5, sticky='nsew')
    start_button.config(
        command=lambda: start_button_command(
            root, ip_entries, port_entries, ip_entries_g, port_entries_g,
            [], [], [], line, line_g, folder_path_label, start_button,
            exit_event, com_ports
        ),
        state=tk.NORMAL
    )
    start_button.grid_remove()

    # Кнопка "Настройки"
    settings_button = create_styled_button(root, text="Настройки", command=settings_w)
    settings_button.grid(row=15, column=8, columnspan=2, padx=10, pady=5, sticky='nsew')

    start_button.invoke()

    # Создаем таблицу счетчиков
    pallet_counter_labels = {}
    pack_counter_labels = {}
    bottle_counter_labels = {}

    def create_counters_table():
        """Создает таблицу счетчиков для всех линий."""
        tk.Label(root, text="Линия", font=("Helvetica", base_font_size)).grid(
            row=8, column=1, padx=5, pady=5, sticky='w'
        )
        tk.Label(root, text="Пакки", font=("Helvetica", base_font_size)).grid(
            row=8, column=2, padx=5, pady=5, sticky='w'
        )
        tk.Label(root, text="Бутылки", font=("Helvetica", base_font_size)).grid(
            row=8, column=3, padx=5, pady=5, sticky='w'
        )

        for i, camera in enumerate(cameras):
            line_num = camera["line"]
            tk.Label(root, text=f"{line_num + 1}", font=("Helvetica", base_font_size)).grid(
                row=9 + i, column=1, padx=5, pady=5, sticky='w'
            )
            pack_counter_labels[line_num] = tk.Label(root, text="0", font=("Helvetica", base_font_size))
            pack_counter_labels[line_num].grid(row=9 + i, column=2, padx=5, pady=5, sticky='w')
            bottle_counter_labels[line_num] = tk.Label(root, text="0", font=("Helvetica", base_font_size))
            bottle_counter_labels[line_num].grid(row=9 + i, column=3, padx=5, pady=5, sticky='w')

    def count_items_from_files(folder_path, pattern, lines):
        """
        Подсчитывает количество строк в файлах, соответствующих регулярному выражению.
        :param folder_path: Путь к папке с файлами.
        :param pattern: Регулярное выражение для имени файла.
        :param lines: Список номеров линий.
        :return: Словарь {line_num: count} с количеством элементов на каждую линию.
        """
        item_counts = {line: 0 for line in lines}
        regex = re.compile(pattern)

        try:
            if not os.path.exists(folder_path):
                print(f"Папка {folder_path} не найдена.")
                return item_counts

            for filename in os.listdir(folder_path):
                match = regex.match(filename)
                if not match:
                    continue

                try:
                    # Извлекаем номер линии
                    line_num = int(match.group(2)) - 1  # индексация с нуля

                    if line_num not in item_counts:
                        continue  # Пропускаем неизвестные линии

                    file_path = os.path.join(folder_path, filename)

                    with open(file_path, "r") as f:
                        lines_in_file = f.readlines()

                    if not lines_in_file:
                        continue

                    item_counts[line_num] += len(lines_in_file)

                except Exception as e:
                    log_error_once(f"Ошибка при обработке файла {filename}: {e}")

        except Exception as e:
            log_error_once(f"Критическая ошибка при подсчете элементов: {e}")

        return item_counts

    def count_pallet_info_from_files_2(folder_path_stage1, folder_path_stage2, lines):
        """
        Подсчитывает информацию о палетных кодах: количество групповых и индивидуальных кодов.
        :param folder_path_stage1: Путь к папке с файлами Stage1.
        :param folder_path_stage2: Путь к папке с файлами Stage2.
        :param lines: Список номеров линий.
        :return: Словарь с информацией о палетных кодах.
        """
        pallet_info = {}
        try:
            # Проходим по всем файлам Stage2
            for filename in os.listdir(folder_path_stage2):
                match = re.match(r"Stage2_.*_line_(\d+)\.txt", filename)
                if match:
                    try:
                        line_num = int(match.group(1))  # Номер линии
                        file_path_stage2 = os.path.join(folder_path_stage2, filename)

                        # Инициализируем запись для линии
                        if line_num not in pallet_info:
                            pallet_info[line_num] = {}

                        # Считываем данные из файла Stage2
                        with open(file_path_stage2, "r") as f:
                            for line_content in f:
                                parts = line_content.strip().split("\t")
                                if len(parts) >= 2:
                                    group_code = parts[0].strip()  # Групповой код
                                    pallet_code = parts[1].strip()  # Палетный код

                                    # Пропускаем строки с палетным кодом "None"
                                    if pallet_code.lower() == "none":
                                        continue

                                    # Инициализируем запись для палетного кода
                                    if pallet_code not in pallet_info[line_num]:
                                        pallet_info[line_num][pallet_code] = {
                                            "group": set(),  # Множество уникальных групповых кодов
                                            "individual": set()  # Множество уникальных индивидуальных кодов
                                        }

                                    # Добавляем групповой код в множество
                                    pallet_info[line_num][pallet_code]["group"].add(group_code)

                    except Exception as e:
                        log_error_once(f"Ошибка при обработке файла {filename}: {e}")

            # Для каждого группового кода находим связанные индивидуальные коды из всех файлов Stage1
            for filename in os.listdir(folder_path_stage1):
                match = re.match(r"Stage1_.*_line_(\d+)\.txt", filename)
                if match:
                    try:
                        line_num = int(match.group(1))  # Номер линии
                        file_path_stage1 = os.path.join(folder_path_stage1, filename)

                        # Считываем данные из файла Stage1
                        with open(file_path_stage1, "r") as f:
                            for line_content in f:
                                parts = line_content.strip().split("\t")
                                if len(parts) < 2:
                                    continue  # Пропус

                                # Разделяем строку на индивидуальный и групповой коды
                                individual_code = parts[0].strip()
                                group_code = parts[1].strip()

                                # Для каждой линии проверяем, есть ли связанный палетный код
                                if line_num in pallet_info:
                                    for pallet_code, data in pallet_info[line_num].items():
                                        # Проверяем, что групповой код связан с паллетным кодом
                                        if group_code in data["group"]:
                                            # Добавляем индивидуальный код в множество
                                            data["individual"].add(individual_code)

                    except Exception as e:
                        log_error_once(f"Ошибка при обработке файла {filename}: {e}")

        except Exception as e:
            log_error_once(f"Критическая ошибка при подсчете информации о палетах: {e}")

        return pallet_info

    def update_pallet_info_display_2(root, folder_path, lines, pallet_info_text):
        """
        Обновляет текстовое поле с информацией о палетах.
        """
        try:
            # Подсчитываем информацию о палетах
            pallet_info = count_pallet_info_from_files_2(folder_path, folder_path, lines)

            # Проверяем, находится ли пользователь внизу текстового поля
            is_at_bottom = pallet_info_text.yview()[1] == 1.0

            # Очищаем текстовое поле
            pallet_info_text.config(state=tk.NORMAL)
            pallet_info_text.delete("1.0", tk.END)

            # Формируем текст для вывода
            for line_num in sorted(pallet_info.keys()):
                for pallet_code, data in pallet_info[line_num].items():
                    group_count = len(data["group"])  # Количество уникальных групповых кодов
                    individual_count = len(data["individual"])  # Количество уникальных индивидуальных кодов

                    # Выводим строку с информацией о паллетном коде
                    pallet_info_text.insert(
                        tk.END,
                        f"{pallet_code:<20} ({group_count:<3})( {individual_count:<3}) Линия {line_num}\n"
                    )

                pallet_info_text.insert(tk.END, "\n")

            # Прокручиваем вниз только если пользователь был внизу
            if is_at_bottom:
                pallet_info_text.see(tk.END)

            pallet_info_text.config(state=tk.DISABLED)

        except Exception as e:
            log_error_once(f"Ошибка при обновлении информации о палетах: {e}")

    def count_unique_pallets_from_files(folder_path, lines):
        pallet_counts = {}
        try:
            if not os.path.exists(folder_path):
                print(f"Папка {folder_path} не найдена.")
                return pallet_counts

            for filename in os.listdir(folder_path):
                match = re.match(r"Stage2_.*_line_(\d+)\.txt", filename)
                if match:
                    try:
                        line_num = int(match.group(1))
                        file_path = os.path.join(folder_path, filename)

                        unique_pallets = set()
                        if line_num not in pallet_counts:
                            pallet_counts[line_num] = set()

                        with open(file_path, "r") as f:
                            for line in f:
                                parts = line.strip().split("\t")
                                if len(parts) < 2:
                                    print(f"Некорректная строка в файле {filename}: {line}")
                                    continue

                                pallet_code = parts[1]
                                if pallet_code.lower() == "none":
                                    continue
                                unique_pallets.add(pallet_code)

                        pallet_counts[line_num].update(unique_pallets)

                    except Exception as e:
                        print(f"Ошибка при обработке файла {filename}: {e}")

            for line_num in pallet_counts:
                pallet_counts[line_num] = len(pallet_counts[line_num])

        except Exception as e:
            print(f"Ошибка при подсчете уникальных палет: {e}")

        return pallet_counts

    def update_counters_1(pack_counter_labels, pack_counts, bottle_counts, bottle_counts1, pallet_counts,
                          bottles, total_pack_counts, total_bottle_counts):

        # Получаем настройки и подключения к контроллерам
        settings_data = get_settings_data()
        use_serial = settings_data.get("use_serial", False)
        modbus_connections1 = get_modbus_connections()

        # Обновляем значения для каждой линии
        for index, connection in enumerate(modbus_connections1):
            line_num = index
            try:
                if not use_pal:
                    # Обновляем значения в интерфейсе
                    pack_counter_labels[line_num].config(text=int(total_pack_counts.get(line_num, 0)))
                    bottle_counter_labels[line_num].config(text=str(total_bottle_counts.get(line_num, 0)))
                # Записываем значения в регистры контроллера
                connection = modbus_connections1[line_num]
                if connection is None:
                    return
                if connection.connected:
                    with write_lock:
                        connection.write_reg(register_addresses["packs"][line_num], pack_counts.get(line_num, 0))
                        connection.write_reg(register_addresses["bottles"][line_num], bottle_counts.get(line_num, 0))
                        connection.write_reg(register_addresses["pallets"][line_num], pallet_counts.get(line_num, 0))
                        connection.write_reg(register_addresses["bottles_ser"][line_num],
                                             bottle_counts1.get(line_num, 0))
                        connection.write_reg(register_addresses["total_packs"][line_num],
                                             total_pack_counts.get(line_num, 0))
                        connection.write_reg(register_addresses["total_bottles"][line_num],
                                             total_bottle_counts.get(line_num, 0))

            except Exception as e:
                print(f"Ошибка при обновлении счетчиков для линии {line_num + 1}: {e}")

    # Проверяем значение параметра "use_pall_aggregation"
    use_pall_aggregation = settings_data.get("use_pall_aggregation", False)

    # Создаем текстовое поле для вывода информации о палетах (если use_pall_aggregation == False)
    if use_pall_aggregation:
        pallet_info_text = create_text_widget(root, row=9, col=0, rowspan=1, colspan=4, text_width=text_width,
                                              text_height=text_height * 0.3, scrollbar_col=4)
    else:
        create_counters_table()

    def load_line_datetimess(json_file):
        """
        Загружает времена для линий из JSON-файла.
        :param json_file: Путь к файлу line_datetimes.json.
        :return: Словарь с временами для каждой линии.
        """
        try:
            with open(json_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            log_error_once(f"Ошибка при загрузке line_datetimes.json: {e}")
            return {}

    total_pal_pack_counts = {}
    total_pal_bottle_counts = {}
    total_pallet_counts = {}

    # Функция для периодического обновления счетчиков
    def periodic_update_counters(root, folder_path, lines, pallet_counter_labels, pack_counter_labels,
                                 bottle_counter_labels, modbus_connections):
        """
        Периодически обновляет счетчики из файлов.
        Учитывает только файлы, соответствующие времени из line_datetimes.json.
        """
        try:
            # Загружаем времена для линий из JSON-файла
            line_datetimes = load_line_datetimess("line_datetimes.json")
            total_pack_counts = []
            total_bottle_counts = []
            # Подсчитываем количество бутылок (Stage1_*_line_X.txt)
            bottle_counts1 = count_items_from_files(
                folder_path,
                r"Stage0_.*_(\d{2}-\d{2}-\d{2})_line_(\d+)\.txt",  # Группа 1: время, Группа 2: номер линии
                lines
            )
            total_bottle_counts = count_items_from_files(
                folder_path,
                r"Stage1_.*_(\d{2}-\d{2}-\d{2})_line_(\d+)\.txt",  # Группа 1: время, Группа 2: номер линии
                lines
            )
            total_pack_counts = count_items_from_files(
                folder_path,
                r"Stage2_.*_(\d{2}-\d{2}-\d{2})_line_(\d+)\.txt",  # Группа 1: время, Группа 2: номер линии
                lines
            )
            bottles = get_bottles_per_pack_for_lines(settings_data, lines)
            # Подсчитываем паллеты в зависимости от значения use_pall_aggregation
            pallet_counts = {}
            pack_counts = {}
            bottle_counts = {}
            if use_pall_aggregation:
                pallet_info = count_pallet_info_from_files_2(folder_path, folder_path, lines)

                # Проходим по всем линиям в pallet_info
                for line_num in sorted(pallet_info.keys()):
                    total_pal_pack_counts[line_num] = 0
                    total_pal_bottle_counts[line_num] = 0
                    total_pallet_counts[line_num] = 0

                    # Проходим по всем паллетным кодам для текущей линии
                    with pallet_lock:
                        pal_code = current_pallet_codes_by_line.get(line_num - 1, '')
                    for pallet_code, data in pallet_info[line_num].items():
                        total_pallet_counts[line_num] += 1
                    if pal_code in pallet_info[line_num]:
                        data = pallet_info[line_num][pal_code]
                        total_pal_pack_counts[line_num] += len(data["group"])
                        total_pal_bottle_counts[line_num] += len(data["individual"])
                    pack_counts[line_num - 1] = total_pal_pack_counts[line_num]
                    bottle_counts[line_num - 1] = total_pal_bottle_counts[line_num]
                    pallet_counts[line_num - 1] = total_pallet_counts[line_num]

                # Проверяем, существует ли текстовое поле pallet_info_text
                if 'pallet_info_text' in globals() or 'pallet_info_text' in locals():
                    update_pallet_info_display_2(root, folder_path, lines, pallet_info_text)

                else:
                    log_error_once("Текстовое поле pallet_info_text не создано.")

            # Обновляем интерфейс
            update_counters_1(pack_counter_labels, pack_counts, bottle_counts, bottle_counts1, pallet_counts, bottles,
                              total_pack_counts, total_bottle_counts)

        except Exception as e:
            log_error_once(f"Ошибка при периодическом обновлении счетчиков: {e}")

        # Повторяем каждую секунду
        root.after(1000, lambda: periodic_update_counters(root, folder_path, lines, pallet_counter_labels,
                                                          pack_counter_labels, bottle_counter_labels,
                                                          modbus_connections))

    # Вызов периодического обновления
    current_date = get_current_date()
    folder_path = settings_data.get("folder_path", "")
    folder_path_for_date = os.path.join(folder_path, current_date)
    if not os.path.exists(folder_path_for_date):
        try:
            os.makedirs(folder_path_for_date, exist_ok=True)
            print(f"Папка успешно создана: {folder_path_for_date}")
        except PermissionError as e:
            print(f"Ошибка доступа: {e}")
    else:
        if os.path.isdir(folder_path_for_date):
            print(f"Папка уже существует: {folder_path_for_date}")
        else:
            print(f"Ошибка: по указанному пути находится файл, а не папка: {folder_path_for_date}")
    lines = [camera["line"] for camera in settings_data.get("cameras", [])]  # Получаем список линий
    periodic_update_counters(root, folder_path_for_date, lines, pallet_counter_labels, pack_counter_labels,
                             bottle_counter_labels, modbus_connections)

    # Чтение задержек
    try:
        for i, controller in enumerate(controllers):
            delays = [controller.get(f"delay{j + 1}", 0) for j in range(4)]
    except FileNotFoundError:
        log_error_once("Файл настроек не найден.")
    except Exception as e:
        log_error_once(f"Ошибка при обновлении интерфейса: {e}")


def create_text_widget(root, row, col, rowspan, colspan, text_width, text_height, scrollbar_col):
    """
    Создаёт текстовый виджет с полосой прокрутки, белым фоном, чёрными рамками и контекстным меню.
    """
    # Создаём Canvas для скруглённой рамки
    canvas = tk.Canvas(
        root,
        width=text_width * 8,  # Ширина виджета
        height=text_height * 20,  # Высота виджета
        bg="#FFFFFF",  # Белый фон
        highlightthickness=0  # Убираем границу Canvas
    )
    canvas.grid(row=row, rowspan=rowspan, column=col, columnspan=colspan, padx=5, pady=5)

    # Рисуем скруглённую рамку на Canvas
    border_radius = 1  # Радиус скругления
    canvas.create_rectangle(
        border_radius, border_radius,
        text_width * 8 - border_radius, text_height * 20 - border_radius,
        outline="black",  # Чёрная рамка
        width=2  # Толщина рамки
    )

    # Создаём текстовый виджет внутри Canvas
    text_widget = tk.Text(
        canvas,
        state=tk.DISABLED,
        width=text_width,
        height=text_height,
        bg="#FFFFFF",  # Белый фон
        fg="#000000",  # Чёрный текст
        font=("Helvetica", 12),  # Современный шрифт
        relief="flat",  # Плоский стиль
        highlightthickness=0,  # Убираем выделение при фокусе
        borderwidth=0  # Убираем границу
    )
    text_widget_window = canvas.create_window(
        border_radius, border_radius,
        anchor="nw",
        window=text_widget,
        width=text_width * 8 - border_radius * 2,
        height=text_height * 20 - border_radius * 2
    )

    # Настройка тегов для цветов
    text_widget.tag_config("error", foreground="red")

    # Полоса прокрутки
    scrollbar = tk.Scrollbar(canvas, command=text_widget.yview)
    scrollbar.place(
        x=text_width * 8 - 20,  # Положение полосы прокрутки
        y=border_radius,
        height=text_height * 20 - border_radius * 2,
        anchor="nw"
    )
    text_widget.config(yscrollcommand=scrollbar.set)

    # Контекстное меню
    def copy_text(event=None):
        try:
            selected_text = text_widget.get("sel.first", "sel.last")
            root.clipboard_clear()
            root.clipboard_append(selected_text)
        except tk.TclError:
            pass  # Ничего не делаем, если текст не выделен

    context_menu = tk.Menu(text_widget, tearoff=0)
    context_menu.add_command(label="Копировать", command=copy_text)

    def show_context_menu(event):
        context_menu.post(event.x_root, event.y_root)

    text_widget.bind("<Button-3>", show_context_menu)  # Правая кнопка мыши

    return text_widget


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)


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
        self.connect_lock = threading.Lock()  # Блокировка для синхронизации подключения

    def connect(self):
        if self.connected:
            return

        with self.connect_lock:
            try:
                # Проверяем кэш
                cache_key = self.ip
                with cache_lock:
                    if cache_key in modbus_connection_cache:
                        cached_connection = modbus_connection_cache[cache_key]
                        if cached_connection.connected:
                            self.master = cached_connection.master
                            self.connected = True
                            print(f"Использовано кэшированное подключение для {self.ip}")
                            return

                # Проверяем доступность по ping
                if not check_ping(self.ip):
                    print(f"Устройство {self.ip} не отвечает на ping")
                    return

                # Создаем новое подключение
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)  # Увеличенный таймаут
                result = sock.connect_ex((self.ip, self.port))
                sock.close()

                if result != 0:
                    print(f"Не удалось установить TCP-соединение с {self.ip}:{self.port}")
                    log_error_once(f"Не удалось подключиться к контроллеру {self.ip}:{self.port}")
                    return

                # Инициализация Modbus клиента
                self.master = ModbusClient(host=self.ip, port=self.port, timeout=2)  # Увеличенный таймаут
                if not self.master.open():
                    print(f"Не удалось открыть Modbus-соединение с {self.ip}:{self.port}")
                    log_error_once(f"Не удалось открыть соединение Modbus с {self.ip}:{self.port}")
                    return

                self.connected = True
                with cache_lock:
                    modbus_connection_cache[cache_key] = self
                with connected_controllers_lock:
                    if self.ip not in connected_controllers:
                        connected_controllers.append(self.ip)

            except Exception as e:
                self.connected = False
                print(f"Ошибка подключения к контроллеру {self.ip}:{self.port}: {e}")
                log_error_once(f"Ошибка подключения к контроллеру {self.ip}:{self.port}: {e}")

    def read_registers(self):
        if not self.connected:
            return "-", "-", "-"

        try:
            with self.connect_lock:
                if not self.connected:
                    return "-", "-", "-"
                settings_data = get_settings_data()
                smesch = settings_data.get("smeschenie", 0)

                with write_lock:
                    reg_d411 = self.master.read_holding_registers(smesch + register_addresses["all_ser"][0], 1)
                    reg_d412 = self.master.read_holding_registers(smesch + register_addresses["suc_ser"][0], 1)
                    reg_d413 = self.master.read_holding_registers(smesch + register_addresses["def_ser"][0], 1)

                    # Проверяем, что данные корректны
                    if not reg_d411 or not reg_d412 or not reg_d413:
                        raise ValueError("Получены некорректные данные от устройства")

                    reg_d411 = reg_d411[0]
                    reg_d412 = reg_d412[0]
                    reg_d413 = reg_d413[0]

                    self.master.write_single_register(smesch + register_addresses["serv_work"][0], 1)

                return reg_d411, reg_d412, reg_d413
        except Exception as e:
            print(f"Ошибка чтения регистров у контроллера {self.ip}:{self.port}: {e}")
            self.connected = False
            with connected_controllers_lock:
                if self.ip in connected_controllers:
                    connected_controllers.remove(self.ip)
            return "-", "-", "-"

    def read_reg(self, number):
        if self.connected:
            try:
                with self.connect_lock:
                    if not self.connected:
                        return "-"
                    settings_data = get_settings_data()
                    smesch = settings_data.get("smeschenie", 0)
                    reg = self.master.read_holding_registers(smesch + number, 1)
                    print(f"Чтение {smesch + number}")
                    if reg is None or len(reg) == 0:
                        print(f"Ошибка чтения регистра {smesch + number} у контроллера {self.ip}:{self.port}.")
                    return reg[0]
            except Exception as e:
                if e:
                    self.connected = False
                    print(f"Контроллер {self.ip}:{self.port} отключён.")
        return 0

    def write_reg(self, number, value):
        if not self.connected:
            return 0

        try:
            with self.connect_lock:
                if not self.connected:
                    return 0
                settings_data = get_settings_data()
                smesch = settings_data.get("smeschenie", 0)

                result = self.master.write_single_register(smesch + number, value)
                if result is None:
                    raise ValueError("Не удалось записать значение в регистр")

                print(f"Запись {number + smesch} {value}")
                return result
        except Exception as e:
            print(f"Ошибка записи регистра у контроллера {self.ip}:{self.port}: {e}")
            self.connected = False
            return 0

    def start_connect_thread(self):
        with self.connect_lock:
            if not hasattr(self, "connect_thread") or not self.connect_thread.is_alive():
                self.connect_thread = threading.Thread(target=self.connect)
                self.connect_thread.start()
                threads_all.append(self.connect_thread)
            else:
                log_info_once(f"Поток подключения уже запущен для контроллера {self.ip}:{self.port}.")


def reconnect_unconnected(interval_reconnect, modbus_connections):
    """
    Пытается переподключиться к отключенным контроллерам.
    Если контроллер недоступен, программа продолжает работу.
    """
    settings_file = get_settings_data()
    ip_entries_controllers = [controller["ip"] for controller in settings_file.get("controllers", [])]

    for connection in modbus_connections:
        try:
            if connection is None:
                continue
            if not connection.connected:
                # Проверяем доступность контроллера перед попыткой подключения
                if check_ping(connection.ip):  # Проверка ping
                    connection.connect()
                else:
                    log_error_once(
                        f"Контроллер {connection.ip}:{connection.port} недоступен. Пропускаем попытку подключения.")
        except Exception as e:
            log_error_once(f"Ошибка при попытке переподключения к контроллеру {connection.ip}:{connection.port}: {e}")

    # Если количество подключенных контроллеров меньше ожидаемого, планируем новую попытку
    with connected_controllers_lock:
        if len(connected_controllers) < len(ip_entries_controllers):
            schedule_reconnect(interval_reconnect, modbus_connections)


reconnect_timer = None  # Инициализируем таймер как None


def schedule_reconnect(interval_reconnect, modbus_connections):
    """
    Планирует повторную попытку подключения к контроллерам через указанный интервал.
    """
    global reconnect_timer  # Обозначаем, что используем глобальную переменную

    try:
        if reconnect_timer is not None:
            reconnect_timer.cancel()  # Останавливаем старый таймер

        # Создаем новый таймер для повторной попытки подключения
        reconnect_timer = threading.Timer(
            interval_reconnect,
            reconnect_unconnected,
            args=[interval_reconnect, modbus_connections]
        )
        reconnect_timer.start()
        threads_all.append(reconnect_timer)
    except Exception as e:
        log_error_once(f"Ошибка при планировании повторного подключения: {e}")


num_cam_labels = []
counter_value_all_labels = []
counter_value_sch_labels = []
counter_value_br_labels = []
counter_value_sch_labels_g = []
counter_value_br_labels_g = []
separator_widgets = []


def update_registers(hidden_windoww, ip_entries_controllers, modbus_connections):
    screen_height = hidden_windoww.winfo_screenheight()

    # Вычислить размер базового шрифта
    base_font_size = int(screen_height * 0.013)

    # Убедитесь, что списки инициализированы
    while len(num_cam_labels) < len(ip_entries_controllers):
        num_cam_labels.append(None)
    while len(counter_value_all_labels) < len(ip_entries_controllers):
        counter_value_all_labels.append(None)
    while len(counter_value_sch_labels) < len(ip_entries_controllers):
        counter_value_sch_labels.append(None)
    while len(counter_value_br_labels) < len(ip_entries_controllers):
        counter_value_br_labels.append(None)
    while len(counter_value_sch_labels_g) < len(ip_entries_controllers):
        counter_value_sch_labels_g.append(None)
    while len(counter_value_br_labels_g) < len(ip_entries_controllers):
        counter_value_br_labels_g.append(None)

    # Сбор данных перед обновлением интерфейса
    reg_values = []
    for i, ip_address in enumerate(ip_entries_controllers):
        try:
            if not check_ping(ip_address):
                reg_values.append((None, None, None))
                continue

            check_and_update_line_datetime(i)
            modbus_connectionss = get_modbus_connections()
            connection = modbus_connectionss[i]

            if connection is not None and hasattr(connection, "connected") and connection.connected:
                with write_lock:
                    with pallet_lock:
                        pallet_code = current_pallet_codes_by_line.get(i, "")
                    connection.write_reg(register_addresses["pal_open"][i], 0 if pallet_code == "" else 1)
                reg_values.append(connection.read_registers())
            else:
                reg_values.append((None, None, None))
        except Exception as e:
            print(f"Ошибка при чтении регистров контроллера {ip_address}: {e}")
            reg_values.append((None, None, None))  # Гарантированно добавляем значение по умолчанию

    # Обновление виджетов
    for i in range(len(ip_entries_controllers)):
        reg411, reg412, reg413 = reg_values[i]

        # Обновление num_cam_value
        if num_cam_labels[i] is None:
            num_cam_labels[i] = tk.Label(hidden_windoww, font=("Helvetica", base_font_size))
            num_cam_labels[i].grid(row=i + 1, column=1, padx=10, pady=5)
        num_cam_labels[i].config(text=i + 1)

        # Обновление counter_value_all
        if counter_value_all_labels[i] is None:
            counter_value_all_labels[i] = tk.Label(hidden_windoww, font=("Helvetica", base_font_size))
            counter_value_all_labels[i].grid(row=i + 1, column=2, padx=10, pady=5)
        counter_value_all_labels[i].config(text=reg411)

        # Обновление counter_value_sch
        if counter_value_sch_labels[i] is None:
            counter_value_sch_labels[i] = tk.Label(hidden_windoww, font=("Helvetica", base_font_size))
            counter_value_sch_labels[i].grid(row=i + 1, column=3, padx=10, pady=5)
        counter_value_sch_labels[i].config(text=reg412)

        # Обновление counter_value_br
        if counter_value_br_labels[i] is None:
            counter_value_br_labels[i] = tk.Label(hidden_windoww, font=("Helvetica", base_font_size))
            counter_value_br_labels[i].grid(row=i + 1, column=4, padx=10, pady=5)
        counter_value_br_labels[i].config(text=reg413)

        # Добавление горизонтальных линий
        index = i  # Индекс для separator_widgets
        if len(separator_widgets) <= index:  # Если длина списка меньше, чем текущий индекс
            # Заполняем список до текущего индекса None-значениями
            separator_widgets.extend([None] * (index - len(separator_widgets) + 1))

        if separator_widgets[index] is None:  # Проверяем, существует ли разделитель для текущего индекса
            separator = ttk.Separator(hidden_windoww, orient='horizontal')
            separator.grid(row=i, column=0, columnspan=5, sticky='sew')  # Устанавливаем линию
            separator_widgets[index] = separator  # Обновляем список, добавляя новую линию

        # Принудительная сборка мусора (по желанию)
        gc.collect()


def update_registers_periodically(hidden_window, ip_entries_controllers, port_entries_controllers, interval,
                                  modbus_connections):
    update_registers(hidden_window, ip_entries_controllers, modbus_connections)
    hidden_window.after(interval * 2000, update_registers_periodically, hidden_window, ip_entries_controllers,
                        port_entries_controllers,
                        interval, modbus_connections)


settings_cache = None


def get_settings_data():
    global settings_cache  # Объявляем, что будем использовать глобальную переменную

    # Если кэш уже загружен, просто возвращаем его
    if settings_cache is not None:
        return settings_cache

    try:
        with open('settings.json', 'r') as f:
            settings_cache = json.load(f)  # Загружаем и кэшируем настройки
            return settings_cache
    except FileNotFoundError:
        create_settings_file()
        with open('settings.json', 'r') as f:
            settings_cache = json.load(f)  # Загружаем и кэшируем настройки после создания файла
            return settings_cache


def close_sockets(sockets_ss):
    for sock in sockets_ss:
        try:
            sock.close()
        except OSError as e:
            print(f"Ошибка при закрытии сокета: {e}")


# Глобальный кэш для хранения подключений
modbus_connection_cache = {}
cache_lock = threading.Lock()
settings_lock = threading.Lock()


def get_modbus_connections():
    """
    Возвращает список подключений ModbusConnection на основе данных из файла настроек.
    Подключения сохраняются в кэше и не создаются заново при каждом вызове.
    Если один из контроллеров недоступен, программа продолжает работу с остальными.
    Возвращает список, где каждому контроллеру соответствует либо объект подключения, либо None.
    """
    settings_file = get_settings_data()
    if not settings_file or "controllers" not in settings_file:
        log_error_once("Файл настроек пуст или отсутствует ключ 'controllers'.")
        return []

    controllers = settings_file.get("controllers", [])
    if not controllers:
        log_error_once("Список контроллеров пуст.")
        return []

    ip_entries_controllers = [controller.get("ip") for controller in controllers]
    port_entries_controllers = [controller.get("port", 502) for controller in controllers]

    # Проверка наличия IP
    if not all(ip_entries_controllers):
        log_error_once("Отсутствуют IP-адреса для некоторых контроллеров.")
        return []

    modbus_connections = [None] * len(controllers)  # Создаем список с None для всех контроллеров

    for index, (ip, port) in enumerate(zip(ip_entries_controllers, port_entries_controllers)):
        cache_key = ip
        with cache_lock:  # Проверяем кэш вне блокировки
            cached_connection = modbus_connection_cache.get(cache_key)
            if cached_connection and getattr(cached_connection, "connected", False):
                modbus_connections[index] = cached_connection
                continue

        # Попытка подключения к контроллеру
        try:
            connection = ModbusConnection(ip, port)
            if connection is not None:
                connection.connect()  # Запускаем подключение в текущем потоке
                with cache_lock:  # Блокируем доступ к кэшу
                    if connection.connected:
                        modbus_connection_cache[ip] = connection
                        modbus_connections[index] = connection  # Сохраняем подключение по индексу
                    else:
                        print(f"Не удалось подключиться к контроллеру {ip}:{port}. Продолжаем работу с остальными.")
                        modbus_connections[index] = None  # Явно указываем отсутствие подключения
            else:
                print(f"Не удалось подключиться к контроллеру {ip}:{port}. Продолжаем работу с остальными.")
                modbus_connections[index] = None  # Явно указываем отсутствие подключения
        except Exception as e:
            log_error_once(f"Ошибка при создании подключения к контроллеру {ip}:{port}: {e}")
            modbus_connections[index] = None  # Явно указываем отсутствие подключения

    # Очищаем кэш от устаревших или ненужных объектов
    cleanup_cache()

    return modbus_connections


def cleanup_cache():
    """
    Очищает кэш от устаревших или ненужных объектов подключений.
    """
    with cache_lock:
        for ip, connection in list(modbus_connection_cache.items()):
            if not connection.connected:
                del modbus_connection_cache[ip]


def periodic_ping(root, devices_by_lines, ip_addresses_by_lines, indicator_widgets):
    """
    Периодически проверяет доступность устройств через ping или COM-порт и обновляет индикаторы.
    :param devices_by_lines: Словарь, где ключи — номера линий, а значения — списки устройств.
    :param ip_addresses_by_lines: Словарь, где ключи — номера линий, а значения — списки IP-адресов.
    :param indicator_widgets: Словарь с виджетами индикаторов, разделенными по линиям.
    """
    if exit_event.is_set():  # Проверяем флаг завершения
        return

    # Проверяем статус устройств для каждой линии
    check_device_status(devices_by_lines, ip_addresses_by_lines)

    # Обновляем индикаторы
    update_indicators(indicator_widgets, devices_by_lines, ip_addresses_by_lines)

    # Повторяем проверку каждые 5 секунд с использованием root.after
    if not exit_event.is_set():
        root.after(3000, lambda: periodic_ping(root, devices_by_lines, ip_addresses_by_lines, indicator_widgets))


MASTER_KEY = "H4743J-KDI42L-KFJGUF-DKEUI7"  # Общий лицензионный ключ
LICENSE_FILE = "license.json"


# Функция для получения уникального "цифрового отпечатка" устройства
def get_device_fingerprint():
    system_info = {
        "machine": platform.machine(),
        "processor": platform.processor(),
        "system": platform.system(),
        "node": platform.node(),
    }

    # Добавление серийного номера жёсткого диска (Windows)
    if platform.system() == "Windows":
        try:
            disk_serial = subprocess.check_output("wmic diskdrive get serialnumber", shell=True).decode().strip()
            system_info["disk_serial"] = disk_serial.split("\n")[-1].strip()
        except Exception:
            system_info["disk_serial"] = "unknown"

    # Добавление UUID системы (Windows)
    if platform.system() == "Windows":
        try:
            uuid_system = subprocess.check_output("wmic csproduct get uuid", shell=True).decode().strip()
            system_info["uuid"] = uuid_system.split("\n")[-1].strip()
        except Exception:
            system_info["uuid"] = "unknown"

    # Хэшируем все данные для создания уникального идентификатора
    fingerprint_data = "".join(f"{key}:{value}" for key, value in system_info.items())
    return hashlib.sha256(fingerprint_data.encode()).hexdigest()


# Функция для генерации хэша с солью
def generate_license_hash(device_id, master_key):
    salt = os.urandom(16).hex()  # Генерация случайной соли
    combined_data = f"{device_id}:{master_key}:{salt}"
    hash_object = hashlib.sha256(combined_data.encode())
    return f"{hash_object.hexdigest()}:{salt}"  # Сохраняем хэш вместе с солью


# Функция для создания открытого лицензионного файла
def create_license_file(device_id, master_key):
    license_hash_with_salt = generate_license_hash(device_id, master_key)
    data = {"device_id": device_id, "license_hash": license_hash_with_salt}
    with open(LICENSE_FILE, "w") as f:
        json.dump(data, f, indent=4)  # Сохраняем данные в JSON формате
    print("Файл лицензии создан.")


# Функция для проверки лицензионного файла
def validate_license():
    if not os.path.exists(LICENSE_FILE):
        print("Файл лицензии не найден.")
        return False

    try:
        with open(LICENSE_FILE, "r") as f:
            data = json.load(f)  # Читаем данные из файла
        stored_device_id = data.get("device_id")
        stored_license_hash, stored_salt = data.get("license_hash").split(":")
    except Exception as e:
        print(f"Ошибка чтения файла лицензии: {e}")
        return False

    current_device_id = get_device_fingerprint()
    expected_hash = hashlib.sha256(f"{current_device_id}:{MASTER_KEY}:{stored_salt}".encode()).hexdigest()

    print(f"Сохранённый device_id: {stored_device_id}")
    print(f"Текущий device_id: {current_device_id}")
    print(f"Сохранённый хэш: {stored_license_hash}")
    print(f"Ожидаемый хэш: {expected_hash}")

    if stored_device_id == current_device_id and stored_license_hash == expected_hash:
        print("Лицензия действительна.")
        return True
    print("Лицензия недействительна.")
    return False


# Функция для активации лицензии
def activate_license():
    """
    Создаёт диалоговое окно для ввода лицензионного ключа.
    :return: True, если активация успешна, иначе False.
    """

    def on_submit():
        nonlocal license_key
        license_key = entry.get()
        dialog.destroy()

    # Создаём новое диалоговое окно
    dialog = tk.Toplevel()
    dialog.title("Активация")
    dialog.geometry("300x100")
    dialog.resizable(False, False)

    # Метка с инструкцией
    label = tk.Label(dialog, text="Введите лицензионный ключ:")
    label.pack(pady=5)

    # Поле ввода с скрытием символов
    entry = tk.Entry(dialog, show="*")
    entry.pack(pady=5)
    entry.focus_set()

    # Кнопка подтверждения
    submit_button = tk.Button(dialog, text="Подтвердить", command=on_submit)
    submit_button.pack(pady=5)

    # Переменная для хранения ключа
    license_key = None

    # Ожидаем закрытия диалогового окна
    dialog.wait_window(dialog)

    # Проверяем введённый ключ
    if not license_key:
        messagebox.showerror("Ошибка", "Лицензионный ключ не может быть пустым.")
        return False

    if license_key != MASTER_KEY:
        messagebox.showerror("Ошибка", "Неверный лицензионный ключ.")
        return False

    # Создаём файл лицензии
    device_id = get_device_fingerprint()
    create_license_file(device_id, MASTER_KEY)
    messagebox.showinfo("Успех", "Лицензия успешно активирована!")
    return True


def create_styled_button(parent, text, command, width=None, height=None, text_height=12):
    """
    Создаёт красивую кнопку с анимацией при наведении и современным дизайном.

    Параметры:
    - parent: родительский виджет (например, окно или фрейм).
    - text: текст на кнопке.
    - command: функция, вызываемая при нажатии на кнопку.
    - width: ширина кнопки (опционально).
    - height: высота кнопки (опционально).
    """

    def on_enter(event):
        button.config(bg="#6200EA", fg="white")  # Цвет при наведении (фиолетовый)

    def on_leave(event):
        button.config(bg="#3700B3", fg="white")  # Исходный цвет (тёмно-фиолетовый)

    button = tk.Button(
        parent,
        text=text,
        font=("Helvetica", text_height, "bold"),
        bg="#3700B3",  # Цвет фона (тёмно-фиолетовый)
        fg="white",  # Цвет текста
        relief="flat",  # Плоский стиль
        activebackground="#6200EA",  # Цвет при нажатии (ярко-фиолетовый)
        activeforeground="white",
        command=command,
        padx=15,  # Внутренние отступы по горизонтали
        pady=8,  # Внутренние отступы по вертикали
        borderwidth=0,  # Убираем границу
        highlightthickness=0,  # Убираем выделение при фокусе
        width=width,  # Ширина кнопки (если указана)
        height=height  # Высота кнопки (если указана)
    )
    button.bind("<Enter>", on_enter)  # Анимация при наведении
    button.bind("<Leave>", on_leave)  # Возврат к исходному состоянию
    return button


# Основная функция с проверкой лицензии
def main():
    global register_addresses
    root = tk.Tk()
    root.withdraw()  # Скрываем главное окно до завершения проверки лицензии
    # Проверка лицензии
    is_valid = validate_license()
    if not is_valid:
        tk.messagebox.showinfo("Лицензия", "Программа запущена на новом устройстве. Требуется активация.")
        if not activate_license():
            tk.messagebox.showerror("Ошибка", "Активация не завершена. Программа будет закрыта.")
            return
    # Если лицензия валидна, продолжаем выполнение программы
    root.deiconify()  # Показываем главное окно
    register_addresses = load_register_addresses()
    root.title("Групповая агрегация")
    modbus_connections = get_modbus_connections()
    update_ui(root, modbus_connections)
    load_line_datetime()

    settings_data = get_settings_data()
    # Остальной код программы
    local_ip = settings_data.get("server_ip", '')
    if local_ip == '':
        local_ip = get_local_ip()
    log_info_once(f"Сервер запущен на {local_ip}, ожидание подключений...")
    interval_reconnect = 60
    schedule_reconnect(interval_reconnect, modbus_connections)
    exit_event_1 = threading.Event()
    error_thread = threading.Thread(target=process_error_queue, name="error", args=(exit_event_1,))
    info_thread = threading.Thread(target=process_info_queue, name="info", args=(exit_event_1,))
    ping_update_thread = threading.Thread(target=update_ping_buffer, daemon=True)
    threads_all.append(error_thread)
    threads_all.append(info_thread)
    threads_all.append(ping_update_thread)
    error_thread.start()
    info_thread.start()
    ping_update_thread.start()

    class SettingsChangeHandler(FileSystemEventHandler):
        def on_modified(self, event):
            if event.src_path.endswith("settings.json"):
                root.after(0, update_ui, root, modbus_connections)
                log_info_once("Перезагрузка интерфейса")

    def on_close():
        exit_event_1.set()
        exit_event.set()
        close_sockets(sockets)
        close_sockets(sockets_g)
        close_sockets(sockets_s)
        close_sockets(sockets_controller)
        for timer in threads_all:
            if isinstance(timer, threading.Timer) and timer.is_alive():
                timer.cancel()
        timeout = 0.1
        for thread in threads_all:
            print(f"Попытка завершить поток: {thread}")
            thread.join(timeout)  # Ожидаем завершения потока в течение `timeout` секунд
            if thread.is_alive():
                print(f"Поток {thread} не завершился за {timeout} секунд. Продолжаем...")
            else:
                print(f"Поток закрыт: {thread}")
        for thread in threading.enumerate():
            print(f"Активный поток: {thread.name}")
        os.kill(os.getpid(), signal.SIGTERM)
        sys.exit()

    root.protocol("WM_DELETE_WINDOW", on_close)
    observer = Observer()
    observer.schedule((SettingsChangeHandler()), path=(os.getcwd()), recursive=False)
    observer.start()
    root.mainloop()


main()

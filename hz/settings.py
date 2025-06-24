import json
import os
import sys
import tkinter as tk
from tkinter import messagebox

from .utils import log_error_once, log_info_once


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


settings_cache = None


def get_settings_data():
    global settings_cache
    if settings_cache is not None:
        return settings_cache
    try:
        with open('settings.json', 'r') as f:
            settings_cache = json.load(f)
            return settings_cache
    except FileNotFoundError:
        create_settings_file()
        with open('settings.json', 'r') as f:
            settings_cache = json.load(f)
            return settings_cache


def restart_application():
    python = sys.executable
    try:
        os.execl(python, python, *sys.argv)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Не удалось перезапустить программу: {e}")
        sys.exit(1)

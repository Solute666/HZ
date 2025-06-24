import datetime
import logging
import queue
import threading

threads_all = []
error_queue = queue.Queue()
info_queue = queue.Queue()
last_info = None

error_history = []  # Хранение последних 5 ошибок
error_history_lock = threading.Lock()
MAX_ERROR_HISTORY = 1


def process_error_queue(exit_event_1):
    global error_history
    while not exit_event_1.wait(0.1):
        try:
            error_message = error_queue.get(timeout=0)
            if error_message not in error_history:
                current_time = datetime.datetime.now().strftime("%H:%M:%S")
                logging.error(f"[{current_time}] {error_message}")
                error_history.append(error_message)
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

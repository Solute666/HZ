# HZ


<<<<<<< codex/update-readme-with-project-instructions
## Project Overview
HZ is a Python application that manages cameras, controllers and scanners using serial and Modbus connections. The program provides a Tkinter based GUI for interacting with connected devices and saving configuration in `settings.json`.

## Setup
1. Install Python 3.
2. Install the required packages:
   ```bash
   pip install psutil pyserial pyModbusTCP
   ```
3. Start the application once to generate `settings.json` if it does not exist.

## Running the Applicatio
Launch the GUI by executing:
```bash
python main.py
```
The main window allows you to view logs, start/stop device polling and open the settings dialog.

## Configuring Devices
Open the settings window from the GUI to specify IP addresses, COM ports and counts of cameras, controllers or scanners. The configuration is stored in `settings.json` in the project directory and loaded automatically on startup.

## File Structure
- `main.py` – main application code and GUI.
- `settings.json` – configuration file created at runtime.
- `README.md` – project documentation.


=======

This package provides the `hz` module. Run the application using:

```bash
python -m hz
```
>>>>>>> main

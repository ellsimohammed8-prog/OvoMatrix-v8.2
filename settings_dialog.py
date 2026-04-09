"""
OvoMatrix v8.2 — settings_dialog.py
Settings & Diagnostic Dialog — CLEAN REWRITE

Features (all working):
1. Simulator Control (Start/Stop + NORMAL/WARNING/EMERGENCY modes)
2. Power Loss Simulation
3. Hardware Connectivity Status (MQTT, Modbus, GSM)
4. AI Expert Advisor (Gemini / ChatGPT / Claude) with DB persistence + Clear
5. History Hub (Alert log + AI log viewer)
6. Maintenance (Factory Reset + Manual DB Pulse)
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
import serial

import paho.mqtt.publish as mqtt_publish

from PyQt6.QtCore import Qt, QThread, QTimer, pyqtSignal, QMetaObject, Q_ARG
from PyQt6.QtGui import QColor, QFont, QPainter
from PyQt6.QtWidgets import (
    QDialog, QTabWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QLabel, QTextEdit, QWidget, QMessageBox, QGroupBox, QComboBox,
    QLineEdit, QTableWidget, QTableWidgetItem, QHeaderView,
    QSizePolicy, QScrollArea, QFrame, QCheckBox,
)

# ── Design System (local copy for standalone use) ─────────────────────────────
class DS:
    BG_VOID        = "#05080f"
    BG_PANEL       = "#0b1120"
    BG_PANEL_ALT   = "#0e1628"
    TEXT_PRIMARY   = "#e8f0fe"
    TEXT_SECONDARY = "#8899bb"
    TEXT_MUTED     = "#445577"
    BORDER         = "#1a2744"
    ACCENT_BLUE    = "#3d82f4"
    ACCENT_GREEN   = "#2ecc71"
    ACCENT_RED     = "#e74c3c"
    ACCENT_YELLOW  = "#f5c400"
    FONT_FAMILY    = "Segoe UI"
    APP_JSON       = "application/json"

STYLE_SHEET = f"""
QDialog, QWidget {{
    background-color: {DS.BG_VOID};
    color: {DS.TEXT_PRIMARY};
    font-family: "{DS.FONT_FAMILY}";
    font-size: 11px;
}}
QTabWidget::pane {{
    border: 1px solid {DS.BORDER};
    background: {DS.BG_VOID};
}}
QTabBar::tab {{
    background: {DS.BG_PANEL};
    color: {DS.TEXT_SECONDARY};
    padding: 9px 18px;
    border: 1px solid {DS.BORDER};
    border-bottom: none;
    min-width: 100px;
}}
QTabBar::tab:selected {{
    background: {DS.BG_VOID};
    color: {DS.ACCENT_BLUE};
    border-bottom: 2px solid {DS.ACCENT_BLUE};
}}
QGroupBox {{
    border: 1px solid {DS.BORDER};
    border-radius: 6px;
    margin-top: 12px;
    padding: 10px 8px 8px 8px;
    color: {DS.TEXT_SECONDARY};
}}
QGroupBox::title {{
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 4px;
    color: {DS.ACCENT_BLUE};
    font-weight: bold;
}}
QPushButton {{
    background: {DS.BG_PANEL};
    border: 1px solid {DS.BORDER};
    color: {DS.TEXT_PRIMARY};
    padding: 8px 14px;
    border-radius: 5px;
    font-weight: bold;
}}
QPushButton:hover {{
    border: 1px solid {DS.ACCENT_BLUE};
    color: {DS.ACCENT_BLUE};
}}
QPushButton:disabled {{
    color: {DS.TEXT_MUTED};
    border-color: {DS.BORDER};
}}
QTextEdit, QLineEdit {{
    background: {DS.BG_PANEL};
    border: 1px solid {DS.BORDER};
    color: {DS.TEXT_PRIMARY};
    border-radius: 4px;
    padding: 6px;
}}
QComboBox {{
    background: {DS.BG_PANEL};
    border: 1px solid {DS.BORDER};
    color: {DS.TEXT_PRIMARY};
    padding: 5px 8px;
    border-radius: 4px;
}}
QComboBox QAbstractItemView {{
    background: {DS.BG_PANEL};
    color: {DS.TEXT_PRIMARY};
    selection-background-color: {DS.ACCENT_BLUE};
}}
QTableWidget {{
    background: {DS.BG_PANEL};
    color: {DS.TEXT_SECONDARY};
    gridline-color: {DS.BORDER};
    border: 1px solid {DS.BORDER};
}}
QHeaderView::section {{
    background: {DS.BG_PANEL_ALT};
    color: {DS.ACCENT_BLUE};
    border: 1px solid {DS.BORDER};
    padding: 4px;
    font-weight: bold;
}}
QScrollBar:vertical {{
    background: {DS.BG_PANEL};
    width: 6px;
    border-radius: 3px;
}}
QScrollBar::handle:vertical {{
    background: {DS.BORDER};
    border-radius: 3px;
}}
"""


# ── Hardware Ping Thread ───────────────────────────────────────────────────────
class HardwarePingThread(QThread):
    """Pings MQTT, Modbus, and GSM every 5 seconds."""
    status_updated = pyqtSignal(str, bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._stop_evt = threading.Event()

    def run(self):
        while not self._stop_evt.is_set():
            self.status_updated.emit("MQTT Broker",   self._ping_mqtt())
            self.status_updated.emit("Modbus RS-485", self._ping_modbus())
            self.status_updated.emit("GSM Modem",     self._ping_gsm())
            self._stop_evt.wait(5.0)

    def stop(self):
        self._stop_evt.set()

    @staticmethod
    def _ping_mqtt() -> bool:
        try:
            import socket
            host = os.environ.get("MQTT_BROKER_HOST", "localhost")
            port = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
            with socket.create_connection((host, port), timeout=2.0):
                return True
        except Exception:
            return False

    @staticmethod
    def _ping_modbus() -> bool:
        port = os.environ.get("MODBUS_PORT", "COM3" if sys.platform == "win32" else "/dev/ttyUSB0")
        try:
            with serial.Serial(port, baudrate=9600, timeout=0.5) as ser:
                ser.write(b"\x01\x04\x00\x00\x00\x01\x31\xCA")
                return len(ser.read(7)) >= 7
        except Exception:
            return False

    @staticmethod
    def _ping_gsm() -> bool:
        port = os.environ.get("GSM_PORT", "COM4" if sys.platform == "win32" else "/dev/ttyUSB1")
        try:
            with serial.Serial(port, baudrate=115200, timeout=0.5) as ser:
                ser.write(b"AT\r\n")
                time.sleep(0.1)
                return b"OK" in ser.read(100)
        except Exception:
            return False


# ── LED Status Indicator ───────────────────────────────────────────────────────
class StatusLED(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedSize(16, 16)
        self._on = False

    def set_status(self, on: bool):
        self._on = on
        self.update()

    def paintEvent(self, _ev):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        p.setBrush(QColor(DS.ACCENT_GREEN if self._on else DS.ACCENT_RED))
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(0, 0, 16, 16)


# ═════════════════════════════════════════════════════════════════════════════
# Main Settings Dialog
# ═════════════════════════════════════════════════════════════════════════════
class SettingsDialog(QDialog):
    """OvoMatrix v8.2 — Settings & Diagnostic Dialog."""

    def __init__(self, db_manager=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("OvoMatrix  ·  Settings & Diagnostics")
        self.setWindowFlags(
            Qt.WindowType.Dialog |
            Qt.WindowType.WindowTitleHint |
            Qt.WindowType.WindowCloseButtonHint |
            Qt.WindowType.WindowMinimizeButtonHint
        )
        self.resize(720, 580)
        self.setStyleSheet(STYLE_SHEET)

        self._db = db_manager
        self._sim_active = False   # Simulator starts PAUSED — user presses Start

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        self.tabs = QTabWidget()
        root.addWidget(self.tabs)

        # self._build_simulation_tab()
        self._build_hardware_tab()
        self._build_ai_tab()
        self._build_history_tab()
        self._build_maintenance_tab()

        # Hardware ping thread
        self._hw_thread = HardwarePingThread(self)
        self._hw_thread.status_updated.connect(self._on_hw_status)
        self._hw_thread.start()

    # ─────────────────────────────────────────────────────────────────────────
    # TAB 1 — Simulation Control
    # ─────────────────────────────────────────────────────────────────────────
    def _build_simulation_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(12)

        # ── Start / Stop ────────────────────────────────────────────────────
        grp_ctrl = QGroupBox("Simulator Control")
        vbox = QVBoxLayout(grp_ctrl)
        vbox.setSpacing(8)

        self._btn_toggle = QPushButton("▶  Start Simulator (NORMAL mode)")
        self._btn_toggle.setMinimumHeight(50)
        self._btn_toggle.setStyleSheet(f"color: {DS.ACCENT_GREEN}; border: 2px solid {DS.ACCENT_GREEN};")
        self._btn_toggle.clicked.connect(self._toggle_simulator)
        vbox.addWidget(self._btn_toggle)

        self._lbl_sim_status = QLabel("Status: STOPPED  —  Press Start to begin simulation.")
        self._lbl_sim_status.setStyleSheet(f"color: {DS.TEXT_MUTED}; font-size: 10px;")
        vbox.addWidget(self._lbl_sim_status)
        layout.addWidget(grp_ctrl)

        # ── Scenario Buttons ─────────────────────────────────────────────────
        grp_mode = QGroupBox("Force Scenario (works while simulator is running)")
        grid = QHBoxLayout(grp_mode)
        grid.setSpacing(8)

        def _scenario_btn(label, colour, mode):
            btn = QPushButton(label)
            btn.setMinimumHeight(44)
            btn.setStyleSheet(f"color: {colour}; border: 1px solid {colour};")
            btn.clicked.connect(lambda: self._set_scenario(mode))
            return btn

        grid.addWidget(_scenario_btn("✅  NORMAL",    DS.ACCENT_GREEN,  "NORMAL"))
        grid.addWidget(_scenario_btn("⚠️  WARNING",   DS.ACCENT_YELLOW, "WARNING"))
        grid.addWidget(_scenario_btn("🚨  EMERGENCY", DS.ACCENT_RED,    "EMERGENCY"))
        layout.addWidget(grp_mode)

        # ── Power Loss ───────────────────────────────────────────────────────
        grp_pw = QGroupBox("Power System Simulation")
        vb2 = QVBoxLayout(grp_pw)
        btn_pl = QPushButton("⚡  Simulate Power Loss (one-shot LOW_BATTERY event)")
        btn_pl.setMinimumHeight(44)
        btn_pl.setStyleSheet(f"color: {DS.ACCENT_YELLOW}; border: 1px solid {DS.ACCENT_YELLOW};")
        btn_pl.clicked.connect(self._simulate_power_loss)
        vb2.addWidget(btn_pl)
        note = QLabel("Sends a LOW_BATTERY MQTT payload to the UPS monitor topic (farm/power).")
        note.setStyleSheet(f"color: {DS.TEXT_MUTED}; font-size: 10px;")
        vb2.addWidget(note)
        layout.addWidget(grp_pw)

        layout.addStretch()
        self.tabs.addTab(tab, "🎮  Simulation")

    # ─────────────────────────────────────────────────────────────────────────
    # TAB 2 — Hardware Status
    # ─────────────────────────────────────────────────────────────────────────
    def _build_hardware_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        grp = QGroupBox("Live Hardware Connectivity")
        vbox = QVBoxLayout(grp)
        vbox.setSpacing(14)

        self._hw_leds: dict[str, StatusLED] = {}
        for name, desc in [
            ("MQTT Broker",   "localhost:1883"),
            ("Modbus RS-485", os.environ.get("MODBUS_PORT", "COM3")),
            ("GSM Modem",     os.environ.get("GSM_PORT",   "COM4")),
        ]:
            row = QHBoxLayout()
            lbl = QLabel(f"<b>{name}</b>  <span style='color:{DS.TEXT_MUTED}'>{desc}</span>")
            lbl.setTextFormat(Qt.TextFormat.RichText)
            led = StatusLED()
            self._hw_leds[name] = led
            row.addWidget(lbl)
            row.addStretch()
            row.addWidget(led)
            vbox.addLayout(row)

        note = QLabel("Refreshed automatically every 5 seconds.")
        note.setStyleSheet(f"color: {DS.TEXT_MUTED}; font-size: 10px;")
        vbox.addWidget(note)
        layout.addWidget(grp)
        
        # ── SMS Notification Settings ───────────────────────────────────────
        grp_sms = QGroupBox("SMS Notification Recipients")
        vbox_sms = QVBoxLayout(grp_sms)
        
        lbl_notes = QLabel("Comma-separated list of E.164 phone numbers (e.g. +212600000001, +212600000002).")
        lbl_notes.setStyleSheet(f"color: {DS.TEXT_MUTED}; font-size: 10px;")
        vbox_sms.addWidget(lbl_notes)
        
        row_sms = QHBoxLayout()
        self._edit_phones = QLineEdit()
        self._edit_phones.setText(os.environ.get("ALERT_PHONE_NUMBERS", ""))
        self._edit_phones.setPlaceholderText("")
        row_sms.addWidget(self._edit_phones, stretch=1)
        
        btn_apply = QPushButton("💾 Save")
        btn_apply.clicked.connect(self._apply_phone_numbers)
        btn_apply.setStyleSheet(f"color: {DS.ACCENT_BLUE}; border: 1px solid {DS.ACCENT_BLUE}; padding: 4px 12px;")
        row_sms.addWidget(btn_apply)
        
        vbox_sms.addLayout(row_sms)
        layout.addWidget(grp_sms)
        
        # ── Hardware Options ───────────────────────────────────────────────
        grp_hw = QGroupBox("Hardware Configuration")
        vbox_hw = QVBoxLayout(grp_hw)
        
        row_hw1 = QHBoxLayout()
        row_hw1.addWidget(QLabel("Modbus Port:"))
        self._combo_modbus = QComboBox()
        self._combo_modbus.addItems(["COM1", "COM2", "COM3", "COM4", "COM5", "COM6"])
        self._combo_modbus.setCurrentText(os.environ.get("MODBUS_PORT", "COM3"))
        row_hw1.addWidget(self._combo_modbus)
        
        row_hw1.addSpacing(20)
        row_hw1.addWidget(QLabel("GSM Port:"))
        self._combo_gsm = QComboBox()
        self._combo_gsm.addItems(["COM1", "COM2", "COM3", "COM4", "COM5", "COM6"])
        self._combo_gsm.setCurrentText(os.environ.get("GSM_PORT", "COM4"))
        row_hw1.addWidget(self._combo_gsm)
        
        vbox_hw.addLayout(row_hw1)
        
        row_hw2 = QHBoxLayout()
        self._chk_mock_modbus = QCheckBox("Simulate Modbus (Mock Hardware Mode)")
        mock_val = os.environ.get("MOCK_MODBUS", "True").upper() == "TRUE"
        self._chk_mock_modbus.setChecked(mock_val)
        row_hw2.addWidget(self._chk_mock_modbus)
        
        btn_apply_hw = QPushButton("💾 Save Hardware Config")
        btn_apply_hw.clicked.connect(self._apply_hardware_config)
        btn_apply_hw.setStyleSheet(f"color: {DS.ACCENT_GREEN}; border: 1px solid {DS.ACCENT_GREEN}; padding: 4px 12px;")
        row_hw2.addStretch()
        row_hw2.addWidget(btn_apply_hw)
        vbox_hw.addLayout(row_hw2)
        
        layout.addWidget(grp_hw)
        
        layout.addStretch()
        self.tabs.addTab(tab, "🛠  Hardware")

    def _apply_hardware_config(self):
        modbus_port = self._combo_modbus.currentText()
        gsm_port = self._combo_gsm.currentText()
        mock_modbus = "True" if self._chk_mock_modbus.isChecked() else "False"

        # Update in memory
        os.environ["MODBUS_PORT"] = modbus_port
        os.environ["GSM_PORT"] = gsm_port
        os.environ["MOCK_MODBUS"] = mock_modbus
        os.environ["MOCK_HARDWARE"] = mock_modbus

        # Save to .env
        try:
            from dotenv import set_key
            base_dir = os.environ.get("OVOMATRIX_BASE_DIR", "")
            if not base_dir:
                base_dir = str(Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent)
            env_file = Path(base_dir) / ".env"
            set_key(str(env_file), "MODBUS_PORT", modbus_port)
            set_key(str(env_file), "GSM_PORT", gsm_port)
            set_key(str(env_file), "MOCK_MODBUS", mock_modbus)
            set_key(str(env_file), "MOCK_HARDWARE", mock_modbus)
            
            QMessageBox.information(
                self, "Saved",
                "✅ Hardware Configuration updated successfully.\n\n"
                "Please restart the application for port changes to take full effect on the modbus layer."
            )
        except ImportError:
            QMessageBox.warning(self, "Warning", "python-dotenv not installed. Changes applied in memory only.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to write to .env file:\n{e}")

    def _apply_phone_numbers(self):
        phones = self._edit_phones.text().strip()
        if not phones:
            QMessageBox.warning(self, "Invalid", "Please enter at least one phone number.")
            return

        # Update environment variable in memory
        os.environ["ALERT_PHONE_NUMBERS"] = phones
        
        # Save to .env file dynamically
        try:
            from dotenv import set_key
            base_dir = os.environ.get("OVOMATRIX_BASE_DIR", "")
            if not base_dir:
                base_dir = str(Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent)
            env_file = Path(base_dir) / ".env"
            set_key(str(env_file), "ALERT_PHONE_NUMBERS", phones)
            QMessageBox.information(
                self, "Saved",
                f"✅ SMS recipients updated successfully.\n\n"
                f"Numbers: {phones}\n"
                f"Note: Backend will pick this up on its next connection."
            )
        except ImportError:
            QMessageBox.warning(self, "Warning", "python-dotenv not installed. Changes applied in memory only.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to write to .env file:\n{e}")

    # ─────────────────────────────────────────────────────────────────────────
    # TAB 3 — AI Advisor
    # ─────────────────────────────────────────────────────────────────────────
    def _build_ai_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        grp = QGroupBox("AI Expert Configuration")
        form = QVBoxLayout(grp)

        # Provider + API Key row
        r1 = QHBoxLayout()
        r1.addWidget(QLabel("Provider:"))
        self._combo_provider = QComboBox()
        self._combo_provider.addItems(["Gemini", "ChatGPT", "Claude"])
        self._combo_provider.setFixedWidth(120)
        r1.addWidget(self._combo_provider)
        
        r1.addSpacing(16)
        r1.addWidget(QLabel("Lang:"))
        self._combo_lang = QComboBox()
        self._combo_lang.addItems(["English", "Arabic", "French", "Spanish"])
        self._combo_lang.setFixedWidth(100)
        r1.addWidget(self._combo_lang)

        r1.addSpacing(16)
        r1.addWidget(QLabel("API Key:"))
        self._edit_api_key = QLineEdit()
        self._edit_api_key.setEchoMode(QLineEdit.EchoMode.Password)
        self._edit_api_key.setPlaceholderText("Paste your API key here…")
        self._edit_api_key.setText(os.environ.get("GEMINI_API_KEY", ""))
        r1.addWidget(self._edit_api_key, stretch=1)
        form.addLayout(r1)

        # System role
        form.addWidget(QLabel("System Role (context prompt):"))
        self._edit_role = QTextEdit(
            "You are an expert poultry farm advisor specialising in Ross 308 broiler welfare. "
            "Provide concise, actionable 2-3 sentence technical advice based on the current sensor metrics."
        )
        self._edit_role.setFixedHeight(70)
        form.addWidget(self._edit_role)
        layout.addWidget(grp)

        # Action buttons
        btn_row = QHBoxLayout()

        self._btn_ask = QPushButton("🧠  Consult AI")
        self._btn_ask.setMinimumHeight(44)
        self._btn_ask.setStyleSheet(f"color: {DS.ACCENT_BLUE}; border: 1px solid {DS.ACCENT_BLUE};")
        self._btn_ask.clicked.connect(self._ask_ai)
        btn_row.addWidget(self._btn_ask)

        btn_clear = QPushButton("🗑  Clear AI Panel")
        btn_clear.setMinimumHeight(44)
        btn_clear.setStyleSheet(f"color: {DS.TEXT_MUTED}; border: 1px solid {DS.BORDER};")
        btn_clear.clicked.connect(self._clear_ai_panel)
        btn_row.addWidget(btn_clear)
        layout.addLayout(btn_row)

        info = QLabel("The AI response will appear in the AI Advisor panel on the main dashboard.")
        info.setStyleSheet(f"color: {DS.TEXT_MUTED}; font-size: 10px;")
        layout.addWidget(info)

        layout.addStretch()
        self.tabs.addTab(tab, "🤖  AI Advisor")

    # ─────────────────────────────────────────────────────────────────────────
    # TAB 4 — History Hub
    # ─────────────────────────────────────────────────────────────────────────
    def _build_history_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(8)

        # Alert log table
        grp_alert = QGroupBox("Alert Incident Log (last 100)")
        vb = QVBoxLayout(grp_alert)
        self._tbl_alerts = QTableWidget(0, 4)
        self._tbl_alerts.setHorizontalHeaderLabels(["Time", "Level", "NH₃ ppm", "Action"])
        self._tbl_alerts.horizontalHeader().setStretchLastSection(True)
        self._tbl_alerts.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._tbl_alerts.setAlternatingRowColors(True)
        self._tbl_alerts.verticalHeader().setVisible(False)
        vb.addWidget(self._tbl_alerts)
        layout.addWidget(grp_alert)

        # AI advice log table
        grp_ai = QGroupBox("AI Advice Archive (last 20)")
        vb2 = QVBoxLayout(grp_ai)
        self._tbl_ai = QTableWidget(0, 2)
        self._tbl_ai.setHorizontalHeaderLabels(["Time", "Advice Summary"])
        self._tbl_ai.horizontalHeader().setStretchLastSection(True)
        self._tbl_ai.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._tbl_ai.setAlternatingRowColors(True)
        self._tbl_ai.verticalHeader().setVisible(False)
        self._tbl_ai.setMaximumHeight(160)
        vb2.addWidget(self._tbl_ai)
        layout.addWidget(grp_ai)

        # Refresh button
        btn_ref = QPushButton("🔄  Refresh History")
        btn_ref.clicked.connect(self._refresh_history)
        layout.addWidget(btn_ref)

        self.tabs.addTab(tab, "📜  History Hub")
        # Auto-populate on open
        QTimer.singleShot(300, self._refresh_history)

    # ─────────────────────────────────────────────────────────────────────────
    # TAB 5 — Maintenance
    # ─────────────────────────────────────────────────────────────────────────
    def _build_maintenance_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(12)

        # Factory Reset
        grp_reset = QGroupBox("Factory Reset")
        vb = QVBoxLayout(grp_reset)
        info = QLabel(
            "Permanently deletes ALL telemetry readings and alert history.\n"
            "The dashboard will reset to 0.  This cannot be undone."
        )
        info.setWordWrap(True)
        info.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        vb.addWidget(info)
        btn_reset = QPushButton("🗑  Factory Reset  –  Clear All Data")
        btn_reset.setMinimumHeight(46)
        btn_reset.setStyleSheet(f"color: {DS.ACCENT_RED}; border: 2px solid {DS.ACCENT_RED};")
        btn_reset.clicked.connect(self._factory_reset)
        vb.addWidget(btn_reset)
        layout.addWidget(grp_reset)

        # Manual Pulse
        grp_pulse = QGroupBox("Diagnostic  —  Manual DB Pulse")
        vb2 = QVBoxLayout(grp_pulse)
        info2 = QLabel(
            "Inserts one mock reading (NH₃=12.5 ppm, Temp=24.8°C, Humid=55%) directly into the "
            "database, bypassing MQTT entirely.\n"
            "Use this to verify the dashboard is connected and reading from the correct database."
        )
        info2.setWordWrap(True)
        info2.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        vb2.addWidget(info2)
        btn_pulse = QPushButton("💾  Send Manual Pulse  →  DB")
        btn_pulse.setMinimumHeight(46)
        btn_pulse.setStyleSheet(f"color: {DS.ACCENT_BLUE}; border: 1px solid {DS.ACCENT_BLUE};")
        btn_pulse.clicked.connect(self._manual_pulse)
        vb2.addWidget(btn_pulse)
        layout.addWidget(grp_pulse)

        layout.addStretch()
        self.tabs.addTab(tab, "⚙  Maintenance")

    # ─────────────────────────────────────────────────────────────────────────
    # SIMULATOR ACTIONS
    # ─────────────────────────────────────────────────────────────────────────
    def _write_control_file(self, running: bool, mode: str = "NORMAL"):
        """Write sim_control.json — reliable IPC to the Simulator process."""
        base = os.environ.get("OVOMATRIX_BASE_DIR", ".")
        ctrl_file = Path(base) / "sim_control.json"
        try:
            ctrl_file.write_text(
                json.dumps({"running": running, "mode": mode}),
                encoding="utf-8"
            )
        except Exception as e:
            print(f"[SimCtrl] Could not write control file: {e}")

    def _toggle_simulator(self):
        if not self._sim_active:
            # START
            self._sim_active = True
            self._btn_toggle.setText("⏹  Stop Simulator")
            self._btn_toggle.setStyleSheet(f"color: {DS.ACCENT_RED}; border: 2px solid {DS.ACCENT_RED};")
            self._lbl_sim_status.setText("Status: RUNNING  —  Mode: NORMAL")
            self._lbl_sim_status.setStyleSheet(f"color: {DS.ACCENT_GREEN}; font-size: 10px;")
            self._write_control_file(running=True,  mode="NORMAL")
            # Send START then SET_SCENARIO in order
            def _send_start():
                host  = os.environ.get("MQTT_BROKER_HOST", "localhost")
                port  = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
                topic = "farm/telemetry/control"
                for raw in [json.dumps({"command": "START"}), json.dumps({"command": "SET_SCENARIO", "mode": "NORMAL"})]:
                    for attempt in range(5):
                        try:
                            mqtt_publish.single(topic, payload=raw, hostname=host, port=port, keepalive=10)
                            break
                        except Exception:
                            if attempt < 4:
                                time.sleep(0.5)
                    time.sleep(0.1)
            threading.Thread(target=_send_start, daemon=True).start()
        else:
            # STOP
            self._sim_active = False
            self._btn_toggle.setText("▶  Start Simulator (NORMAL mode)")
            self._btn_toggle.setStyleSheet(f"color: {DS.ACCENT_GREEN}; border: 2px solid {DS.ACCENT_GREEN};")
            self._lbl_sim_status.setText("Status: STOPPED  —  Press Start to begin telemetry simulation.")
            self._lbl_sim_status.setStyleSheet(f"color: {DS.TEXT_MUTED}; font-size: 10px;")
            self._write_control_file(running=False, mode="NORMAL")
            self._mqtt_cmd({"command": "STOP"})

    def _set_scenario(self, mode: str):
        """Force a scenario. Auto-starts simulator if not running."""
        if not self._sim_active:
            self._sim_active = True
            self._btn_toggle.setText("⏹  Stop Simulator")
            self._btn_toggle.setStyleSheet(f"color: {DS.ACCENT_RED}; border: 2px solid {DS.ACCENT_RED};")

        colours = {"NORMAL": DS.ACCENT_GREEN, "WARNING": DS.ACCENT_YELLOW, "EMERGENCY": DS.ACCENT_RED}
        colour  = colours.get(mode, DS.ACCENT_BLUE)
        self._lbl_sim_status.setText(f"Status: RUNNING  —  Mode: {mode}")
        self._lbl_sim_status.setStyleSheet(f"color: {colour}; font-size: 10px;")

        self._write_control_file(running=True, mode=mode)

        # Send all 3 commands in ONE thread to guarantee ordering:
        # 1. START (un-pauses the bridge)
        # 2. SET_SCENARIO (changes the mock values)
        # 3. CLEAR_HYSTERESIS (resets alert engine buffer + bypasses cooldown)
        def _send_all():
            host  = os.environ.get("MQTT_BROKER_HOST", "localhost")
            port  = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
            topic = "farm/telemetry/control"
            user  = os.environ.get("MQTT_USER", "")
            pw    = os.environ.get("MQTT_PASSWORD", "")
            auth  = {'username': user, 'password': pw} if user else None

            commands = [
                json.dumps({"command": "START"}),
                json.dumps({"command": "SET_SCENARIO", "mode": mode}),
                json.dumps({"command": "CLEAR_HYSTERESIS"}),
            ]
            for raw in commands:
                for attempt in range(5):
                    try:
                        mqtt_publish.single(
                            topic, payload=raw,
                            hostname=host, port=port,
                            auth=auth, keepalive=10
                        )
                        break
                    except Exception as e:
                        if attempt < 4:
                            time.sleep(0.5)
                        else:
                            print(f"📡 MQTT send error: {e}")
                time.sleep(0.1)  # small gap between commands

        threading.Thread(target=_send_all, daemon=True).start()

    def _simulate_power_loss(self):
        reply = QMessageBox.question(
            self, "Confirm",
            "Send a LOW_BATTERY payload to simulate a power outage?\n"
            "This triggers UPS shutdown logic.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        # Send the command to the CONTROL topic so the UPSMonitorWorker can trigger
        # an immediate mock outage.
        self._mqtt_cmd({"command": "SIMULATE_POWER_LOSS"})
        QMessageBox.information(self, "Sent", "⚡ Power loss simulation command sent to backend.")

    def _mqtt_cmd(self, payload: dict):
        """Send a command to the simulator via MQTT (non-blocking, with retry)."""
        host  = os.environ.get("MQTT_BROKER_HOST", "localhost")
        port  = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
        topic = "farm/telemetry/control"
        raw   = json.dumps(payload)
        
        # SEC-04: MQTT Authentication
        user = os.environ.get("MQTT_USER", "")
        pw   = os.environ.get("MQTT_PASSWORD", "")
        auth = {'username': user, 'password': pw} if user else None

        def _pub():
            # Retry 5 times over 5 seconds
            for attempt in range(5):
                try:
                    mqtt_publish.single(
                        topic, payload=raw,
                        hostname=host, port=port,
                        auth=auth,
                        keepalive=10
                    )
                    return  # success
                except Exception as e:
                    if attempt < 4:
                        time.sleep(1.0)
                    else:
                        print(f"📡 MQTT Config Command Error: {e}")
        
        threading.Thread(target=_pub, daemon=True).start()

    # ─────────────────────────────────────────────────────────────────────────
    # HARDWARE STATUS
    # ─────────────────────────────────────────────────────────────────────────
    def _on_hw_status(self, device: str, is_ok: bool):
        if device in self._hw_leds:
            self._hw_leds[device].set_status(is_ok)

    # ─────────────────────────────────────────────────────────────────────────
    # AI ADVISOR ACTIONS
    # ─────────────────────────────────────────────────────────────────────────
    def _ask_ai(self):
        if not self._db:
            QMessageBox.warning(self, "No DB", "Database not connected.")
            return

        # Fetch last 10 readings for context
        try:
            with self._db._lock:
                conn = self._db._connect()
                cursor = conn.execute(
                    "SELECT nh3_ppm, temp_c, humid_pct FROM readings ORDER BY id DESC LIMIT 10"
                )
                rows = [dict(r) for r in cursor.fetchall()]
                conn.close()
            readings = list(reversed(rows))
        except Exception as e:
            readings = []
            print(f"[AI] Trend fetch error: {e}")

        if not readings:
            self._deliver_ai_text("⚠️ Not Ready: No telemetry data in the database yet.\nVerify hardware sensors are connected and wait a few seconds, then try again.")
            return

        # Build enriched prompt
        latest    = readings[-1]
        nh3_avg   = sum(r["nh3_ppm"]   for r in readings) / len(readings)
        temp_avg  = sum(r["temp_c"]    for r in readings) / len(readings)
        humid_avg = sum(r["humid_pct"] for r in readings) / len(readings)

        if len(readings) >= 6:
            early = sum(r["nh3_ppm"] for r in readings[:5]) / 5
            late  = sum(r["nh3_ppm"] for r in readings[5:]) / len(readings[5:])
            if late > early + 1.0:   trend = "📈 RISING"
            elif late < early - 1.0: trend = "📉 FALLING"
            else:                    trend = "→ STABLE"
        else:
            trend = "— (insufficient data)"

        context = self._edit_role.toPlainText()
        lang = self._combo_lang.currentText()
        prompt = (
            f"{context}\n\n"
            "ROSS 308 REFERENCE STANDARDS:\n"
            "  NH₃ ideal < 10 ppm  |  Critical > 20 ppm  |  Emergency > 35 ppm\n"
            "  Temperature: ~24°C  |  Humidity: 50–70%\n\n"
            "CURRENT FARM STATUS\n"
            f"  NH₃:      {latest['nh3_ppm']:.2f} ppm  (10-sample avg: {nh3_avg:.2f} ppm,  trend: {trend})\n"
            f"  Temp:     {latest['temp_c']:.2f}°C    (avg: {temp_avg:.2f}°C)\n"
            f"  Humidity: {latest['humid_pct']:.1f}%  (avg: {humid_avg:.1f}%)\n\n"
            "Provide exactly 3 sentences: (1) risk assessment, (2) root cause, (3) one specific corrective action.\n"
            f"IMPORTANT: You MUST respond in the following language: {lang}."
        )

        provider = self._combo_provider.currentText()
        api_key  = self._edit_api_key.text().strip()

        self._btn_ask.setEnabled(False)
        self._btn_ask.setText("⏳  Consulting AI…")

        threading.Thread(
            target=self._call_ai,
            args=(provider, api_key, prompt),
            daemon=True
        ).start()

    def _call_ai(self, provider: str, api_key: str, prompt: str):
        if not api_key:
            self._deliver_ai_text("❌ No API key provided. Enter your key in the AI Advisor tab.")
            return

        try:
            answer = ""
            if provider == "Gemini":
                url = (
                    f"https://generativelanguage.googleapis.com/v1beta/"
                    f"models/gemini-1.5-flash:generateContent?key={api_key}"
                )
                resp = requests.post(
                    url,
                    headers={"Content-Type": DS.APP_JSON},
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                    timeout=15,
                )
                if resp.status_code in (400, 403) and (
                    "API_KEY_INVALID" in resp.text or "API key" in resp.text
                ):
                    answer = "❌ Gemini: API key is invalid or not activated."
                else:
                    resp.raise_for_status()
                    answer = resp.json()["candidates"][0]["content"]["parts"][0]["text"]

            elif provider == "ChatGPT":
                resp = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": DS.APP_JSON,
                    },
                    json={"model": "gpt-3.5-turbo", "messages": [{"role": "user", "content": prompt}]},
                    timeout=15,
                )
                if resp.status_code == 401:
                    answer = "❌ ChatGPT: Invalid OpenAI API key."
                else:
                    resp.raise_for_status()
                    answer = resp.json()["choices"][0]["message"]["content"]

            elif provider == "Claude":
                resp = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key":         api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type":      DS.APP_JSON,
                    },
                    json={
                        "model":     "claude-3-haiku-20240307",
                        "max_tokens": 256,
                        "messages":  [{"role": "user", "content": prompt}],
                    },
                    timeout=15,
                )
                if resp.status_code == 401:
                    answer = "❌ Claude: Invalid Anthropic API key."
                else:
                    resp.raise_for_status()
                    answer = resp.json()["content"][0]["text"]

            self._deliver_ai_text(answer)

        except Exception as exc:
            msg = str(exc)
            if any(code in msg for code in ("401", "403", "invalid")):
                self._deliver_ai_text(f"❌ Authentication error: {msg}")
            else:
                self._deliver_ai_text(f"⚠️ API Error: {msg}")

    def _deliver_ai_text(self, text: str):
        """Save to DB and push to main dashboard — thread-safe."""
        # Save to DB
        if self._db:
            try:
                self._db.insert_ai_advice(text)
            except Exception as e:
                print(f"[AI] DB save error: {e}")

        # Push to parent MainWindow
        parent = self.parent()
        if parent and hasattr(parent, "update_expert_advice"):
            QMetaObject.invokeMethod(
                parent, "update_expert_advice",
                Qt.ConnectionType.QueuedConnection,
                Q_ARG(str, text),
            )

        # Re-enable button
        QMetaObject.invokeMethod(
            self._btn_ask, "setText",
            Qt.ConnectionType.QueuedConnection,
            Q_ARG(str, "🧠  Consult AI"),
        )
        QMetaObject.invokeMethod(
            self._btn_ask, "setEnabled",
            Qt.ConnectionType.QueuedConnection,
            Q_ARG(bool, True),
        )

    def _clear_ai_panel(self):
        """Clear the AI panel on the main dashboard."""
        parent = self.parent()
        if parent and hasattr(parent, "update_expert_advice"):
            QMetaObject.invokeMethod(
                parent, "update_expert_advice",
                Qt.ConnectionType.QueuedConnection,
                Q_ARG(str, ""),
            )
        QMessageBox.information(self, "Cleared", "AI Advisor panel cleared.")

    # ─────────────────────────────────────────────────────────────────────────
    # HISTORY HUB
    # ─────────────────────────────────────────────────────────────────────────
    def _refresh_history(self):
        if not self._db:
            return

        # ── Alert log ────────────────────────────────────────────────────────
        try:
            rows = self._db.get_full_alert_history(100)
            self._tbl_alerts.setRowCount(0)
            for i, r in enumerate(rows):
                self._tbl_alerts.insertRow(i)
                ts = str(r.get("timestamp", ""))
                ts = ts.split("T")[1][:8] if "T" in ts else ts
                self._tbl_set(self._tbl_alerts, i, 0, ts)
                self._tbl_set(self._tbl_alerts, i, 1, str(r.get("level", "")))
                self._tbl_set(self._tbl_alerts, i, 2, f"{r.get('nh3_ppm', 0):.2f}")
                self._tbl_set(self._tbl_alerts, i, 3, str(r.get("action_taken", "")))
        except Exception as e:
            print(f"[History] Alert fetch error: {e}")

        # ── AI advice log ─────────────────────────────────────────────────────
        try:
            ai_rows = self._db.get_ai_history(20)
            self._tbl_ai.setRowCount(0)
            for i, r in enumerate(ai_rows):
                self._tbl_ai.insertRow(i)
                ts = str(r.get("timestamp", ""))
                ts = ts.split("T")[1][:8] if "T" in ts else ts
                self._tbl_set(self._tbl_ai, i, 0, ts)
                advice = str(r.get("advice", ""))[:120] + ("…" if len(str(r.get("advice", ""))) > 120 else "")
                self._tbl_set(self._tbl_ai, i, 1, advice)
        except Exception as e:
            print(f"[History] AI fetch error: {e}")

    @staticmethod
    def _tbl_set(table: QTableWidget, row: int, col: int, text: str):
        item = QTableWidgetItem(text)
        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        table.setItem(row, col, item)

    # ─────────────────────────────────────────────────────────────────────────
    # MAINTENANCE ACTIONS
    # ─────────────────────────────────────────────────────────────────────────
    def _factory_reset(self):
        reply = QMessageBox.critical(
            self, "⚠️ Confirm Factory Reset",
            "This will PERMANENTLY DELETE all telemetry readings and alert history.\n\n"
            "The dashboard will reset to zero.\n\n"
            "This action CANNOT be undone. Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        if not self._db:
            QMessageBox.warning(self, "Error", "Database not available.")
            return

        try:
            # 1. Clear SQL tables (readings, alerts, AI advice)
            self._db.clear_data()
            
            # 2. Notify Simulator to reset its drift baseline
            self._mqtt_cmd({"command": "RESET"})
            self._write_control_file(running=False, mode="NORMAL")
            self._sim_active = False
            self._btn_toggle.setText("▶  Start Simulator")
            
            # 3. Update UI (MainWindow)
            parent = self.parent()
            if parent and hasattr(parent, "reset_dashboard"):
                parent.reset_dashboard()
            QMessageBox.information(
                self, "Done",
                "✅ Database cleared successfully.\n"
                "The dashboard has been reset to zero.",
            )
            # Also refresh history tab
            self._refresh_history()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Factory reset failed:\n{e}")

    def _manual_pulse(self):
        """Insert one test reading directly into DB (bypasses MQTT)."""
        if not self._db:
            QMessageBox.warning(self, "Error", "Database not available.")
            return

        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
            "nh3_ppm":   12.5,
            "temp_c":    24.8,
            "humid_pct": 55.0,
            "source":    "manual_pulse",
            "uuid":      str(uuid.uuid4()),
        }
        try:
            inserted = self._db.insert_reading(payload)
            if inserted:
                QMessageBox.information(
                    self, "Pulse Sent ✅",
                    "Reading injected directly into the database:\n"
                    "  NH₃  = 12.50 ppm\n"
                    "  Temp = 24.8°C\n"
                    "  RH   = 55%\n\n"
                    "The dashboard should update within 1–2 seconds.\n"
                    "If it does not, check the DB path in the logs.",
                )
            else:
                QMessageBox.warning(self, "Warning", "Insert returned False (possible UUID collision).")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Manual pulse failed:\n{e}")

    # ─────────────────────────────────────────────────────────────────────────
    # LIFECYCLE
    # ─────────────────────────────────────────────────────────────────────────
    def closeEvent(self, event):
        self._hw_thread.stop()
        self._hw_thread.wait(3000)
        super().closeEvent(event)

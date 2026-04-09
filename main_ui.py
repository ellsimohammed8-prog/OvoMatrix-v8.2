"""
OvoMatrix v8.0 — Phase 2, Step 7
main_ui.py: Industrial-Grade Dashboard (PyQt6)

Architecture — Dumb Client Contract:
  ┌─────────────────────────────────────────────────────┐
  │  main_ui.py (READ-ONLY)                             │
  │                                                     │
  │  DataPoller (QThread) ──► SQLite (ovomatrix.db)    │
  │       │                        READ-ONLY            │
  │       │ pyqtSignal (bounded Queue=100)              │
  │       ▼                                             │
  │  MainWindow (GUI thread)                            │
  │    • MetricCard × 3 (NH₃ / Temp / Humidity)        │
  │    • LiveChart (last 60 readings per sensor)        │
  │    • AlertBanner (dynamic level colour)             │
  │    • AlertLogPanel (last 10 events)                 │
  │    • StatusBar                                      │
  └─────────────────────────────────────────────────────┘

This module NEVER writes to the database, never sends GSM commands,
and never calls alert_engine logic.  It is a pure read-only viewport.

Dependencies:
  pip install PyQt6 pyqtgraph
  (pyqtgraph provides hardware-accelerated real-time charts)

Run (UI only — backend must be running separately):
  python main_ui.py
  python main_ui.py --db path/to/ovomatrix.db
"""

from __future__ import annotations

import argparse
import logging
import math
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from queue import Full, Queue
from typing import Optional

from PyQt6.QtCore import (
    QEasingCurve,
    QPoint,
    QPropertyAnimation,
    QRect,
    QSize,
    Qt,
    QThread,
    QTimer,
    pyqtSignal,
    pyqtSlot,
)
from PyQt6.QtGui import (
    QColor,
    QFont,
    QFontDatabase,
    QIcon,
    QLinearGradient,
    QPainter,
    QPainterPath,
    QPalette,
    QPen,
    QPixmap,
)
from PyQt6.QtWidgets import (
    QApplication,
    QFrame,
    QGraphicsOpacityEffect,
    QGraphicsDropShadowEffect,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpacerItem,
    QStatusBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

# Optional: pyqtgraph for hardware-accelerated charts.
try:
    import pyqtgraph as pg
    _PYQTGRAPH_AVAILABLE = True
    pg.setConfigOptions(antialias=True, foreground="w", background=None)
except ImportError:
    _PYQTGRAPH_AVAILABLE = False

APP_LOGO_DATA=b'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'


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [UI] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("ovomatrix.ui")

# ---------------------------------------------------------------------------
# Design System — colours, fonts, dimensions
# ---------------------------------------------------------------------------

class DS:
    """Design-system constants — single source of truth for every colour/size."""

    # ── Palette ─────────────────────────────────────────────────────────────
    BG_VOID        = "#05080f"    # absolute background
    BG_PANEL       = "#0b1120"    # card / panel background
    BG_PANEL_ALT   = "#0f1829"    # alternate panel (chart bg)
    BORDER         = "#1a2744"    # default border
    BORDER_BRIGHT  = "#2a3d66"    # highlighted border

    TEXT_PRIMARY   = "#e8f0fe"    # primary text
    TEXT_SECONDARY = "#8899bb"    # secondary / label text
    TEXT_MUTED     = "#445577"    # muted / disabled

    ACCENT_BLUE    = "#3d82f4"    # healthy accent
    ACCENT_GLOW    = "#1a3d80"    # blue glow ring

    # PER-LEVEL alert colours
    LEVEL_COLOURS = {
        "NORMAL":    {"bg": "#05080f", "border": "#1a2744",  "text": "#3d82f4", "accent": "#1a3d80"},
        "WARNING":   {"bg": "#0f0c00", "border": "#554400",  "text": "#f5c400", "accent": "#332800"},
        "CRITICAL":  {"bg": "#100500", "border": "#662200",  "text": "#ff6a00", "accent": "#3a1100"},
        "EMERGENCY": {"bg": "#150000", "border": "#880000",  "text": "#ff1a1a", "accent": "#4a0000"},
    }

    # ── Sensor colours (chart lines) ─────────────────────────────────────────
    COLOUR_NH3    = "#ff4d6d"    # red-pink  for NH₃
    COLOUR_TEMP   = "#ffc94d"    # amber     for temperature
    COLOUR_HUMID  = "#4dc9ff"    # cyan      for humidity

    # ── NH₃ level thresholds (for card progress bar colouring) ───────────────
    NH3_NORMAL   = 20.0
    NH3_WARNING  = 35.0
    NH3_CRITICAL = 50.0
    NH3_MAX_DISPLAY = 80.0   # clip beyond this for display

    # ── Metrics ──────────────────────────────────────────────────────────────
    CARD_HEIGHT    = 180
    GRAPH_HEIGHT   = 280
    CORNER_RADIUS  = 14
    BORDER_WIDTH   = 2

    # ── Fonts ────────────────────────────────────────────────────────────────
    FONT_FAMILY    = "Segoe UI"
    FONT_MONO      = "Consolas"

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

class TelemetrySnapshot:
    """Immutable data container for one reading cycle."""
    __slots__ = ("nh3_ppm", "temp_c", "humid_pct", "timestamp", "source", "uuid")

    def __init__(self, row: dict) -> None:
        self.nh3_ppm   = float(row.get("nh3_ppm",   0.0))
        self.temp_c    = float(row.get("temp_c",    0.0))
        self.humid_pct = float(row.get("humid_pct", 0.0))
        self.timestamp = row.get("timestamp", "")
        self.source    = row.get("source",    "—")
        self.uuid      = row.get("uuid",      "")


class AlertSnapshot:
    """Last alert level and metadata."""
    __slots__ = ("level", "nh3_ppm", "temp_c", "action_taken",
                 "timestamp", "suppressed")

    def __init__(self, row: Optional[dict] = None) -> None:
        if row:
            self.level        = str(row.get("level",        "NORMAL"))
            self.nh3_ppm      = float(row.get("nh3_ppm",    0.0))
            self.temp_c       = float(row.get("temp_c",     0.0))
            self.action_taken = str(row.get("action_taken", ""))
            self.timestamp    = str(row.get("timestamp",    ""))
            self.suppressed   = int(row.get("suppressed",   0))
        else:
            self.level        = "NORMAL"
            self.nh3_ppm      = 0.0
            self.temp_c       = 0.0
            self.action_taken = "System initialising …"
            self.timestamp    = ""
            self.suppressed   = 0

# ---------------------------------------------------------------------------
# DataPoller — background QThread for all SQLite reads
# ---------------------------------------------------------------------------

class DataPoller(QThread):
    """
    Read-only SQLite polling thread.

    Emits ``reading_ready`` every polling cycle if a new reading is found.
    Emits ``alert_ready`` whenever the most recent non-suppressed alert changes.
    Emits ``connection_changed`` when the DB becomes available or unavailable.

    Uses a bounded ``Queue(100)`` to prevent memory pressure when the GUI
    thread is slower than the polling rate.
    """

    reading_ready:      pyqtSignal = pyqtSignal(object)   # TelemetrySnapshot
    alert_ready:        pyqtSignal = pyqtSignal(object)   # AlertSnapshot
    ups_ready:          pyqtSignal = pyqtSignal(object)   # dict
    connection_changed: pyqtSignal = pyqtSignal(bool, str)# (ok, reason)
    history_ready:      pyqtSignal = pyqtSignal(object)   # list[TelemetrySnapshot]

    _POLL_INTERVAL: float = 1.0         # seconds
    _HISTORY_LIMIT: int   = 60          # readings to keep per sensor
    _HISTORY_BOOT_FETCH: int = 60       # initial fetch on startup

    def __init__(self, db_path: str, parent: Optional[QThread] = None) -> None:
        super().__init__(parent)
        self._db_path         = db_path
        self._stop_flag       = False
        self._queue: Queue    = Queue(maxsize=100)
        self._last_uuid:  str = ""
        self._last_alert_id:int = -1
        self._last_ups_ts: str = ""
        self._conn: Optional[sqlite3.Connection] = None

    # ── Public control ──────────────────────────────────────────────────────

    def stop(self) -> None:
        self._stop_flag = True
        self.quit()

    def reset_state(self) -> None:
        """Reset internal tracking IDs to force immediate data refresh after DB clear."""
        self._last_uuid = ""
        self._last_alert_id = -1
        self._last_ups_ts = ""
        logger.info("DataPoller internal state tracking reset.")

    # ── QThread entry-point ─────────────────────────────────────────────────

    def run(self) -> None:
        logger.info("DataPoller thread started | DB=%s", self._db_path)
        self._connect()
        # self._prefetch_history() # DISABLED: User requests cold-start (zero values)
        
        self._set_initial_state()

        while not self._stop_flag:
            t0 = time.monotonic()
            self._poll_once()
            elapsed = time.monotonic() - t0
            sleep_ms = max(0, int((self._POLL_INTERVAL - elapsed) * 1000))
            self.msleep(sleep_ms)

        self._close()
        logger.info("DataPoller thread stopped.")

    # ── Connection management ───────────────────────────────────────────────

    def _connect(self) -> None:
        try:
            self._conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES,
            )
            self._conn.row_factory = sqlite3.Row
            # Read-only PRAGMAs for maximum safety.
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA query_only=ON;")
            self.connection_changed.emit(True, "Connected")
            logger.info("DataPoller: DB connection established.")
        except sqlite3.Error as exc:
            self._conn = None
            self.connection_changed.emit(False, str(exc))
            logger.error("DataPoller: DB connect failed: %s", exc)

    def _close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
            self._conn = None

    def _reconnect_if_needed(self) -> bool:
        if self._conn:
            return True
        self._connect()
        return self._conn is not None

    # ── Data fetching ───────────────────────────────────────────────────────

    def _set_initial_state(self) -> None:
        """Fetch the latest UUID/Alert ID so old DB history is ignored on startup."""
        if not self._reconnect_if_needed():
            return
        try:
            # Seed last reading UUID
            cur = self._conn.execute("SELECT uuid FROM readings ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                self._last_uuid = str(row["uuid"])
                
            # Seed last alert ID
            cur = self._conn.execute("SELECT id FROM alert_log ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                self._last_alert_id = int(row["id"])
        except sqlite3.Error as exc:
            logger.warning("DataPoller: _set_initial_state failed: %s", exc)

    def _prefetch_history(self) -> None:
        """Fetch the last N readings on startup to populate the chart immediately."""
        if not self._reconnect_if_needed():
            return
        try:
            cursor = self._conn.execute(
                """
                SELECT id, timestamp, nh3_ppm, temp_c, humid_pct, source, uuid
                FROM   readings
                ORDER  BY id DESC
                LIMIT  ?
                """,
                (self._HISTORY_BOOT_FETCH,),
            )
            rows = cursor.fetchall()
            snaps = [TelemetrySnapshot(dict(r)) for r in reversed(rows)]
            if snaps:
                self.history_ready.emit(snaps)
        except sqlite3.Error as exc:
            logger.warning("DataPoller: prefetch failed: %s", exc)

    def _poll_once(self) -> None:
        if not self._reconnect_if_needed():
            self.connection_changed.emit(False, "DB unreachable")
            time.sleep(3.0)
            return

        # ── Latest reading ──────────────────────────────────────────────────
        try:
            cur = self._conn.execute(
                """
                SELECT id, timestamp, nh3_ppm, temp_c, humid_pct, source, uuid
                FROM   readings
                ORDER  BY id DESC
                LIMIT  1
                """
            )
            row = cur.fetchone()
            if row:
                row_dict = dict(row)
                if row_dict.get("uuid") != self._last_uuid:
                    self._last_uuid = row_dict.get("uuid", "")
                    snap = TelemetrySnapshot(row_dict)
                    try:
                        self._queue.put_nowait(snap)
                        self.reading_ready.emit(snap)
                    except Full:
                        logger.debug("DataPoller: reading queue full — drop oldest.")
        except sqlite3.Error as exc:
            logger.warning("DataPoller: reading query error: %s", exc)
            self._close()
            return

        # ── Latest active (non-suppressed) alert ─────────────────────────────
        try:
            cur = self._conn.execute(
                """
                SELECT id, timestamp, level, nh3_ppm, temp_c, action_taken, suppressed
                FROM   alert_log
                ORDER  BY id DESC
                LIMIT  1
                """
            )
            row = cur.fetchone()
            if row:
                row_dict = dict(row)
                if row_dict.get("id", -1) != self._last_alert_id:
                    self._last_alert_id = row_dict.get("id", -1)
                    self.alert_ready.emit(AlertSnapshot(row_dict))
        except sqlite3.Error as exc:
            logger.warning("DataPoller: alert query error: %s", exc)

        # ── Latest UPS status ───────────────────────────────────────────────
        try:
            cur = self._conn.execute("SELECT * FROM ups_status WHERE id = 1")
            row = cur.fetchone()
            if row:
                rd = dict(row)
                if rd.get("timestamp") != self._last_ups_ts:
                    self._last_ups_ts = rd.get("timestamp", "")
                    self.ups_ready.emit(rd)
        except sqlite3.Error:
            pass

    def fetch_recent_alerts(self, limit: int = 10) -> list[dict]:
        """Synchronous fetch for populating the alert log table (called once)."""
        if not self._conn:
            return []
        try:
            cur = self._conn.execute(
                """
                SELECT timestamp, level, nh3_ppm, temp_c, action_taken, suppressed
                FROM   alert_log
                ORDER  BY id DESC
                LIMIT  ?
                """,
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []

# ---------------------------------------------------------------------------
# MetricCard — individual sensor display widget
# ---------------------------------------------------------------------------

class MetricCard(QFrame):
    """
    A large, styled card displaying a single sensor metric.

    Visual anatomy:
      ┌────────────────────────────────┐
      │ SENSOR LABEL                   │
      │                                │
      │      12.45 ppm          ← value │
      │                                │
      │  ████████░░░░░░░  71%  ← bar   │
      │                                │
      │   min / max range label        │
      └────────────────────────────────┘
    """

    def __init__(
        self,
        title:    str,
        unit:     str,
        accent:   str,
        val_min:  float,
        val_max:  float,
        parent:   Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self._title   = title
        self._unit    = unit
        self._accent  = QColor(accent)
        self._val_min = val_min
        self._val_max = val_max
        self._value   = 0.0
        self._bar_pct = 0.0

        self.setMinimumHeight(DS.CARD_HEIGHT)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setObjectName("MetricCard")
        self._build_ui()
        self._apply_style()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 14, 20, 14)
        layout.setSpacing(4)

        # ── Title row ─────────────────────────────────────────────────────────
        self._lbl_title = QLabel(self._title)
        self._lbl_title.setFont(QFont(DS.FONT_FAMILY, 11, QFont.Weight.Medium))
        self._lbl_title.setStyleSheet(f"color: {DS.TEXT_SECONDARY};")
        layout.addWidget(self._lbl_title)

        # Push the value block toward the vertical centre of the card.
        layout.addStretch(1)

        # ── Value display ─────────────────────────────────────────────────────
        # AlignCenter keeps the number horizontally and vertically centred
        # within the label's own bounding rect.
        self._lbl_value = QLabel("—")
        self._lbl_value.setFont(QFont(DS.FONT_MONO, 34, QFont.Weight.Bold))
        self._lbl_value.setAlignment(
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter
        )
        self._lbl_value.setStyleSheet(f"color: {self._accent.name()};")
        layout.addWidget(self._lbl_value)

        # ── Unit label ────────────────────────────────────────────────────────
        self._lbl_unit = QLabel(self._unit)
        self._lbl_unit.setFont(QFont(DS.FONT_FAMILY, 11))
        self._lbl_unit.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        self._lbl_unit.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        layout.addWidget(self._lbl_unit)

        # Equal counter-stretch below the value block — centres it vertically.
        layout.addStretch(1)

        # ── Progress bar ──────────────────────────────────────────────────────
        self._bar_widget = _ProgressBar(accent=self._accent.name())
        self._bar_widget.setFixedHeight(6)
        layout.addWidget(self._bar_widget)

        # ── Range label ───────────────────────────────────────────────────────
        self._lbl_range = QLabel(f"{self._val_min} – {self._val_max} {self._unit}")
        self._lbl_range.setFont(QFont(DS.FONT_FAMILY, 8))
        self._lbl_range.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        self._lbl_range.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        layout.addWidget(self._lbl_range)

    def _apply_style(self) -> None:
        self.setStyleSheet(
            f"""
            QFrame#MetricCard {{
                background-color: {DS.BG_PANEL};
                border: {DS.BORDER_WIDTH}px solid {DS.BORDER};
                border-radius: {DS.CORNER_RADIUS}px;
            }}
            """
        )

    def update_value(self, value: float) -> None:
        self._value   = value
        pct           = max(0.0, min(1.0, (value - self._val_min) / max(self._val_max - self._val_min, 1)))
        self._bar_pct = pct
        self._lbl_value.setText(f"{value:,.2f}")
        self._bar_widget.set_pct(pct)

    def set_accent(self, hex_colour: str) -> None:
        self._accent = QColor(hex_colour)
        self._lbl_value.setStyleSheet(f"color: {hex_colour};")
        self._bar_widget.set_accent(hex_colour)

    def highlight_border(self, hex_colour: str) -> None:
        self.setStyleSheet(
            f"""
            QFrame#MetricCard {{
                background-color: {DS.BG_PANEL};
                border: {DS.BORDER_WIDTH}px solid {hex_colour};
                border-radius: {DS.CORNER_RADIUS}px;
            }}
            """
        )


class _ProgressBar(QWidget):
    def __init__(self, accent: str = DS.ACCENT_BLUE, parent=None):
        super().__init__(parent)
        self._pct    = 0.0
        self._accent = QColor(accent)
        self.setMinimumHeight(8)

    def set_pct(self, pct: float) -> None:
        self._pct = max(0.0, min(1.0, pct))
        self.update()

    def set_accent(self, hex_colour: str) -> None:
        self._accent = QColor(hex_colour)
        self.update()

    def paintEvent(self, _event) -> None:
        p   = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        w, h = self.width(), self.height()
        r    = h // 2

        # Background track
        p.setBrush(QColor(DS.BORDER))
        p.setPen(Qt.PenStyle.NoPen)
        p.drawRoundedRect(0, 0, w, h, r, r)

        # Filled portion
        filled = int(w * self._pct)
        if filled > 0:
            grad = QLinearGradient(0, 0, filled, 0)
            c    = self._accent
            grad.setColorAt(0.0, c.darker(130))
            grad.setColorAt(1.0, c)
            p.setBrush(grad)
            p.drawRoundedRect(0, 0, filled, h, r, r)

        p.end()


# ---------------------------------------------------------------------------
# AlertBanner — status strip that changes colour by alert level
# ---------------------------------------------------------------------------

class AlertBanner(QFrame):
    """
    Full-width banner at the top of the dashboard indicating current alert level.
    In EMERGENCY mode, the entire border flashes via a QTimer.
    """

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setFixedHeight(56)
        self.setObjectName("AlertBanner")
        self._level       = "NORMAL"
        self._flash_state = False

        self._build_ui()
        self._flash_timer  = QTimer(self)
        self._flash_timer.timeout.connect(self._toggle_flash)

        self.set_level("NORMAL", "System initialising …")

    def _build_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(24, 0, 24, 0)

        # Level badge
        self._badge = QLabel("● NORMAL")
        self._badge.setFont(QFont(DS.FONT_FAMILY, 13, QFont.Weight.Bold))
        layout.addWidget(self._badge)

        layout.addSpacerItem(QSpacerItem(16, 0))

        # Action / description text
        self._lbl_action = QLabel("")
        self._lbl_action.setFont(QFont(DS.FONT_FAMILY, 10))
        self._lbl_action.setStyleSheet(f"color: {DS.TEXT_SECONDARY};")
        self._lbl_action.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred
        )
        layout.addWidget(self._lbl_action)

        # Timestamp
        self._lbl_ts = QLabel("")
        self._lbl_ts.setFont(QFont(DS.FONT_MONO, 9))
        self._lbl_ts.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        layout.addWidget(self._lbl_ts)

    def set_level(self, level: str, action: str = "", ts: str = "") -> None:
        self._level = level.upper()
        colours     = DS.LEVEL_COLOURS.get(self._level, DS.LEVEL_COLOURS["NORMAL"])
        text_colour = colours["text"]
        border      = colours["border"]
        bg          = colours["accent"]

        self._badge.setText(f"● {self._level}")
        self._badge.setStyleSheet(f"color: {text_colour};")
        self._lbl_action.setText(action)
        self._lbl_ts.setText(ts[:19] if ts else "")

        self.setStyleSheet(
            f"""
            QFrame#AlertBanner {{
                background-color: {bg};
                border: 2px solid {border};
                border-radius: {DS.CORNER_RADIUS}px;
            }}
            """
        )

        # Manage flash timer.
        if self._level == "EMERGENCY":
            if not self._flash_timer.isActive():
                self._flash_timer.start(500)
        else:
            self._flash_timer.stop()
            self._flash_state = False

    def _toggle_flash(self) -> None:
        self._flash_state = not self._flash_state
        colours = DS.LEVEL_COLOURS["EMERGENCY"]
        border  = "#ff1a1a" if self._flash_state else colours["border"]
        bg      = "#220000"  if self._flash_state else colours["accent"]
        self.setStyleSheet(
            f"""
            QFrame#AlertBanner {{
                background-color: {bg};
                border: 2px solid {border};
                border-radius: {DS.CORNER_RADIUS}px;
            }}
            """
        )


# ---------------------------------------------------------------------------
# LiveChart — real-time graph (pyqtgraph or QPainter fallback)
# ---------------------------------------------------------------------------

class LiveChart(QWidget):
    """
    Plots the last `HISTORY` readings for NH₃, Temperature, and Humidity.

    Uses pyqtgraph when available (GPU-accelerated); falls back to a
    pure QPainter implementation so the UI works without pyqtgraph.
    """

    HISTORY = 60

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setMinimumHeight(DS.GRAPH_HEIGHT)

        self._nh3   : deque[float] = deque([0.0] * self.HISTORY, maxlen=self.HISTORY)
        self._temp  : deque[float] = deque([0.0] * self.HISTORY, maxlen=self.HISTORY)
        self._humid : deque[float] = deque([0.0] * self.HISTORY, maxlen=self.HISTORY)
        self._x     : list[int]    = list(range(self.HISTORY))

        if _PYQTGRAPH_AVAILABLE:
            self._build_pyqtgraph()
        else:
            self._build_fallback()

    def reset(self) -> None:
        self._nh3.clear()
        self._temp.clear()
        self._humid.clear()
        self._nh3.extend([0.0] * self.HISTORY)
        self._temp.extend([0.0] * self.HISTORY)
        self._humid.extend([0.0] * self.HISTORY)
        self._refresh()

    # ── pyqtgraph backend ────────────────────────────────────────────────────

    def _build_pyqtgraph(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self._pw = pg.PlotWidget()
        self._pw.setBackground(DS.BG_PANEL_ALT)
        self._pw.showGrid(x=True, y=True, alpha=0.12)
        self._pw.getAxis("left").setTextPen(pg.mkPen(DS.TEXT_SECONDARY))
        self._pw.getAxis("bottom").setTextPen(pg.mkPen(DS.TEXT_SECONDARY))
        self._pw.getAxis("left").setPen(pg.mkPen(DS.BORDER))
        self._pw.getAxis("bottom").setPen(pg.mkPen(DS.BORDER))

        # Legend inside chart.
        legend = self._pw.addLegend(
            offset=(10, 10),
            labelTextColor=DS.TEXT_SECONDARY,
        )
        legend.setBrush(pg.mkBrush(DS.BG_PANEL))
        legend.setPen(pg.mkPen(DS.BORDER))

        pen_nh3   = pg.mkPen(color=DS.COLOUR_NH3,   width=2)
        pen_temp  = pg.mkPen(color=DS.COLOUR_TEMP,  width=2)
        pen_humid = pg.mkPen(color=DS.COLOUR_HUMID, width=2)

        y0 = list(self._nh3)
        self._curve_nh3   = self._pw.plot(self._x, y0,   name="NH₃ (ppm)",   pen=pen_nh3)
        self._curve_temp  = self._pw.plot(self._x, y0,   name="Temp (°C)",   pen=pen_temp)
        self._curve_humid = self._pw.plot(self._x, y0,   name="Humid (%)",   pen=pen_humid)

        # Threshold lines for NH₃.
        for threshold, colour in [
            (DS.NH3_NORMAL,   "#665500"),
            (DS.NH3_WARNING,  "#663300"),
            (DS.NH3_CRITICAL, "#660000"),
        ]:
            inf_line = pg.InfiniteLine(
                pos      = threshold,
                angle    = 0,
                pen      = pg.mkPen(color=colour, width=1, style=Qt.PenStyle.DashLine),
                movable  = False,
            )
            self._pw.addItem(inf_line)

        layout.addWidget(self._pw)

    # ── QPainter fallback backend ────────────────────────────────────────────

    def _build_fallback(self) -> None:
        """Minimal chart using QPainter — no extra dependencies."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self._canvas = _FallbackCanvas(self)
        self._canvas.set_deques(self._nh3, self._temp, self._humid)
        layout.addWidget(self._canvas)

    # ── Public API ───────────────────────────────────────────────────────────

    def push(self, nh3: float, temp: float, humid: float) -> None:
        self._nh3.append(nh3)
        self._temp.append(temp)
        self._humid.append(humid)
        self._refresh()

    def load_history(self, snaps: list[TelemetrySnapshot]) -> None:
        for s in snaps[-self.HISTORY:]:
            self._nh3.append(s.nh3_ppm)
            self._temp.append(s.temp_c)
            self._humid.append(s.humid_pct)
        self._refresh()

    def _refresh(self) -> None:
        if _PYQTGRAPH_AVAILABLE:
            self._curve_nh3.setData(self._x, list(self._nh3))
            self._curve_temp.setData(self._x, list(self._temp))
            self._curve_humid.setData(self._x, list(self._humid))
        else:
            self._canvas.update()


class _FallbackCanvas(QWidget):
    """Simple QPainter chart (fallback when pyqtgraph is absent)."""

    _MARGIN = 52

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._nh3 = self._temp = self._humid = None

    def set_deques(self, nh3, temp, humid) -> None:
        self._nh3, self._temp, self._humid = nh3, temp, humid

    def paintEvent(self, _ev) -> None:
        if self._nh3 is None:
            return
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        w, h  = self.width(), self.height()
        m     = self._MARGIN
        pw, ph = w - 2 * m, h - 2 * m

        # Background
        p.fillRect(0, 0, w, h, QColor(DS.BG_PANEL_ALT))

        # Grid lines
        p.setPen(QPen(QColor(DS.BORDER), 1, Qt.PenStyle.DotLine))
        for i in range(5):
            y = m + int(ph * i / 4)
            p.drawLine(m, y, w - m, y)

        def _draw_series(data: deque, colour: str, scale_max: float) -> None:
            pts   = list(data)
            n     = len(pts)
            if n < 2:
                return
            pen   = QPen(QColor(colour), 2)
            p.setPen(pen)
            prev_x = prev_y = None
            for i, v in enumerate(pts):
                x  = m + int(pw * i / (n - 1))
                yf = 1.0 - min(max(v, 0), scale_max) / scale_max
                y  = m + int(ph * yf)
                if prev_x is not None:
                    p.drawLine(prev_x, prev_y, x, y)
                prev_x, prev_y = x, y

        _draw_series(self._nh3,   DS.COLOUR_NH3,   DS.NH3_MAX_DISPLAY)
        _draw_series(self._temp,  DS.COLOUR_TEMP,  60.0)
        _draw_series(self._humid, DS.COLOUR_HUMID, 100.0)

        # Legend
        legend_items = [
            ("NH₃ ppm", DS.COLOUR_NH3),
            ("Temp °C",  DS.COLOUR_TEMP),
            ("Humid %",  DS.COLOUR_HUMID),
        ]
        p.setFont(QFont(DS.FONT_FAMILY, 9))
        lx = m + 10
        for label, colour in legend_items:
            p.setPen(QPen(QColor(colour), 2))
            p.drawLine(lx, 20, lx + 20, 20)
            p.setPen(QColor(DS.TEXT_SECONDARY))
            p.drawText(lx + 26, 25, label)
            lx += 90
        p.end()


# ---------------------------------------------------------------------------
# AlertLogPanel — last 10 alert entries in a scrollable table
# ---------------------------------------------------------------------------

class AlertLogPanel(QFrame):
    """Read-only list of the last 10 alert_log entries."""

    _MAX_ROWS = 10

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setObjectName("AlertLogPanel")
        self.setStyleSheet(
            f"""
            QFrame#AlertLogPanel {{
                background-color: {DS.BG_PANEL};
                border: {DS.BORDER_WIDTH}px solid {DS.BORDER};
                border-radius: {DS.CORNER_RADIUS}px;
            }}
            """
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(6)

        title = QLabel("⚠  Recent Alerts")
        title.setFont(QFont(DS.FONT_FAMILY, 11, QFont.Weight.Medium))
        title.setStyleSheet(f"color: {DS.TEXT_SECONDARY};")
        layout.addWidget(title)

        # Scroll area for entries.
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._scroll.setStyleSheet("background: transparent;")
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        self._inner = QWidget()
        self._inner.setStyleSheet("background: transparent;")
        self._inner_layout = QVBoxLayout(self._inner)
        self._inner_layout.setContentsMargins(0, 0, 0, 0)
        self._inner_layout.setSpacing(4)
        self._inner_layout.addStretch()

        self._scroll.setWidget(self._inner)
        layout.addWidget(self._scroll)

        self._rows: list[QLabel] = []

    def load_alerts(self, rows: list[dict]) -> None:
        """Populate from a list of raw DB row dicts (most-recent first)."""
        # Clear existing rows.
        for r in self._rows:
            r.deleteLater()
        self._rows.clear()

        if not rows:
            placeholder = QLabel("No alerts recorded yet.")
            placeholder.setStyleSheet(f"color: {DS.TEXT_MUTED};")
            self._inner_layout.insertWidget(0, placeholder)
            self._rows.append(placeholder)
            return

        level_colours = {
            "NORMAL":    DS.TEXT_SECONDARY,
            "WARNING":   "#f5c400",
            "CRITICAL":  "#ff6a00",
            "EMERGENCY": "#ff1a1a",
        }

        for row in rows[: self._MAX_ROWS]:
            level     = str(row.get("level", "NORMAL"))
            ts        = str(row.get("timestamp", ""))[:19]
            nh3       = float(row.get("nh3_ppm", 0))
            suppressed= int(row.get("suppressed", 0))
            colour    = level_colours.get(level, DS.TEXT_SECONDARY)
            sup_tag   = " [suppressed]" if suppressed else ""

            text = (
                f"<span style='color:{colour};font-weight:bold;'>{level}</span>"
                f"<span style='color:{DS.TEXT_MUTED};'>{sup_tag}</span>"
                f"  <span style='color:{DS.TEXT_SECONDARY};'>{ts}</span>"
                f"  <span style='color:{DS.TEXT_MUTED};'>NH₃={nh3:.1f}ppm</span>"
            )
            lbl = QLabel()
            lbl.setTextFormat(Qt.TextFormat.RichText)
            lbl.setText(text)
            lbl.setFont(QFont(DS.FONT_MONO, 9))
            lbl.setStyleSheet("background: transparent; padding: 2px 0;")
            self._inner_layout.insertWidget(0, lbl)
            self._rows.append(lbl)


# ---------------------------------------------------------------------------
# MainWindow — the primary application window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    """
    OvoMatrix v8.0 Dashboard.

    Layout (top → bottom):
      HeaderBar     — logo, clock, connection indicator
      AlertBanner   — dynamic alert level strip
      MetricCards   — 3 sensor cards (NH₃ / Temp / Humidity)
      LiveChart     — 60-reading time-series graph
      AlertLogPanel — last 10 alert events
      StatusBar     — last update time, row counts
    """

    def __init__(self, db_path: str) -> None:
        super().__init__()
        self._db_path       = db_path
        self._last_level    = "NORMAL"
        self._update_count  = 0

        self.setWindowTitle("ovomatrix  —  Poultry Telemetry Dashboard")
        
        import base64
        pm = QPixmap()
        pm.loadFromData(base64.b64decode(APP_LOGO_DATA))
        self.setWindowIcon(QIcon(pm))
        
        self.setMinimumSize(1100, 780)
        self._apply_global_style()
        self._build_ui()

        # Zero values on Cold Start
        self._card_nh3.update_value(0.00)
        self._card_temp.update_value(0.00)
        self._card_humid.update_value(0.00)
        self._chart.reset()

        self._start_poller()
        self._start_clock()
        self._load_last_ai_advice()
        self.showMaximized()

    # ── Global stylesheet ────────────────────────────────────────────────────

    def _apply_global_style(self) -> None:
        self.setStyleSheet(
            f"""
            QMainWindow, QWidget {{
                background-color: {DS.BG_VOID};
                color: {DS.TEXT_PRIMARY};
                font-family: "{DS.FONT_FAMILY}", Arial, sans-serif;
            }}
            QScrollBar:vertical {{
                background: {DS.BG_PANEL};
                width: 6px;
                border-radius: 3px;
            }}
            QScrollBar::handle:vertical {{
                background: {DS.BORDER_BRIGHT};
                border-radius: 3px;
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                height: 0;
            }}
            QStatusBar {{
                background-color: {DS.BG_PANEL};
                color: {DS.TEXT_MUTED};
                font-size: 10px;
                border-top: 1px solid {DS.BORDER};
            }}
            """
        )

    # ── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        root = QWidget()
        self.main_h_layout = QHBoxLayout(root)
        self.main_h_layout.setContentsMargins(0, 0, 0, 0)
        self.main_h_layout.setSpacing(0)
        self.setCentralWidget(root)

        # ── 1. Central Dashboard Area ──────────────────────────────────────
        self._dashboard_root = QWidget()
        layout = QVBoxLayout(self._dashboard_root)
        layout.setContentsMargins(16, 14, 16, 10)
        layout.setSpacing(12)

        layout.addWidget(self._build_header())

        self._alert_banner = AlertBanner()
        layout.addWidget(self._alert_banner)

        layout.addWidget(self._build_cards())

        middle_row = QHBoxLayout()
        middle_row.setSpacing(12)

        self._chart = LiveChart()
        self._chart.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )
        self._chart.setMinimumHeight(DS.GRAPH_HEIGHT)
        middle_row.addWidget(self._chart, stretch=5)

        self._alert_log = AlertLogPanel()
        self._alert_log.setFixedWidth(340)
        middle_row.addWidget(self._alert_log, stretch=2)

        layout.addLayout(middle_row)
        layout.addStretch(1)

        self.main_h_layout.addWidget(self._dashboard_root, stretch=1)

        # ── 2. AI Sidebar Panel ───────────────────────────────────────────
        self._build_ai_panel()
        self.main_h_layout.addWidget(self._ai_panel)

        # ── 3. Status bar ──────────────────────────────────────────────────
        self._status_bar_main = self._build_status_bar()

    def _build_ai_panel(self) -> None:
        self._ai_panel = QFrame()
        self._ai_panel.setObjectName("AIPanel")
        self._ai_panel.setStyleSheet(
            f"QFrame#AIPanel {{"
            f"  background-color: {DS.BG_PANEL_ALT};"
            f"  border-left: 1px solid {DS.BORDER};"
            f"}}"
        )
        self._ai_panel.setFixedWidth(0) # Initially hidden
        
        panel_layout = QVBoxLayout(self._ai_panel)
        panel_layout.setContentsMargins(16, 16, 16, 16)
        
        # Top row: Title + Close Button
        top_row = QHBoxLayout()
        lbl_title = QLabel("✨ AI Advisor")
        lbl_title.setFont(QFont(DS.FONT_FAMILY, 14, QFont.Weight.Bold))
        top_row.addWidget(lbl_title)
        
        btn_close = QPushButton("✕")
        btn_close.setFixedSize(28, 28)
        btn_close.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_close.setStyleSheet(
            "QPushButton { background: transparent; border: none; color: #8899bb; font-size: 16px; font-weight: bold; }"
            "QPushButton:hover { color: #ffffff; }"
        )
        btn_close.clicked.connect(self._toggle_ai_panel)
        top_row.addWidget(btn_close)
        
        panel_layout.addLayout(top_row)
        
        # Expert Advice Box
        self._expert_advice = QTextEdit()
        self._expert_advice.setReadOnly(True)
        self._expert_advice.setText("Waiting for configuration...")
        self._expert_advice.setPlaceholderText("Expert AI Advice will appear here... (Open Settings to configure)")
        self._expert_advice.setStyleSheet(
            f"background-color: {DS.BG_PANEL}; border: 1px solid {DS.BORDER}; border-radius: 4px; padding: 10px; color: {DS.ACCENT_BLUE}; font-size: 13px;"
        )
        panel_layout.addWidget(self._expert_advice)
        
        # Action Button
        self._btn_ask_ai = QPushButton("Consult Expert")
        self._btn_ask_ai.setMinimumHeight(40)
        self._btn_ask_ai.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn_ask_ai.setStyleSheet(
            f"background-color: {DS.ACCENT_BLUE}; color: white; border-radius: 4px; font-weight: bold;"
        )
        self._btn_ask_ai.clicked.connect(self._on_sidebar_ai_clicked)
        panel_layout.addWidget(self._btn_ask_ai)
        
        # Animation
        self._ai_anim = QPropertyAnimation(self._ai_panel, b"maximumWidth")
        self._ai_anim.setDuration(250)
        self._ai_anim.setEasingCurve(QEasingCurve.Type.InOutSine)
        self._ai_panel_open = False

    def _toggle_ai_panel(self) -> None:
        if self._ai_panel_open:
            self._ai_anim.setStartValue(340)
            self._ai_anim.setEndValue(0)
            self._ai_panel_open = False
        else:
            self._ai_anim.setStartValue(0)
            self._ai_anim.setEndValue(340)
            self._ai_panel_open = True
        self._ai_anim.start()

    def _on_sidebar_ai_clicked(self):
        import os, threading
        from PyQt6.QtWidgets import QMessageBox
        
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            QMessageBox.warning(self, "Configuration Required", "Configuration Required: Please set your AI API key in the Settings menu first.")
            return
            
        self._btn_ask_ai.setEnabled(False)
        self._btn_ask_ai.setText("Thinking... ✨")
        self.update_expert_advice("Fetching response from Expert AI...")
        threading.Thread(target=self._fetch_and_call_ai, daemon=True).start()

    def _fetch_and_call_ai(self):
        import os, time, requests
        nh3 = temp = humid = 0.0
        try:
            from db_manager import DatabaseManager
            db = DatabaseManager(self._db_path)
            last = db.get_last_reading()
            if last:
                nh3 = last.get("nh3_ppm", 0)
                temp = last.get("temp_c", 0)
                humid = last.get("humid_pct", 0)
        except Exception as e:
            logger.warning("Could not fetch reading for floating AI: %s", e)
            
        prompt = f"Poultry farm current status: NH3={nh3}ppm, Temperature={temp}C, Humidity={humid}%. Provide a concise, clear 2-3 sentence technical advice focusing on the birds' welfare based on the current metrics."
        
        provider = "Gemini"
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            time.sleep(1)
            self._update_parent_ai_text("Not Connected: Please set the GEMINI_API_KEY environment variable.")
            return

        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
            headers = {'Content-Type': 'application/json'}
            data = {"contents": [{"parts": [{"text": prompt}]}]}
            
            resp = requests.post(url, headers=headers, json=data, timeout=10)
            if resp.status_code == 400 and 'API_KEY_INVALID' in resp.text:
                self._update_parent_ai_text("Not Connected: API Key was invalid.")
            else:
                resp.raise_for_status()
                result = resp.json()
                answer = result["candidates"][0]["content"]["parts"][0]["text"]
                self._update_parent_ai_text(answer)
        except Exception as e:
            self._update_parent_ai_text(f"API Error: {str(e)}")

    def _update_parent_ai_text(self, text: str):
        from PyQt6.QtCore import QMetaObject, Q_ARG
        QMetaObject.invokeMethod(self, "update_expert_advice", Qt.ConnectionType.QueuedConnection, Q_ARG(str, text))
        QMetaObject.invokeMethod(self._btn_ask_ai, "setEnabled", Qt.ConnectionType.QueuedConnection, Q_ARG(bool, True))
        QMetaObject.invokeMethod(self._btn_ask_ai, "setText", Qt.ConnectionType.QueuedConnection, Q_ARG(str, "Consult Expert"))

    def _build_header(self) -> QWidget:
        container = QWidget()
        row = QHBoxLayout(container)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(12)

        # Logo mark
        logo_badge = QLabel("OVO")
        logo_badge.setFont(QFont(DS.FONT_MONO, 18, QFont.Weight.Black))
        logo_badge.setFixedSize(60, 44)
        logo_badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        logo_badge.setStyleSheet(
            f"""
            background-color: {DS.ACCENT_BLUE};
            color: white;
            border-radius: 8px;
            """
        )
        row.addWidget(logo_badge)

        # Title block
        title_block = QVBoxLayout()
        title_block.setSpacing(0)
        lbl_title = QLabel("ovomatrix")
        lbl_title.setFont(QFont(DS.FONT_FAMILY, 16, QFont.Weight.Bold))
        lbl_title.setStyleSheet(f"color: {DS.TEXT_PRIMARY};")
        lbl_subtitle = QLabel("Safety-Critical Poultry Telemetry  ·  Read-Only Dashboard")
        lbl_subtitle.setFont(QFont(DS.FONT_FAMILY, 9))
        lbl_subtitle.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        title_block.addWidget(lbl_title)
        title_block.addWidget(lbl_subtitle)
        row.addLayout(title_block)

        row.addStretch()
        
        # Toggle AI Panel Hook
        self.btn_toggle_ai = QPushButton("✨ AI Advisor")
        self.btn_toggle_ai.setFont(QFont(DS.FONT_FAMILY, 10, QFont.Weight.Bold))
        self.btn_toggle_ai.setStyleSheet(
            f"background-color: transparent; color: {DS.ACCENT_BLUE}; border: 1px solid {DS.ACCENT_BLUE}; border-radius: 6px; padding: 6px 14px;"
        )
        self.btn_toggle_ai.clicked.connect(self._toggle_ai_panel)
        self.btn_toggle_ai.setCursor(Qt.CursorShape.PointingHandCursor)
        row.addWidget(self.btn_toggle_ai)
        
        # Settings Hook
        self.settings_btn = QPushButton("⚙ Settings")
        self.settings_btn.setFont(QFont(DS.FONT_FAMILY, 10, QFont.Weight.Bold))
        self.settings_btn.setStyleSheet(
            f"background-color: {DS.BG_PANEL_ALT}; color: {DS.TEXT_PRIMARY}; border: 1px solid {DS.BORDER}; border-radius: 6px; padding: 6px 14px;"
        )
        self.settings_btn.clicked.connect(self._open_settings)
        self.settings_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        row.addWidget(self.settings_btn)
        
        row.addSpacerItem(QSpacerItem(16, 0))

        # DB path indicator
        db_lbl = QLabel(f"DB: {Path(self._db_path).name}")
        db_lbl.setFont(QFont(DS.FONT_MONO, 9))
        db_lbl.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        row.addWidget(db_lbl)

        # Connection dot
        self._conn_dot = QLabel("● OFFLINE")
        self._conn_dot.setFont(QFont(DS.FONT_FAMILY, 10, QFont.Weight.Medium))
        self._conn_dot.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        row.addWidget(self._conn_dot)

        # Live clock
        self._lbl_clock = QLabel()
        self._lbl_clock.setFont(QFont(DS.FONT_MONO, 13))
        self._lbl_clock.setStyleSheet(f"color: {DS.TEXT_SECONDARY};")
        self._lbl_clock.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self._lbl_clock.setFixedWidth(170)
        row.addWidget(self._lbl_clock)

        return container

    def _build_cards(self) -> QWidget:
        container = QWidget()
        row = QHBoxLayout(container)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(12)

        self._card_nh3 = MetricCard(
            title   = "NH₃  AMMONIA",
            unit    = "ppm",
            accent  = DS.COLOUR_NH3,
            val_min = 0,
            val_max = DS.NH3_MAX_DISPLAY,
        )
        self._card_temp = MetricCard(
            title   = "TEMPERATURE",
            unit    = "°C",
            accent  = DS.COLOUR_TEMP,
            val_min = 0,
            val_max = 50,
        )
        self._card_humid = MetricCard(
            title   = "HUMIDITY",
            unit    = "%RH",
            accent  = DS.COLOUR_HUMID,
            val_min = 0,
            val_max = 100,
        )

        row.addWidget(self._card_nh3, stretch=1)
        row.addWidget(self._card_temp, stretch=1)
        row.addWidget(self._card_humid, stretch=1)

        return container

    def _build_status_bar(self) -> QStatusBar:
        sb = QStatusBar()
        self.setStatusBar(sb)

        self._sb_last_update = QLabel("Last update: —")
        self._sb_update_count = QLabel("Updates: 0")
        self._sb_source       = QLabel("Source: —")
        self._sb_power        = QLabel("Power: —")
        self._sb_stream       = QLabel("Stream: IDLE")

        for lbl in (self._sb_last_update, self._sb_update_count, self._sb_source, self._sb_power, self._sb_stream):
            lbl.setFont(QFont(DS.FONT_MONO, 9))
            sb.addWidget(lbl)
            sb.addWidget(_StatusSep())

        sb.addPermanentWidget(QLabel("ovomatrix  |  Read-Only Viewport"))
        sb.addPermanentWidget(_StatusSep())
        sig_lbl = QLabel("2026. mohammed guejjim")
        sig_lbl.setFont(QFont(DS.FONT_FAMILY, 10))
        sig_lbl.setStyleSheet("color: #667788;")
        sb.addPermanentWidget(sig_lbl)
        
        return sb

    # ── Poller + signals ────────────────────────────────────────────────────

    def _start_poller(self) -> None:
        self._poller = DataPoller(self._db_path)
        self._poller.reading_ready.connect(self._on_reading)
        self._poller.alert_ready.connect(self._on_alert)
        self._poller.ups_ready.connect(self._on_ups_status)
        self._poller.connection_changed.connect(self._on_connection)
        self._poller.history_ready.connect(self._on_history)
        self._poller.start()

    def _start_clock(self) -> None:
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._tick_clock)
        self._clock_timer.start(1000)
        self._tick_clock()

    # ── Slot handlers (GUI thread only) ─────────────────────────────────────

    def _on_reading(self, snap: TelemetrySnapshot) -> None:
        """Update cards and chart with new telemetry."""
        self._update_count += 1

        # Determine NH₃ alert accent for card.
        if snap.nh3_ppm >= DS.NH3_CRITICAL:
            nh3_colour = DS.LEVEL_COLOURS["EMERGENCY"]["text"]
        elif snap.nh3_ppm >= DS.NH3_WARNING:
            nh3_colour = DS.LEVEL_COLOURS["CRITICAL"]["text"]
        elif snap.nh3_ppm >= DS.NH3_NORMAL:
            nh3_colour = DS.LEVEL_COLOURS["WARNING"]["text"]
        else:
            nh3_colour = DS.COLOUR_NH3
            if self._last_level != "NORMAL":
                self._last_level = "NORMAL"
                self._alert_banner.set_level("NORMAL", "No active alerts: values within safe boundaries.", "")
                border = DS.LEVEL_COLOURS["NORMAL"]["border"]
                for card in (self._card_nh3, self._card_temp, self._card_humid):
                    card.highlight_border(border)

        self._card_nh3.update_value(snap.nh3_ppm)
        self._card_nh3.set_accent(nh3_colour)
        self._card_temp.update_value(snap.temp_c)
        self._card_humid.update_value(snap.humid_pct)
        self._chart.push(snap.nh3_ppm, snap.temp_c, snap.humid_pct)

        # Status bar.
        now_local = datetime.now().strftime("%H:%M:%S")
        self._sb_last_update.setText(f"Last update: {now_local}")
        self._sb_update_count.setText(f"Updates: {self._update_count:,}")
        self._sb_source.setText(f"Source: {snap.source}")
        self._sb_stream.setText("Stream: ACTIVE")
        self._sb_stream.setStyleSheet(f"color: {DS.ACCENT_BLUE}; font-weight: bold;")

    def _on_alert(self, alert: AlertSnapshot) -> None:
        """Update alert banner and card borders based on alert level."""
        level = alert.level
        if level == self._last_level and alert.timestamp:
            # Only re-draw UI on actual level change to reduce flicker.
            pass

        self._last_level = level
        colours = DS.LEVEL_COLOURS.get(level, DS.LEVEL_COLOURS["NORMAL"])

        self._alert_banner.set_level(level, alert.action_taken, alert.timestamp)

        # Highlight card borders.
        border = colours["border"]
        for card in (self._card_nh3, self._card_temp, self._card_humid):
            card.highlight_border(border)

        # Refresh alert log.
        if self._poller._conn:
            rows = self._poller.fetch_recent_alerts(10)
            self._alert_log.load_alerts(rows)

    def _on_ups_status(self, data: dict) -> None:
        """Update the power status indicator in the status bar."""
        status = data.get("status", "UNKNOWN")
        pct    = data.get("battery_pct", 0.0)
        
        if status == "ONLINE":
            text = f"⚡ ONLINE ({pct:.0f}%)"
            colour = DS.ACCENT_BLUE
        elif status == "ONBATT":
            text = f"🔋 ON BATTERY ({pct:.0f}%)"
            colour = DS.LEVEL_COLOURS["WARNING"]["text"]
        elif status == "LOW_BATTERY":
            text = f"🪫 LOW BATTERY ({pct:.0f}%)"
            colour = DS.LEVEL_COLOURS["EMERGENCY"]["text"]
        else:
            text = "⚡ POWER UNKNOWN"
            colour = DS.TEXT_MUTED
            
        self._sb_power.setText(text)
        self._sb_power.setStyleSheet(f"color: {colour}; font-weight: bold;")

    def _on_connection(self, ok: bool, reason: str) -> None:
        if ok:
            self._conn_dot.setText("● LIVE")
            self._conn_dot.setStyleSheet(f"color: {DS.ACCENT_BLUE};")
        else:
            self._conn_dot.setText("● OFFLINE")
            self._conn_dot.setStyleSheet(f"color: {DS.LEVEL_COLOURS['CRITICAL']['text']};")
            self.statusBar().showMessage(f"DB connection lost: {reason}", 5000)

    def _on_history(self, snaps: list[TelemetrySnapshot]) -> None:
        """Pre-populate the chart with historical readings on startup."""
        self._chart.load_history(snaps)
        logger.info("Chart pre-populated with %d historical readings.", len(snaps))

    def _tick_clock(self) -> None:
        now = datetime.now()
        self._lbl_clock.setText(now.strftime("%Y-%m-%d  %H:%M:%S"))

    def reset_dashboard(self) -> None:
        """Reset all UI components to zero / initial state (cold start)."""
        logger.info("Resetting dashboard UI state to zero.")
        
        # Reset the background poller's memory of last seen records
        if self._poller:
            self._poller.reset_state()
            
        self._update_count = 0
        self._card_nh3.update_value(0.0)
        self._card_nh3.set_accent(DS.COLOUR_NH3)
        self._card_temp.update_value(0.0)
        self._card_humid.update_value(0.0)
        self._chart.reset()
        self._alert_banner.set_level("NORMAL", "System initialising …", "")
        self._alert_log.load_alerts([])
        
        # Reset counters/source in status bar.
        self._sb_update_count.setText("Updates: 0")
        self._sb_last_update.setText("Last update: —")
        self._sb_source.setText("Source: —")
        self._sb_stream.setText("Stream: IDLE")
        self._sb_stream.setStyleSheet(f"color: {DS.TEXT_MUTED};")
        
        # Reset card borders.
        default_border = DS.LEVEL_COLOURS["NORMAL"]["border"]
        for card in (self._card_nh3, self._card_temp, self._card_humid):
            card.highlight_border(default_border)

    # ── Clean shutdown ───────────────────────────────────────────────────────

    @pyqtSlot(str)
    def update_expert_advice(self, text: str) -> None:
        """Called by SettingsDialog to update the Expert Advice box."""
        self._expert_advice.setText(text)

    def _load_last_ai_advice(self) -> None:
        """Fetch the most recent AI advice from DB on startup (persistence)."""
        try:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.execute("SELECT advice FROM ai_log ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            conn.close()
            if row:
                self._expert_advice.setText(row["advice"])
            else:
                self._expert_advice.setText("No previous technical advice found. Use the Consult AI button in Settings to generate a new analysis.")
        except Exception as e:
            logger.warning("Failed to load last AI advice: %s", e)
            self._expert_advice.setText("System ready. AI Technical recommendations will appear here.")

    def _open_settings(self) -> None:
        try:
            if hasattr(self, '_settings_dlg') and self._settings_dlg.isVisible():
                self._settings_dlg.raise_()
                self._settings_dlg.activateWindow()
                return
                
            from settings_dialog import SettingsDialog
            from db_manager import DatabaseManager
            db_mgr = DatabaseManager(self._db_path)
            self._settings_dlg = SettingsDialog(db_manager=db_mgr, parent=self)
            self._settings_dlg.show()
        except Exception as exc:
            logger.error("Failed to open SettingsDialog: %s", exc)

    def closeEvent(self, event) -> None:
        """Stop the poller thread; leave backend process untouched."""
        logger.info("Dashboard closing — stopping DataPoller …")
        self._poller.stop()
        self._poller.wait(3000)
        logger.info("Dashboard closed cleanly.")
        super().closeEvent(event)


class _StatusSep(QLabel):
    """Thin separator for the status bar."""
    def __init__(self):
        super().__init__("  |  ")
        self.setStyleSheet(f"color: {DS.TEXT_MUTED};")

# ---------------------------------------------------------------------------
# Application entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="ovomatrix — Telemetry Dashboard",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default="ovomatrix.db",
        help="Path to the SQLite database to visualise.",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="UI scale factor (e.g. 1.25 for 125%% DPI scaling).",
    )
    args = parser.parse_args()

    # Validate DB path (warn, don't abort — DB may not exist yet).
    db_path = Path(args.db).resolve()
    if not db_path.exists():
        logger.warning(
            "Database '%s' does not exist yet. "
            "Start main_backend.py first. The UI will connect when it appears.",
            db_path,
        )

    app = QApplication(sys.argv)
    app.setApplicationName("ovomatrix")
    app.setOrganizationName("ovomatrix")

    # High-DPI scaling.
    if args.scale != 1.0:
        os_env_name = "QT_SCALE_FACTOR"
        import os
        os.environ[os_env_name] = str(args.scale)

    # Apply a base dark palette to ensure all native widgets match.
    palette = QPalette()
    for role, colour in [
        (QPalette.ColorRole.Window,          QColor(DS.BG_VOID)),
        (QPalette.ColorRole.WindowText,      QColor(DS.TEXT_PRIMARY)),
        (QPalette.ColorRole.Base,            QColor(DS.BG_PANEL)),
        (QPalette.ColorRole.AlternateBase,   QColor(DS.BG_PANEL_ALT)),
        (QPalette.ColorRole.Text,            QColor(DS.TEXT_PRIMARY)),
        (QPalette.ColorRole.Button,          QColor(DS.BG_PANEL)),
        (QPalette.ColorRole.ButtonText,      QColor(DS.TEXT_PRIMARY)),
        (QPalette.ColorRole.Highlight,       QColor(DS.ACCENT_BLUE)),
        (QPalette.ColorRole.HighlightedText, QColor("#ffffff")),
    ]:
        palette.setColor(role, colour)
    app.setPalette(palette)

    # Log chart engine in use.
    if _PYQTGRAPH_AVAILABLE:
        logger.info("Chart engine: pyqtgraph (GPU-accelerated)")
    else:
        logger.warning(
            "pyqtgraph not found — using QPainter fallback chart.\n"
            "Install for best performance: pip install pyqtgraph"
        )

    window = MainWindow(str(db_path))
    window.show()

    return app.exec()


if __name__ == "__main__":
    import os
    sys.exit(main())

"""
OvoMatrix v8.0 — Phase 3, Step 8
ups_monitor.py: UPS Power-Awareness Monitor

Hardware target:
  Beelink Mini-PC + DC-UPS (LiFePO4 battery bank) managed by
  Network UPS Tools (NUT / upsd) running locally or over LAN.

Responsibilities:
  1. Poll the NUT daemon every UPS_POLL_INTERVAL seconds for:
       - ups.status          (ONLINE / OB / OB LB / …)
       - battery.charge      (%)
       - battery.runtime     (seconds)
       - ups.load            (% of rated capacity)
  2. Normalise raw NUT status into: ONLINE | ONBATT | LOW_BATTERY | UNKNOWN.
  3. Publish a compact JSON payload to MQTT topic  farm/power  on every poll.
  4. On ONBATT transition  → fire SMS via gsm_layer.send_alert().
  5. On LOW_BATTERY (< LOW_BATTERY_PCT %)  → fire final SMS, then execute
       a clean OS shutdown to protect the SQLite database from corruption.
  6. In MOCK_UPS mode  → simulate a realistic battery-discharge scenario
       so the whole pipeline can be tested without any physical hardware.

Thread model:
  UPSMonitorWorker is a threading.Thread (daemon capable).
  main_backend.py calls .start() and .stop() just like every other component.

Integration points:
  • Called from  main_backend.OvoMatrixBackend.start()  as Step 5b.
  • Publishes to MQTT; does NOT read from or write to SQLite DB directly.
  • Calls  gsm_layer.send_alert()  for SMS notifications.
  • Sets the shared  shutdown_event  before OS-level power-off so the
    backend teardown sequence runs first (flushes WAL → closes DB).

OvoMatrix additionally checks for local DC-UPS battery status to prevent
data corruption during unexpected power loss.

Dependencies:
  pip install paho-mqtt python-dotenv
  (NUT protocol is implemented natively via sockets to support Python 3.13+)
"""

from __future__ import annotations

import json
import logging
import math
import os
import platform
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

# .env loading (graceful fallback)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", override=False)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_UPS_LOGGER_NAME = "ovomatrix.ups_monitor"
logger = logging.getLogger(_UPS_LOGGER_NAME)

# ---------------------------------------------------------------------------
# Configuration — all overridable via .env or environment variables
# ---------------------------------------------------------------------------

def _env_bool(key: str, default: bool) -> bool:
    return os.environ.get(key, str(default)).strip().lower() in ("1", "true", "yes", "on")

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)).strip())
    except ValueError:
        return default

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, str(default)).strip())
    except ValueError:
        return default

def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default).strip()


#: True  → simulated battery; no NUT connection attempted.
MOCK_UPS: bool         = _env_bool("MOCK_UPS", True)

#: NUT daemon host.
NUT_HOST: str          = _env_str("NUT_HOST", "localhost")

#: NUT daemon port (default 3493).
NUT_PORT: int          = _env_int("NUT_PORT", 3493)

#: UPS name as registered in ups.conf (e.g. "beelink-ups").
NUT_UPS_NAME: str      = _env_str("NUT_UPS_NAME", "ups")

#: NUT username (optional, set to "" if access is open).
NUT_USERNAME: str      = _env_str("NUT_USERNAME", "")

#: NUT password (optional).
NUT_PASSWORD: str      = _env_str("NUT_PASSWORD", "")

#: Seconds between consecutive UPS polls.
UPS_POLL_INTERVAL: int = _env_int("UPS_POLL_INTERVAL", 10)

#: Battery % below which the LOW_BATTERY alert fires.
LOW_BATTERY_PCT: float = _env_float("LOW_BATTERY_PCT", 10.0)

#: Seconds from LOW_BATTERY detection to actual OS shutdown.
SHUTDOWN_GRACE: int    = _env_int("SHUTDOWN_GRACE_SECONDS", 30)

#: MQTT topic for power status publication.
POWER_MQTT_TOPIC: str  = _env_str("POWER_MQTT_TOPIC", "farm/power")

#: Mock scenario: seconds for a full 100 % → 0 % simulated discharge.
MOCK_DISCHARGE_SECONDS: float = _env_float("MOCK_DISCHARGE_SECONDS", 120.0)

#: Mock scenario: seconds until simulated mains failure (after start).
MOCK_OUTAGE_DELAY: float      = _env_float("MOCK_OUTAGE_DELAY", 20.0)

# ---------------------------------------------------------------------------
# Power-status enumeration
# ---------------------------------------------------------------------------

class PowerStatus(str, Enum):
    """
    Normalised UPS power state — independent of NUT's raw string format.

    NUT raw → OvoMatrix:
      OL             → ONLINE
      OB             → ONBATT
      OB LB          → LOW_BATTERY
      OB LB SD       → LOW_BATTERY  (shutdown already in progress at UPS)
      FSD, etc.      → LOW_BATTERY
      BYPASS, TRIM … → ONLINE  (still on utility power variant)
    """
    ONLINE      = "ONLINE"       # On utility power — normal operation
    ONBATT      = "ONBATT"       # Utility outage; running on battery
    LOW_BATTERY = "LOW_BATTERY"  # Battery critical; shutdown imminent
    UNKNOWN     = "UNKNOWN"      # Cannot read UPS data


# ---------------------------------------------------------------------------
# UPS reading dataclass
# ---------------------------------------------------------------------------

@dataclass
class UPSReading:
    """Snapshot of one UPS poll cycle."""
    timestamp:    str         # ISO-8601 UTC
    status:       PowerStatus
    battery_pct:  float       # 0–100 %
    runtime_left: int         # estimated seconds remaining (−1 = unknown)
    load_pct:     float       # UPS load as % of rated capacity
    raw_status:   str         # raw NUT "ups.status" string for logging

    def to_mqtt_payload(self) -> str:
        """Serialise to compact JSON for MQTT publication."""
        return json.dumps(
            {
                "timestamp":    self.timestamp,
                "status":       self.status.value,
                "battery_pct":  round(self.battery_pct, 1),
                "runtime_left": self.runtime_left,
                "load_pct":     round(self.load_pct, 1),
            },
            separators=(",", ":"),
        )

    @classmethod
    def unknown(cls) -> "UPSReading":
        return cls(
            timestamp    = datetime.now(timezone.utc).isoformat(),
            status       = PowerStatus.UNKNOWN,
            battery_pct  = 0.0,
            runtime_left = -1,
            load_pct     = 0.0,
            raw_status   = "UNKNOWN",
        )


# ---------------------------------------------------------------------------
# NUT status normaliser
# ---------------------------------------------------------------------------

def _normalise_nut_status(raw: str, battery_pct: float) -> PowerStatus:
    """
    Map a raw NUT ``ups.status`` string + battery charge to a PowerStatus.

    NUT status tokens (space-separated):
      OL  = On Line (utility power present)
      OB  = On Battery
      LB  = Low Battery (triggered by UPS firmware threshold)
      SD  = Shutdown (UPS is about to cut power)
      FSD = Forced Shutdown (host requested)
      BYPASS / TRIM / BOOST = utility variants → ONLINE

    OvoMatrix additionally enforces its own LOW_BATTERY_PCT threshold
    so safety is not entirely delegated to the UPS firmware.
    """
    tokens = raw.upper().split()

    if "LB" in tokens or "SD" in tokens or "FSD" in tokens:
        return PowerStatus.LOW_BATTERY

    if "OB" in tokens:
        if battery_pct <= LOW_BATTERY_PCT:
            return PowerStatus.LOW_BATTERY
        return PowerStatus.ONBATT

    if "OL" in tokens or "BYPASS" in tokens or "TRIM" in tokens or "BOOST" in tokens:
        return PowerStatus.ONLINE

    return PowerStatus.UNKNOWN


# ---------------------------------------------------------------------------
# UPS Backends
# ---------------------------------------------------------------------------

class _MockUPSBackend:
    """
    Simulates a realistic power-outage scenario for integration testing.

    Timeline:
      0 → MOCK_OUTAGE_DELAY s   :  ONLINE at 100 %
      MOCK_OUTAGE_DELAY → end   :  ONBATT, battery drains linearly
      At LOW_BATTERY_PCT        :  LOW_BATTERY
    """

    def __init__(self) -> None:
        self._start_time    = time.monotonic()
        self._outage_start: Optional[float] = None
        self._force_outage: bool = False
        self._logger        = logging.getLogger("ovomatrix.ups_monitor")

    def force_outage(self) -> None:
        """Manually trigger a simulated outage immediately."""
        self._force_outage = True
        self._logger.info("MockUPS: Manual outage trigger received.")

    def read(self) -> UPSReading:
        now     = time.monotonic()
        elapsed = now - self._start_time
        
        # Trigger outage if delay exceeded OR forced manually
        is_outage = elapsed >= MOCK_OUTAGE_DELAY or self._force_outage
        
        if not is_outage:
            # Stable mains power
            return UPSReading(
                timestamp    = datetime.now(timezone.utc).isoformat(),
                status       = PowerStatus.ONLINE,
                battery_pct  = 100.0,
                runtime_left = int(MOCK_DISCHARGE_SECONDS),
                load_pct     = 35.0,
                raw_status   = "OL",
            )

        # Simulated outage — battery draining
        if self._outage_start is None:
            self._outage_start = now
            self._logger.warning(
                "\033[93m[MOCK UPS] ⚡ SIMULATED POWER OUTAGE — "
                "switched to battery\033[0m"
            )

        on_batt_secs = now - self._outage_start
        pct          = max(0.0, 100.0 - (on_batt_secs / MOCK_DISCHARGE_SECONDS) * 100.0)
        runtime      = max(0, int(MOCK_DISCHARGE_SECONDS - on_batt_secs))

        if pct <= LOW_BATTERY_PCT:
            status     = PowerStatus.LOW_BATTERY
            raw_status = "OB LB"
        else:
            status     = PowerStatus.ONBATT
            raw_status = "OB"

        return UPSReading(
            timestamp    = datetime.now(timezone.utc).isoformat(),
            status       = status,
            battery_pct  = round(pct, 1),
            runtime_left = runtime,
            load_pct     = 35.0,
            raw_status   = raw_status,
        )


# ---------------------------------------------------------------------------
# Simple NUT Client (Python 3.13+ Friendly)
# ---------------------------------------------------------------------------

import socket

class SimpleNUTClient:
    """
    Performance-optimised, minimal NUT protocol client.
    Replaces 'nut2' to eliminate the 'telnetlib' dependency (removed in Python 3.13).
    """
    def __init__(self, host: str, port: int, login: str = "", password: str = ""):
        self.host = host
        self.port = port
        self.login = login
        self.password = password

    def list_vars(self, ups_name: str) -> dict[str, str]:
        """Fetch all variables for a given UPS name using the LIST VAR command."""
        try:
            with socket.create_connection((self.host, self.port), timeout=5.0) as s:
                # Optional: Authenticate if credentials provided
                if self.login and self.password:
                    s.sendall(f"USERNAME {self.login}\n".encode())
                    s.recv(1024)
                    s.sendall(f"PASSWORD {self.password}\n".encode())
                    s.recv(1024)

                # Query variables
                s.sendall(f"LIST VAR {ups_name}\n".encode())
                
                response = b""
                # Read until END LIST or timeout
                while b"END LIST VAR" not in response:
                    chunk = s.recv(4096)
                    if not chunk: break
                    response += chunk
                
                lines = response.decode(errors='replace').splitlines()
                vars_dict = {}
                for line in lines:
                    if line.startswith("VAR "):
                        # Format: VAR <ups> <varname> "<value>"
                        parts = line.split(" ", 3)
                        if len(parts) >= 4:
                            var_name = parts[2]
                            value = parts[3].strip('"')
                            vars_dict[var_name] = value
                return vars_dict
        except Exception as e:
            logger.debug(f"SimpleNUTClient error: {e}")
            return {}

class _NUTUPSBackend:
    """
    Real NUT (Network UPS Tools) backend via native socket implementation.
    """

    def __init__(
        self,
        host:     str,
        port:     int,
        ups_name: str,
        username: str,
        password: str,
    ) -> None:
        self._host     = host
        self._port     = port
        self._name     = ups_name
        self._username = username
        self._password = password
        self._client   = SimpleNUTClient(host, port, username, password)
        self._logger   = logging.getLogger(_UPS_LOGGER_NAME)
        self._logger.info(
            "NUT client initialized for %s:%d | UPS=%s",
            self._host, self._port, self._name,
        )

    def _safe_float(self, raw: dict, key: str, fallback: float = 0.0) -> float:
        try:
            return float(raw.get(key, fallback))
        except (ValueError, TypeError):
            return fallback

    def read(self) -> UPSReading:
        try:
            variables = self._client.list_vars(self._name)
            if not variables:
                return UPSReading.unknown()

            raw_status   = str(variables.get("ups.status", "UNKNOWN")).strip()
            _raw_bat     = self._safe_float(variables, "battery.charge", 100.0)
            battery_pct  = max(0.0, min(100.0, _raw_bat))
            runtime_left = int(self._safe_float(variables, "battery.runtime", -1))
            load_pct     = max(0.0, min(200.0, self._safe_float(variables, "ups.load", 0.0)))
            status       = _normalise_nut_status(raw_status, battery_pct)

            return UPSReading(
                timestamp    = datetime.now(timezone.utc).isoformat(),
                status       = status,
                battery_pct  = battery_pct,
                runtime_left = runtime_left,
                load_pct     = load_pct,
                raw_status   = raw_status,
            )

        except Exception as exc:            # noqa: BLE001
            self._logger.warning("NUT read error: %s", exc)
            return UPSReading.unknown()


# ---------------------------------------------------------------------------
# UPS Monitor Statistics
# ---------------------------------------------------------------------------

from dataclasses import dataclass as _dc, field as _f

@_dc
class UPSStats:
    """Thread-safe statistics counters for the UPS monitor."""
    _lock:               threading.Lock = _f(default_factory=threading.Lock, repr=False)
    total_polls:         int = 0
    onbatt_transitions:  int = 0
    low_battery_events:  int = 0
    sms_sent:            int = 0
    mqtt_publishes:      int = 0
    read_errors:         int = 0

    def bump(self, **kwargs: int) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "total_polls":        self.total_polls,
                "onbatt_transitions": self.onbatt_transitions,
                "low_battery_events": self.low_battery_events,
                "sms_sent":           self.sms_sent,
                "mqtt_publishes":     self.mqtt_publishes,
                "read_errors":        self.read_errors,
            }


# ---------------------------------------------------------------------------
# UPSMonitorWorker — the public component class
# ---------------------------------------------------------------------------

class UPSMonitorWorker(threading.Thread):
    """
    OvoMatrix v8.0 UPS power-awareness monitoring thread.

    Usage from main_backend.py:
    ::
        ups = UPSMonitorWorker(
            broker_host     = cfg.host,
            broker_port     = cfg.port,
            shutdown_event  = self._shutdown_event,
        )
        ups.start()
        …
        ups.stop()

    QThread-compatible via the same run() / stop() contract as every other
    OvoMatrix worker. To use with PyQt6:
    ::
        self._ups_thread = QThread()
        self._ups = UPSMonitorWorker(…)
        self._ups_thread.started.connect(self._ups.run)
        self._ups.moveToThread(self._ups_thread)
        self._ups_thread.start()
    """

    # Cooldown: seconds between repeated ONBATT SMS notifications.
    _ONBATT_SMS_COOLDOWN:  float = 30 * 60   # 30 minutes
    _LOWBATT_SMS_COOLDOWN: float = 2  * 60   # 2 minutes (repeats until shutdown)

    def __init__(
        self,
        db:             Optional[any] = None,
        broker_host:    str = "localhost",
        broker_port:    int = 1883,
        shutdown_event: Optional[threading.Event] = None,
        on_status_change: Optional[Callable[[UPSReading], None]] = None,
    ) -> None:
        super().__init__(name="UPSMonitor", daemon=True)
        self.db              = db
        self._broker_host    = broker_host
        self._broker_port    = broker_port
        self._shutdown_event = shutdown_event or threading.Event()
        self._on_status_change = on_status_change
        self._stop_event     = threading.Event()
        self.stats           = UPSStats()

        # State tracking (thread-internal; no lock needed)
        self._last_status:          PowerStatus       = PowerStatus.UNKNOWN
        self._last_onbatt_sms_ts:   Optional[float]   = None
        self._last_lowbatt_sms_ts:  Optional[float]   = None
        # SEC-07 fix: use threading.Event for atomic shutdown guard (not plain bool)
        self._shutdown_triggered:   threading.Event   = threading.Event()
        self._low_battery_confirm_count: int          = 0   # SEC-02: require 2 consecutive readings

        # MQTT client (initialised lazily in run())
        self._mqtt_client = None

        # Select backend.
        if MOCK_UPS:
            self._backend = _MockUPSBackend()
            logger.info(
                "UPSMonitor: MOCK mode active | outage in %.0fs | "
                "discharge time %.0fs",
                MOCK_OUTAGE_DELAY,
                MOCK_DISCHARGE_SECONDS,
            )
        else:
            self._backend = _NUTUPSBackend(
                host     = NUT_HOST,
                port     = NUT_PORT,
                ups_name = NUT_UPS_NAME,
                username = NUT_USERNAME,
                password = NUT_PASSWORD,
            )

    # ── Public control ──────────────────────────────────────────────────────

    def stop(self, timeout: float = 5.0) -> None:
        """Request graceful shutdown and join the thread."""
        logger.info("UPSMonitor stopping …")
        self._stop_event.set()
        self.join(timeout=timeout)
        if self.is_alive():
            logger.warning("UPSMonitor did not exit within %.1f s.", timeout)
        else:
            logger.info("UPSMonitor stopped cleanly.")

    def get_stats(self) -> dict:
        return self.stats.snapshot()

    # ── Thread run-loop ─────────────────────────────────────────────────────

    def run(self) -> None:
        logger.info(
            "UPSMonitor started | mode=%s | poll=%ds | low_bat_threshold=%.0f%%",
            "MOCK" if MOCK_UPS else f"NUT@{NUT_HOST}:{NUT_PORT}",
            UPS_POLL_INTERVAL,
            LOW_BATTERY_PCT,
        )
        self._connect_mqtt()

        while not self._stop_event.is_set():
            t0 = time.monotonic()

            try:
                reading = self._backend.read()
                self._process(reading)
            except Exception as exc:        # noqa: BLE001
                self.stats.bump(read_errors=1)
                logger.error("UPSMonitor: unexpected error in poll cycle: %s", exc, exc_info=True)

            elapsed   = time.monotonic() - t0
            sleep_for = max(0.0, UPS_POLL_INTERVAL - elapsed)
            self._stop_event.wait(timeout=sleep_for)

        self._disconnect_mqtt()
        logger.info(
            "UPSMonitor finished | polls=%d | onbatt=%d | lowbatt=%d",
            self.stats.total_polls,
            self.stats.onbatt_transitions,
            self.stats.low_battery_events,
        )

    # ── Core processing logic ────────────────────────────────────────────────

    def _process(self, reading: UPSReading) -> None:
        self.stats.bump(total_polls=1)

        logger.debug(
            "UPS poll | status=%-12s battery=%5.1f%% runtime=%ds load=%.1f%%",
            reading.status.value,
            reading.battery_pct,
            reading.runtime_left,
            reading.load_pct,
        )

        # ── Publish to MQTT ──────────────────────────────────────────────────
        self._mqtt_publish(reading)

        # ── Update Database ──────────────────────────────────────────────────
        if self.db:
            self.db.update_ups_status(
                status       = reading.status.value,
                battery_pct  = reading.battery_pct,
                runtime_left = reading.runtime_left,
                load_pct     = reading.load_pct
            )

        # ── Detect transitions ────────────────────────────────────────────────
        previous       = self._last_status
        current        = reading.status
        self._last_status = current

        # Reset low-battery confirmation counter when status is no longer LOW_BATTERY
        if current != PowerStatus.LOW_BATTERY:
            self._low_battery_confirm_count = 0

        # ── LOW_BATTERY: highest urgency ──────────────────────────────────────
        if current == PowerStatus.LOW_BATTERY:
            self._handle_low_battery(reading, is_new=(previous != PowerStatus.LOW_BATTERY))
            return

        # ── ONBATT (but not yet low) ──────────────────────────────────────────
        if current == PowerStatus.ONBATT:
            self._handle_on_battery(reading, is_new=(previous == PowerStatus.ONLINE or
                                                      previous == PowerStatus.UNKNOWN))
            return

        # ── Back on ONLINE after outage ───────────────────────────────────────
        if current == PowerStatus.ONLINE and previous == PowerStatus.ONBATT:
            self._handle_power_restored(reading)

        # ── GUI callback ──────────────────────────────────────────────────────
        if self._on_status_change and current != previous:
            self._safe_callback(reading)

    def _handle_low_battery(self, reading: UPSReading, is_new: bool) -> None:
        """React to critical battery level — notify then schedule OS shutdown."""
        self.stats.bump(low_battery_events=1)

        # SEC-02: Require 2 consecutive LOW_BATTERY readings before taking action.
        # Prevents a single malformed NUT response from triggering a shutdown.
        self._low_battery_confirm_count += 1
        if self._low_battery_confirm_count < 2:
            logger.warning(
                "LOW_BATTERY detected (confirmation %d/2) — "
                "waiting for next poll before scheduling shutdown.",
                self._low_battery_confirm_count,
            )
            return

        now        = time.monotonic()
        cooldown_ok = (
            self._last_lowbatt_sms_ts is None
            or (now - self._last_lowbatt_sms_ts) >= self._LOWBATT_SMS_COOLDOWN
        )

        msg = (
            f"[OvoMatrix] ⚠ LOW BATTERY: {reading.battery_pct:.1f}% remaining "
            f"(~{reading.runtime_left}s). "
            f"Auto-shutdown commences in {SHUTDOWN_GRACE}s."
        )
        logger.critical(
            "\033[41;97m UPS LOW BATTERY \033[0m "
            "battery=%.1f%% runtime=%ds",
            reading.battery_pct, reading.runtime_left,
        )

        if cooldown_ok:
            self._send_sms(
                level        = "CRITICAL",
                nh3_ppm      = 0.0,
                temp_c       = 0.0,
                action_taken = msg,
            )
            self._last_lowbatt_sms_ts = now

        # SEC-07: threading.Event is atomic — prevents double-shutdown race condition.
        if not self._shutdown_triggered.is_set():
            self._shutdown_triggered.set()
            if is_new or cooldown_ok:
                logger.critical(
                    "\033[41;97m SAFE SHUTDOWN scheduled in %d s \033[0m", SHUTDOWN_GRACE
                )
            shutdown_thread = threading.Thread(
                target = self._execute_shutdown,
                args   = (SHUTDOWN_GRACE,),
                daemon = True,
                name   = "SafeShutdown",
            )
            shutdown_thread.start()

    def _handle_on_battery(self, reading: UPSReading, is_new: bool) -> None:
        """React to mains failure / battery switchover."""
        now = time.monotonic()
        if is_new:
            self.stats.bump(onbatt_transitions=1)
            logger.warning(
                "\033[93m UPS SWITCHED TO BATTERY \033[0m "
                "battery=%.1f%% runtime=%ds",
                reading.battery_pct, reading.runtime_left,
            )

        cooldown_ok = (
            self._last_onbatt_sms_ts is None
            or (now - self._last_onbatt_sms_ts) >= self._ONBATT_SMS_COOLDOWN
        )
        if cooldown_ok:
            rt_min = reading.runtime_left // 60 if reading.runtime_left >= 0 else "?"
            msg = (
                f"[OvoMatrix] انقطاع التيار الكهربائي! النظام يعمل على البطارية. "
                f"شحن البطارية: {reading.battery_pct:.1f}%. "
                f"الوقت المتبقي: ~{rt_min} دقيقة."
            )
            self._send_sms(
                level        = "WARNING",
                nh3_ppm      = 0.0,
                temp_c       = 0.0,
                action_taken = msg,
            )
            self._last_onbatt_sms_ts = now
            if self._on_status_change:
                self._safe_callback(reading)

    def _handle_power_restored(self, reading: UPSReading) -> None:
        """Mains power has returned after an outage."""
        # SEC-07: Clear shutdown flag & confirmation counter if power restored before grace period.
        self._shutdown_triggered.clear()
        self._low_battery_confirm_count = 0
        self._last_onbatt_sms_ts = None
        logger.info(
            "\033[92m UPS POWER RESTORED \033[0m battery=%.1f%% ✔",
            reading.battery_pct,
        )
        msg = (
            f"[OvoMatrix] ✔ عاد التيار الكهربائي. "
            f"شحن البطارية: {reading.battery_pct:.1f}%."
        )
        self._send_sms(
            level        = "NORMAL",
            nh3_ppm      = 0.0,
            temp_c       = 0.0,
            action_taken = msg,
        )
        if self._on_status_change:
            self._safe_callback(reading)

    # ── Safe OS shutdown ─────────────────────────────────────────────────────

    def _execute_shutdown(self, grace_seconds: int) -> None:
        """
        Wait ``grace_seconds`` to allow main_backend.py to flush SQLite WAL
        and close all connections, then issue the OS shutdown command.

        The ``_shutdown_event`` is set FIRST so the backend's ordered teardown
        sequence runs during the grace period (closes DB → disconnects MQTT →
        logs final stats).  Only after the grace period does the OS command run.
        """
        logger.critical(
            "SafeShutdown: signalling backend teardown (grace=%ds) …",
            grace_seconds,
        )
        self._shutdown_event.set()   # Trigger main_backend graceful shutdown

        for remaining in range(grace_seconds, 0, -5):
            logger.critical(
                "\033[41;97m SHUTDOWN IN %d SECONDS \033[0m "
                "(waiting for DB flush …)",
                remaining,
            )
            self._stop_event.wait(timeout=5.0)
            if remaining <= 5:
                break

        logger.critical("SafeShutdown: executing OS shutdown now.")
        _os_shutdown()

    # ── MQTT helpers ─────────────────────────────────────────────────────────

    def _connect_mqtt(self) -> None:
        try:
            import paho.mqtt.client as mqtt
            client_id  = f"ovomatrix-ups-{os.getpid()}"
            self._mqtt_client = mqtt.Client(client_id=client_id)
            self._mqtt_client.on_connect = self._on_mqtt_connect
            self._mqtt_client.on_message = self._on_mqtt_message
            self._mqtt_client.connect(self._broker_host, self._broker_port, keepalive=60)
            self._mqtt_client.loop_start()
        except Exception as exc:    # noqa: BLE001
            logger.warning("UPSMonitor: MQTT connect failed: %s — continuing without MQTT.", exc)
            self._mqtt_client = None

    def _disconnect_mqtt(self) -> None:
        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:  # noqa: BLE001
                pass
            self._mqtt_client = None

    def _on_mqtt_connect(self, _client, _userdata, _flags, rc: int) -> None:
        if rc == 0:
            logger.info("UPSMonitor: MQTT connected ✔ (topic=%s)", POWER_MQTT_TOPIC)
            # Listen for simulation commands (START, STOP, SIMULATE_POWER_LOSS)
            control_topic = os.environ.get("MODBUS_MQTT_CONTROL_TOPIC", "farm/telemetry/control")
            self._mqtt_client.subscribe(control_topic)
            logger.info(f"UPSMonitor: Subscribed to control topic '{control_topic}'")
        else:
            logger.warning("UPSMonitor: MQTT connection refused (rc=%d).", rc)

    def _on_mqtt_message(self, _client, _userdata, msg):
        """Handle incoming control commands (SIMULATE_POWER_LOSS)."""
        try:
            payload = json.loads(msg.payload.decode())
            cmd = payload.get("command", "").upper()
            
            if cmd == "SIMULATE_POWER_LOSS":
                if isinstance(self._backend, _MockUPSBackend):
                    self._backend.force_outage()
                    logger.info("UPSMonitor: SIMULATE_POWER_LOSS command processed.")
                else:
                    logger.warning("UPSMonitor: SIMULATE_POWER_LOSS ignored (not in MOCK_UPS mode).")
        except Exception as e:
            logger.error(f"UPSMonitor: Failed to process control message: {e}")

    def _mqtt_publish(self, reading: UPSReading) -> None:
        if not self._mqtt_client:
            return
        try:
            payload = reading.to_mqtt_payload()
            self._mqtt_client.publish(
                topic   = POWER_MQTT_TOPIC,
                payload = payload,
                qos     = 1,
                retain  = True,   # retain so new subscribers see last state immediately
            )
            self.stats.bump(mqtt_publishes=1)
            logger.debug("UPSMonitor: MQTT published → %s | %s", POWER_MQTT_TOPIC, payload)
        except Exception as exc:    # noqa: BLE001
            logger.warning("UPSMonitor: MQTT publish failed: %s", exc)

    # ── GSM dispatch ─────────────────────────────────────────────────────────

    def _send_sms(
        self,
        level:        str,
        nh3_ppm:      float,
        temp_c:       float,
        action_taken: str,
    ) -> None:
        """
        Dispatch an SMS via gsm_layer.send_alert().

        Wrapped defensively so any GSM failure cannot break the monitor loop.
        Only fires for NORMAL (power restored), WARNING (ONBATT), CRITICAL (LowBat).
        """
        try:
            from gsm_layer import send_alert, AlertLevel
            alert_level = AlertLevel(level)
            
            # 1. Dispatch SMS
            send_alert(
                level        = alert_level,
                nh3_ppm      = nh3_ppm,
                temp_c       = temp_c,
                action_taken = action_taken,
            )
            self.stats.bump(sms_sent=1)

            # 2. Log to Database for UI visibility
            if self.db:
                # Map AlertLevel back to string if needed, or just use 'level'
                self.db.log_alert(
                    level        = level,
                    nh3_ppm      = nh3_ppm,
                    temp_c       = temp_c,
                    action_taken = action_taken,
                )
        except Exception as exc:        # noqa: BLE001
            logger.error("UPSMonitor: alert dispatch failed: %s", exc, exc_info=True)

    # ── GUI callback ──────────────────────────────────────────────────────────

    def _safe_callback(self, reading: UPSReading) -> None:
        try:
            if self._on_status_change:
                self._on_status_change(reading)
        except Exception as exc:    # noqa: BLE001
            logger.debug("UPSMonitor: on_status_change callback raised: %s", exc)


# ---------------------------------------------------------------------------
# OS shutdown — cross-platform
# ---------------------------------------------------------------------------

def _os_shutdown() -> None:
    """
    Issue a clean OS shutdown command.

    Windows: shutdown /s /t 1
    Linux  : shutdown -h +1  (or systemctl poweroff)
    macOS  : shutdown -h +1

    The function logs and then executes — it does not return.
    """
    system = platform.system().lower()
    
    # In Mock mode, we don't want to actually turn off the developer's / operator's PC
    if MOCK_UPS:
        logger.critical(f"EXECUTING SAFE SHUTDOWN | platform={system.upper()} [SKIPPED - MOCK_UPS IS ACTIVE]")
        return
        
    logger.critical(
        "EXECUTING SAFE SHUTDOWN | platform=%s", system.upper()
    )
    # SEC-02: Use subprocess (not os.system) to prevent shell-injection.
    if system == "windows":
        subprocess.run(["shutdown", "/s", "/t", "1"], check=False)
    else:
        ret = subprocess.run(["systemctl", "poweroff"], check=False)
        if ret.returncode != 0:
            subprocess.run(["shutdown", "-h", "+1"], check=False)


# ---------------------------------------------------------------------------
# CLI self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import signal as _signal

    logging.basicConfig(
        stream  = sys.stdout,
        level   = logging.DEBUG,
        format  = "%(asctime)s  %(levelname)-8s  [UPS] %(message)s",
        datefmt = "%Y-%m-%dT%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description = "OvoMatrix v8.0 — UPS Monitor Self-Test",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    args = parser.parse_args()

    stop_ev = threading.Event()

    # Override MOCK timings for a quick demo.
    MOCK_OUTAGE_DELAY        = 8.0
    MOCK_DISCHARGE_SECONDS   = 60.0

    monitor = UPSMonitorWorker(
        broker_host    = args.host,
        broker_port    = args.port,
        shutdown_event = stop_ev,
    )

    def _sig(_n, _f) -> None:
        logger.info("Ctrl-C — stopping monitor …")
        monitor.stop()
        stop_ev.set()

    _signal.signal(_signal.SIGINT,  _sig)
    _signal.signal(_signal.SIGTERM, _sig)

    monitor.start()
    stop_ev.wait()

    snap = monitor.get_stats()
    print(
        f"\n  UPS Monitor stats:\n"
        f"  polls={snap['total_polls']}  "
        f"onbatt={snap['onbatt_transitions']}  "
        f"lowbatt={snap['low_battery_events']}  "
        f"sms={snap['sms_sent']}  "
        f"mqtt={snap['mqtt_publishes']}\n"
    )

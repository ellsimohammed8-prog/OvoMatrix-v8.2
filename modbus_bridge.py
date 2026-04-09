"""
OvoMatrix v8.0 — Phase 3, Step 9
modbus_bridge.py: Industrial Modbus RTU → MQTT Telemetry Bridge

Role:
  This module replaces the software simulator with physical IP65-grade
  industrial sensors connected via RS-485 Modbus RTU.  It polls two
  physical slave devices on a configurable serial bus, normalises the raw
  integer register values into engineering-unit floats, then publishes a
  JSON payload to ``farm/telemetry`` in the *exact same format* as
  simulator.py — so mqtt_subscriber.py and every downstream component
  require zero modifications.

Hardware topology:
  ┌──────────────┐   RS-485 (half-duplex)   ┌─────────────────────────────┐
  │  Beelink PC  │◄─────────────────────────►│ Slave 01 — NH₃ Sensor       │
  │  (USB-RS485  │                           │   Reg 0x0000 : NH3 × 10     │
  │   adapter)   │◄─────────────────────────►│ Slave 02 — Temp/Humid Sensor│
  └──────────────┘                           │   Reg 0x0000 : Temp × 10    │
                                             │   Reg 0x0001 : Humid × 10   │
                                             └─────────────────────────────┘

Payload format (identical to simulator.py so subscriber needs no changes):
  {
    "timestamp":  "2026-03-08T20:00:01.123456Z",   ← ISO-8601 UTC
    "nh3_ppm":    12.5,                             ← float, 1 decimal
    "temp_c":     26.3,                             ← float, 1 decimal
    "humid_pct":  65.1,                             ← float, 1 decimal
    "source":     "modbus_bridge",                  ← identifies real HW
    "uuid":       "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx"
  }

Error heartbeat (published on sensor failure):
  {
    "error":     "SENSOR_DISCONNECTED",
    "sensor_id": 1,
    "timestamp": "2026-03-08T20:00:01.123456Z"
  }
  Note: This is published to ``farm/telemetry/errors`` (NOT the main topic)
  to avoid polluting the readings table with non-numeric data.  The
  alert_engine.py can be extended to subscribe to this topic separately.
  The bridge also logs CRITICAL, which is picked up by the Watchdog.

Mock mode (MOCK_MODBUS=True):
  Generates realistic sensor readings using the same drift algorithm as
  simulator.py — no RS-485 adapter required for development/testing.

Dependencies:
  pip install pymodbus paho-mqtt python-dotenv
  (pymodbus ≥ 3.x API is used; 2.x users must adjust import paths)

Run standalone:
  python modbus_bridge.py
  python modbus_bridge.py --port COM4 --scenario WARNING
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# .env loading (graceful absence)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", override=False)
except ImportError:
    pass

logger = logging.getLogger("ovomatrix.modbus_bridge")

# ---------------------------------------------------------------------------
# Configuration — fully driven by .env / environment variables
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


# ── Mock / hardware toggle ────────────────────────────────────────────────────
MOCK_MODBUS: bool       = _env_bool("MOCK_MODBUS",  True)

# ── RS-485 serial port ────────────────────────────────────────────────────────
MODBUS_PORT: str        = _env_str ("MODBUS_PORT",   "COM3")
MODBUS_BAUD: int        = _env_int ("MODBUS_BAUD",   9600)
MODBUS_BYTESIZE: int    = _env_int ("MODBUS_BYTESIZE", 8)
MODBUS_PARITY: str      = _env_str ("MODBUS_PARITY", "N")   # N / E / O
MODBUS_STOPBITS: int    = _env_int ("MODBUS_STOPBITS", 1)
MODBUS_TIMEOUT: float   = _env_float("MODBUS_TIMEOUT", 1.5)  # seconds

# ── Slave IDs ──────────────────────────────────────────────────────────────────
NH3_SLAVE_ID: int       = _env_int("NH3_SLAVE_ID",   1)
TH_SLAVE_ID:  int       = _env_int("TH_SLAVE_ID",    2)

# ── Holding register addresses (0-based) ─────────────────────────────────────
NH3_REGISTER:   int     = _env_int("NH3_REGISTER",   0x0000)  # NH3 × 10
TEMP_REGISTER:  int     = _env_int("TEMP_REGISTER",  0x0000)  # Temp × 10
HUMID_REGISTER: int     = _env_int("HUMID_REGISTER", 0x0001)  # Humid × 10

# A negative raw register value indicates a signed 16-bit reading.
# Set True if your sensor uses signed 16-bit integers (e.g., sub-zero temps).
MODBUS_SIGNED: bool     = _env_bool("MODBUS_SIGNED", True)

# ── Normalisation factor ──────────────────────────────────────────────────────
#  Industrial sensors often encode floats as integers × 10 (e.g., 265 = 26.5°C)
NH3_SCALE:   float      = _env_float("NH3_SCALE",   10.0)
TEMP_SCALE:  float      = _env_float("TEMP_SCALE",  10.0)
HUMID_SCALE: float      = _env_float("HUMID_SCALE", 10.0)

# ── MQTT ───────────────────────────────────────────────────────────────────────
MQTT_BROKER_HOST:   str = _env_str("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT:   int = _env_int("MQTT_BROKER_PORT", 1883)
MQTT_TOPIC_DATA:    str = _env_str("MODBUS_MQTT_TOPIC",       "farm/telemetry")
MQTT_TOPIC_ERROR:   str = _env_str("MODBUS_MQTT_ERROR_TOPIC", "farm/telemetry/errors")
MQTT_TOPIC_CONTROL: str = _env_str("MODBUS_MQTT_CONTROL_TOPIC", "farm/telemetry/control")
MQTT_QOS:           int = 1

# ── Polling ────────────────────────────────────────────────────────────────────
POLL_INTERVAL:  float   = _env_float("MODBUS_POLL_INTERVAL", 1.0)  # seconds
MAX_RETRIES:    int     = _env_int ("MODBUS_MAX_RETRIES",    3)
RETRY_DELAY:    float   = _env_float("MODBUS_RETRY_DELAY",  0.5)   # seconds

# ── Sanity bounds (discard physically impossible readings) ────────────────────
NH3_MIN,   NH3_MAX   =  0.0, 500.0
TEMP_MIN,  TEMP_MAX  = -40.0, 85.0
HUMID_MIN, HUMID_MAX =  0.0, 100.0

# ── Source identifier tag embedded in every MQTT payload ─────────────────────
SOURCE_TAG: str         = _env_str("MODBUS_SOURCE_TAG", "modbus_bridge")

# ---------------------------------------------------------------------------
# Raw sensor reading dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SensorReading:
    """Normalised reading from one polling cycle (all sensors combined)."""
    nh3_ppm:   float
    temp_c:    float
    humid_pct: float
    timestamp: str
    reading_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_mqtt_payload(self) -> str:
        """
        Serialise to the JSON format expected by mqtt_subscriber.py.

        This is the *exact* schema validated by MessageValidator in
        mqtt_subscriber.py — field names, types, and value ranges must match.
        """
        return json.dumps(
            {
                "timestamp":  self.timestamp,
                "nh3_ppm":    round(self.nh3_ppm,   1),
                "temp_c":     round(self.temp_c,    1),
                "humid_pct":  round(self.humid_pct, 1),
                "source":     SOURCE_TAG,
                "uuid":       self.reading_uuid,
            },
            separators=(",", ":"),
        )


# ---------------------------------------------------------------------------
# Modbus backends
# ---------------------------------------------------------------------------

class _ModbusBackendBase:
    """Abstract interface all backends must satisfy."""

    def read_nh3(self) -> float:
        raise NotImplementedError

    def read_temp_humid(self) -> tuple[float, float]:
        raise NotImplementedError

    def close(self) -> None:
        pass


# ── Real Modbus RTU backend ────────────────────────────────────────────────────

class _RealModbusBackend(_ModbusBackendBase):
    """
    Modbus RTU client using pymodbus ≥ 3.x.

    pymodbus 3.x moved to a synchronous/async split.  This class uses the
    synchronous ``ModbusSerialClient`` which is a direct drop-in replacement
    for pymodbus 2.x ``ModbusSerialClient(method='rtu', …)``.

    Register map (holding registers, 0-based):
      NH₃ sensor  (slave_id=NH3_SLAVE_ID):
        0x0000 : NH₃ concentration (register × NH3_SCALE = ppm)

      Temp/Humid sensor (slave_id=TH_SLAVE_ID):
        0x0000 : Temperature        (register × TEMP_SCALE = °C)
        0x0001 : Relative Humidity  (register × HUMID_SCALE = %RH)

    All register values are unsigned 16-bit unless MODBUS_SIGNED=True.
    """

    def __init__(self) -> None:
        try:
            from pymodbus.client import ModbusSerialClient
        except ImportError as exc:
            raise ImportError(
                "pymodbus ≥ 3.0 is required for hardware mode. "
                "Install with: pip install pymodbus"
            ) from exc

        self._client = ModbusSerialClient(
            port     = MODBUS_PORT,
            baudrate = MODBUS_BAUD,
            bytesize = MODBUS_BYTESIZE,
            parity   = MODBUS_PARITY,
            stopbits = MODBUS_STOPBITS,
            timeout  = MODBUS_TIMEOUT,
        )
        if not self._client.connect():
            raise ConnectionError(
                f"Cannot open serial port '{MODBUS_PORT}'. "
                "Check that the RS-485 adapter is connected and the port is correct."
            )
        logger.info(
            "Modbus RTU connected | port=%s baud=%d parity=%s timeout=%.1fs",
            MODBUS_PORT, MODBUS_BAUD, MODBUS_PARITY, MODBUS_TIMEOUT,
        )

    # ── Private helpers ──────────────────────────────────────────────────────

    def _read_register(self, slave_id: int, address: int) -> int:
        """
        Read a single holding register with retry logic.

        Raises:
            IOError: If the register cannot be read after MAX_RETRIES attempts.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._client.read_holding_registers(
                    address    = address,
                    count      = 1,
                    slave      = slave_id,
                )
                if response.isError():
                    raise IOError(
                        f"Modbus error response from slave {slave_id} "
                        f"addr 0x{address:04X}: {response}"
                    )
                raw = response.registers[0]
                # Convert unsigned 16-bit to signed if configured.
                if MODBUS_SIGNED and raw > 0x7FFF:
                    raw -= 0x10000
                return raw
            except Exception as exc:    # noqa: BLE001
                last_exc = exc
                logger.debug(
                    "Register read attempt %d/%d failed | slave=%d addr=0x%04X: %s",
                    attempt, MAX_RETRIES, slave_id, address, exc,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

        raise IOError(
            f"Slave {slave_id} addr 0x{address:04X} unreachable "
            f"after {MAX_RETRIES} attempts: {last_exc}"
        )

    def _read_registers(self, slave_id: int, address: int, count: int) -> list[int]:
        """
        Read multiple contiguous holding registers in a single RTU frame.

        This is preferred over individual reads to reduce bus occupancy.

        Raises:
            IOError: If the registers cannot be read after MAX_RETRIES attempts.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._client.read_holding_registers(
                    address = address,
                    count   = count,
                    slave   = slave_id,
                )
                if response.isError():
                    raise IOError(
                        f"Modbus error from slave {slave_id}: {response}"
                    )
                regs = response.registers
                if MODBUS_SIGNED:
                    regs = [r - 0x10000 if r > 0x7FFF else r for r in regs]
                return regs
            except Exception as exc:    # noqa: BLE001
                last_exc = exc
                logger.debug(
                    "Multi-register read attempt %d/%d | slave=%d: %s",
                    attempt, MAX_RETRIES, slave_id, exc,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

        raise IOError(
            f"Slave {slave_id} unreachable after {MAX_RETRIES} attempts: {last_exc}"
        )

    # ── Public interface ─────────────────────────────────────────────────────

    def read_nh3(self) -> float:
        """
        Read NH₃ concentration from slave NH3_SLAVE_ID, register NH3_REGISTER.

        The raw register value is divided by NH3_SCALE (default 10.0) to
        produce the engineering-unit float.  Example: register=125 → 12.5 ppm.

        Raises:
            IOError: On sensor timeout or Modbus error.
        """
        raw = self._read_register(NH3_SLAVE_ID, NH3_REGISTER)
        ppm = raw / NH3_SCALE
        logger.debug("NH₃ raw=%d → %.2f ppm", raw, ppm)
        return ppm

    def read_temp_humid(self) -> tuple[float, float]:
        """
        Read temperature and humidity from slave TH_SLAVE_ID in one RTU frame.

        Registers (sequential, read in a single request):
          TEMP_REGISTER   : temperature (raw / TEMP_SCALE → °C)
          HUMID_REGISTER  : humidity    (raw / HUMID_SCALE → %RH)

        Raises:
            IOError: On sensor timeout or Modbus error.
        """
        # Read 2 consecutive registers starting at the lower of the two addresses.
        base    = min(TEMP_REGISTER, HUMID_REGISTER)
        regs    = self._read_registers(TH_SLAVE_ID, base, 2)
        t_idx   = TEMP_REGISTER  - base
        h_idx   = HUMID_REGISTER - base
        temp_c    = regs[t_idx] / TEMP_SCALE
        humid_pct = regs[h_idx] / HUMID_SCALE
        logger.debug(
            "Temp raw=%d → %.2f°C | Humid raw=%d → %.2f%%",
            regs[t_idx], temp_c, regs[h_idx], humid_pct,
        )
        return temp_c, humid_pct

    def close(self) -> None:
        try:
            self._client.close()
            logger.info("Modbus RTU connection closed.")
        except Exception:  # noqa: BLE001
            pass


# ── Mock Modbus backend ────────────────────────────────────────────────────────

class _MockModbusBackend(_ModbusBackendBase):
    """
    Software-only backend that simulates industrial sensor readings using
    the same gradual-drift algorithm as simulator.py.

    Purpose:
      Allows the full OvoMatrix pipeline to be tested (including modbus_bridge →
      MQTT → subscriber → DB → alert_engine → GSM) without any RS-485 hardware.

    Drift model (per poll tick):
      - Values evolve continuously as cumulative sums of small delta steps.
      - Independent noise is added each tick (range: ±NOISE_SCALE).
      - Values are clamped to physical operating bounds.
    """

    # Starting values (clean barn, stable climate)
    _NH3_BASE:   float = 10.0
    _TEMP_BASE:  float = 26.0
    _HUMID_BASE: float = 65.0

    # Max drift step per tick (±)
    _NH3_DRIFT:   float = 0.3
    _TEMP_DRIFT:  float = 0.1
    _HUMID_DRIFT: float = 0.2

    # Random noise amplitude per tick
    _NH3_NOISE:   float = 0.15
    _TEMP_NOISE:  float = 0.05
    _HUMID_NOISE: float = 0.10

    def __init__(self, initial_nh3: float = _NH3_BASE) -> None:
        self._nh3   = initial_nh3
        self._temp  = self._TEMP_BASE
        self._humid = self._HUMID_BASE
        self._lock  = threading.Lock()
        self._scenario_base_nh3 = self._NH3_BASE
        logger.info(
            "MockModbus started | NH₃=%.1fppm Temp=%.1f°C Humid=%.1f%%",
            self._nh3, self._temp, self._humid,
        )

    def set_scenario(self, mode: str):
        with self._lock:
            if mode == "EMERGENCY":
                self._scenario_base_nh3 = 65.0
                self._nh3 = 65.0
                self._temp = 32.0
                self._humid = 80.0
            elif mode == "WARNING":
                self._scenario_base_nh3 = 28.0
                self._nh3 = 28.0
                self._temp = 27.0
                self._humid = 70.0
            else:
                self._scenario_base_nh3 = self._NH3_BASE
                self._nh3 = self._NH3_BASE
                self._temp = self._TEMP_BASE
                self._humid = self._HUMID_BASE
        logger.info("MockModbus scenario forced to %s: SNAP applied.", mode)

    def _drift(self, value: float, base: float, drift: float, noise: float,
               lo: float, hi: float) -> float:
        """Apply one drift + noise step with convergence."""
        diff = base - value
        # Slow pull towards the base if we are far
        if abs(diff) > drift:
            new_val = value + math.copysign(drift, diff)
        else:
            new_val = value
            
        # Continuous random walk
        noise_v = random.uniform(-noise, noise)
        new_val += noise_v
        
        # Prevent it from wandering too far from base when it should be stable
        if abs(new_val - base) > noise * 4:
            new_val -= math.copysign(noise * 0.5, new_val - base)
            
        return max(lo, min(hi, new_val))

    def read_nh3(self) -> float:
        with self._lock:
            self._nh3 = self._drift(
                self._nh3, self._scenario_base_nh3, self._NH3_DRIFT, self._NH3_NOISE,
                NH3_MIN, NH3_MAX,
            )
            return round(self._nh3, 2)

    def read_temp_humid(self) -> tuple[float, float]:
        with self._lock:
            self._temp = self._drift(
                self._temp, self._TEMP_BASE, self._TEMP_DRIFT, self._TEMP_NOISE,
                TEMP_MIN, TEMP_MAX,
            )
            self._humid = self._drift(
                self._humid, self._HUMID_BASE, self._HUMID_DRIFT, self._HUMID_NOISE,
                HUMID_MIN, HUMID_MAX,
            )
            return round(self._temp, 2), round(self._humid, 2)

    def inject_spike(self, nh3_ppm: float) -> None:
        """Manually inject an NH₃ spike (useful for integration testing)."""
        with self._lock:
            self._nh3 = min(nh3_ppm, NH3_MAX)
        logger.warning("MockModbus: NH₃ spike injected → %.1f ppm", self._nh3)


# ---------------------------------------------------------------------------
# Bounds validator
# ---------------------------------------------------------------------------

def _validate_bounds(nh3: float, temp: float, humid: float) -> list[str]:
    """
    Return a list of validation error strings.  Empty list = all valid.

    Rejects physically impossible values that indicate sensor saturation,
    wiring faults, or firmware errors.
    """
    errors: list[str] = []
    if not (NH3_MIN <= nh3 <= NH3_MAX):
        errors.append(f"NH3 out of bounds: {nh3:.2f} (expected {NH3_MIN}–{NH3_MAX})")
    if not (TEMP_MIN <= temp <= TEMP_MAX):
        errors.append(f"Temp out of bounds: {temp:.2f}°C (expected {TEMP_MIN}–{TEMP_MAX})")
    if not (HUMID_MIN <= humid <= HUMID_MAX):
        errors.append(f"Humid out of bounds: {humid:.2f}% (expected {HUMID_MIN}–{HUMID_MAX})")
    return errors


# ---------------------------------------------------------------------------
# Statistics counter
# ---------------------------------------------------------------------------

@dataclass
class ModbusStats:
    """Thread-safe operational counters."""
    _lock:           threading.Lock = field(default_factory=threading.Lock, repr=False)
    polls_ok:        int = 0     # successful poll cycles
    polls_failed:    int = 0     # poll cycles with ≥ 1 sensor error
    nh3_errors:      int = 0     # NH₃ sensor read failures
    th_errors:       int = 0     # Temp/Humid sensor read failures
    bounds_rejected: int = 0     # readings outside physical bounds
    mqtt_publishes:  int = 0     # successful MQTT publishes
    mqtt_errors:     int = 0     # failed MQTT publishes

    def bump(self, **kwargs: int) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)

    def snapshot(self) -> dict:
        with self._lock:
            return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


# ---------------------------------------------------------------------------
# ModbusBridgeWorker — the public component class
# ---------------------------------------------------------------------------

class ModbusBridgeWorker(threading.Thread):
    """
    Industrial Modbus RTU → MQTT bridge thread.

    Usage from main_backend.py:
    ::
        bridge = ModbusBridgeWorker(
            broker_host = cfg.host,
            broker_port = cfg.port,
        )
        bridge.start()
        …
        bridge.stop()

    The thread is marked ``daemon=True`` by default so it exits automatically
    with the parent process.  main_backend.py calls stop() to ensure the
    serial port is closed cleanly before process exit.
    """

    def __init__(
        self,
        broker_host: str = MQTT_BROKER_HOST,
        broker_port: int = MQTT_BROKER_PORT,
        daemon:      bool = True,
    ) -> None:
        super().__init__(name="ModbusBridge", daemon=daemon)
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._stop_event  = threading.Event()
        self.stats        = ModbusStats()
        self._running     = False # Start paused by default for a clean cold-start

        # Backend selected at construction time.
        if MOCK_MODBUS:
            self._backend: _ModbusBackendBase = _MockModbusBackend()
            logger.info("ModbusBridge: MOCK mode | no RS-485 hardware required.")
        else:
            self._backend = _RealModbusBackend()
            logger.info(
                "ModbusBridge: HARDWARE mode | port=%s slave_nh3=%d slave_th=%d",
                MODBUS_PORT, NH3_SLAVE_ID, TH_SLAVE_ID,
            )

        # MQTT client (initialised lazily in run())
        self._mqtt = None

    # ── Public control ──────────────────────────────────────────────────────

    def stop(self, timeout: float = 5.0) -> None:
        """Request graceful shutdown, close serial port, and join thread."""
        logger.info("ModbusBridge stopping …")
        self._stop_event.set()
        self.join(timeout=timeout)
        self._backend.close()
        if self.is_alive():
            logger.warning("ModbusBridge did not exit within %.1f s.", timeout)
        else:
            logger.info("ModbusBridge stopped cleanly.")

    def get_stats(self) -> dict:
        return self.stats.snapshot()

    def inject_spike(self, nh3_ppm: float) -> None:
        """
        Manually inject an NH₃ spike in MOCK mode (for testing alert pipeline).
        No-op in hardware mode.
        """
        if isinstance(self._backend, _MockModbusBackend):
            self._backend.inject_spike(nh3_ppm)
        else:
            logger.warning("inject_spike() is only available in MOCK mode.")

    # ── QThread-compatible: run() is the entry point ─────────────────────────

    def run(self) -> None:
        logger.info(
            "ModbusBridge run-loop started | poll=%.1fs topic=%s",
            POLL_INTERVAL, MQTT_TOPIC_DATA,
        )
        self._mqtt_connect()

        while not self._stop_event.is_set():
            t0 = time.monotonic()
            if self._running:
                self._poll_cycle()
            else:
                # Optionally publish a 'PAUSED' heartbeat
                pass
            elapsed   = time.monotonic() - t0
            sleep_for = max(0.0, POLL_INTERVAL - elapsed)
            self._stop_event.wait(timeout=sleep_for)

        self._mqtt_disconnect()
        snap = self.stats.snapshot()
        logger.info(
            "ModbusBridge finished | ok=%d fail=%d nh3_err=%d th_err=%d "
            "rejected=%d mqtt_pub=%d",
            snap["polls_ok"],    snap["polls_failed"],
            snap["nh3_errors"],  snap["th_errors"],
            snap["bounds_rejected"], snap["mqtt_publishes"],
        )

    # ── Core poll cycle ──────────────────────────────────────────────────────

    def _poll_cycle(self) -> None:
        """
        One complete sensor poll cycle.

        Steps:
          1. Read NH₃ from slave NH3_SLAVE_ID.
          2. Read Temp + Humidity from slave TH_SLAVE_ID.
          3. Validate bounds on all three values.
          4. Build SensorReading and publish to MQTT.
          5. On any read failure, publish an error heartbeat and log CRITICAL.
        """
        now_ts  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        nh3     = None
        temp    = None
        humid   = None
        failed  = False

        # ── Read NH₃ ─────────────────────────────────────────────────────────
        try:
            nh3 = self._backend.read_nh3()
        except Exception as exc:      # noqa: BLE001
            self.stats.bump(nh3_errors=1, polls_failed=1)
            failed = True
            logger.critical(
                "\033[91mMODBUS SENSOR ERROR\033[0m NH₃ slave=%d: %s",
                NH3_SLAVE_ID, exc,
            )
            self._publish_error(NH3_SLAVE_ID, "SENSOR_DISCONNECTED", now_ts)

        # ── Read Temp + Humidity ──────────────────────────────────────────────
        try:
            temp, humid = self._backend.read_temp_humid()
        except Exception as exc:      # noqa: BLE001
            self.stats.bump(th_errors=1, polls_failed=1)
            failed = True
            logger.critical(
                "\033[91mMODBUS SENSOR ERROR\033[0m Temp/Humid slave=%d: %s",
                TH_SLAVE_ID, exc,
            )
            self._publish_error(TH_SLAVE_ID, "SENSOR_DISCONNECTED", now_ts)

        if failed or nh3 is None or temp is None or humid is None:
            # Cannot build a complete reading — skip this cycle.
            return

        # ── Bounds validation ─────────────────────────────────────────────────
        errors = _validate_bounds(nh3, temp, humid)
        if errors:
            self.stats.bump(bounds_rejected=1, polls_failed=1)
            for err in errors:
                logger.warning("ModbusBridge: Out-of-bounds rejected | %s", err)
            return

        # ── Build and publish reading ─────────────────────────────────────────
        reading = SensorReading(
            nh3_ppm   = nh3,
            temp_c    = temp,
            humid_pct = humid,
            timestamp = now_ts,
        )
        self._publish_reading(reading)
        self.stats.bump(polls_ok=1)

        logger.debug(
            "Modbus poll OK | NH₃=%6.2fppm  Temp=%6.2f°C  Humid=%6.2f%%  uuid=%.8s…",
            reading.nh3_ppm, reading.temp_c, reading.humid_pct, reading.reading_uuid,
        )

    # ── MQTT helpers ─────────────────────────────────────────────────────────

    def _mqtt_connect(self) -> None:
        try:
            import paho.mqtt.client as mqtt
            client_id   = f"ovomatrix-modbus-{os.getpid()}"
            self._mqtt  = mqtt.Client(client_id=client_id)
            self._mqtt.on_connect    = self._on_mqtt_connect
            self._mqtt.on_disconnect = self._on_mqtt_disconnect
            self._mqtt.on_message    = self._on_mqtt_message
            self._mqtt.connect(self._broker_host, self._broker_port, keepalive=60)
            self._mqtt.loop_start()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "ModbusBridge: MQTT connect failed: %s — data will not be published.", exc
            )
            self._mqtt = None

    def _on_mqtt_message(self, _client, _userdata, msg):
        """Handle incoming control commands (START, STOP, RESET, SET_SCENARIO)."""
        try:
            payload = json.loads(msg.payload.decode())
            cmd = payload.get("command", "").upper()
            
            if cmd == "STOP":
                self._running = False
                logger.info("ModbusBridge: STOP command received — publishing paused.")
            elif cmd == "START":
                self._running = True
                logger.info("ModbusBridge: START command received — publishing resumed.")
            elif cmd == "RESET":
                self._running = False
                if isinstance(self._backend, _MockModbusBackend):
                    self._backend.set_scenario("NORMAL")
                logger.info("ModbusBridge: RESET command received — baseline restored and paused.")
            elif cmd == "SET_SCENARIO":
                mode = payload.get("mode", "NORMAL").upper()
                if isinstance(self._backend, _MockModbusBackend):
                    self._backend.set_scenario(mode)
                
                # Signal the alert engine to clear its hysteresis and bypass cooldown
                if self._mqtt:
                    self._mqtt.publish("farm/telemetry/control", json.dumps({"command": "CLEAR_HYSTERESIS"}))
                
                logger.info(f"ModbusBridge: Scenario forced to {mode}.")
        except Exception as e:
            logger.error(f"ModbusBridge: Failed to process control message: {e}")

    def _mqtt_disconnect(self) -> None:
        if self._mqtt:
            try:
                self._mqtt.loop_stop()
                self._mqtt.disconnect()
            except Exception:  # noqa: BLE001
                pass
            self._mqtt = None

    def _on_mqtt_connect(self, _client, _ud, _flags, rc: int) -> None:
        if rc == 0:
            logger.info(
                "ModbusBridge: MQTT connected ✔ → %s:%d",
                self._broker_host, self._broker_port,
            )
            self._mqtt.subscribe(MQTT_TOPIC_CONTROL)
            logger.info(f"ModbusBridge: Subscribed to control topic '{MQTT_TOPIC_CONTROL}'")
        else:
            logger.warning("ModbusBridge: MQTT connection refused rc=%d", rc)

    def _on_mqtt_disconnect(self, _client, _ud, rc: int) -> None:
        if rc != 0:
            logger.warning("ModbusBridge: MQTT unexpectedly disconnected rc=%d", rc)

    def _publish_reading(self, reading: SensorReading) -> None:
        """Publish a normalised SensorReading to farm/telemetry."""
        if not self._mqtt:
            return
        try:
            payload = reading.to_mqtt_payload()
            self._mqtt.publish(
                topic   = MQTT_TOPIC_DATA,
                payload = payload,
                qos     = MQTT_QOS,
            )
            self.stats.bump(mqtt_publishes=1)
        except Exception as exc:  # noqa: BLE001
            self.stats.bump(mqtt_errors=1)
            logger.error("ModbusBridge: MQTT publish failed: %s", exc)

    def _publish_error(self, sensor_id: int, error_code: str, ts: str) -> None:
        """
        Publish a sensor-disconnection heartbeat to the error topic.

        This payload is intentionally NOT routed to ``farm/telemetry`` to
        avoid polluting the readings table with non-numeric records.  It is
        sent to ``farm/telemetry/errors`` so it can be consumed by
        a dedicated alert subscriber without modifying mqtt_subscriber.py.
        """
        if not self._mqtt:
            return
        try:
            payload = json.dumps(
                {
                    "error":     error_code,
                    "sensor_id": sensor_id,
                    "timestamp": ts,
                    "source":    SOURCE_TAG,
                },
                separators=(",", ":"),
            )
            self._mqtt.publish(
                topic   = MQTT_TOPIC_ERROR,
                payload = payload,
                qos     = MQTT_QOS,
                retain  = False,
            )
            logger.debug(
                "ModbusBridge: error heartbeat published | sensor=%d code=%s",
                sensor_id, error_code,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("ModbusBridge: error heartbeat publish failed: %s", exc)


# ---------------------------------------------------------------------------
# CLI self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import signal as _signal

    logging.basicConfig(
        stream  = sys.stdout,
        level   = logging.DEBUG,
        format  = "%(asctime)s  %(levelname)-8s  [MODB] %(message)s",
        datefmt = "%Y-%m-%dT%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description     = "OvoMatrix v8.0 — Modbus Bridge Self-Test",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host",    default="localhost", help="MQTT broker host")
    parser.add_argument("--port",    type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--spike",   type=float, default=0.0,
                        help="Inject NH₃ spike value after 10s (mock only, 0=disabled)")
    parser.add_argument("--duration", type=int,   default=30,
                        help="Run duration in seconds (0=indefinite)")
    args = parser.parse_args()

    bridge = ModbusBridgeWorker(broker_host=args.host, broker_port=args.port)

    def _shutdown(_n, _f) -> None:
        print("\n[SIGINT] Stopping bridge …")
        bridge.stop()

    _signal.signal(_signal.SIGINT,  _shutdown)
    _signal.signal(_signal.SIGTERM, _shutdown)

    bridge.start()

    if args.spike > 0:
        def _inject():
            time.sleep(10)
            logger.warning("Injecting NH₃ spike → %.1f ppm …", args.spike)
            bridge.inject_spike(args.spike)
        threading.Thread(target=_inject, daemon=True).start()

    end_time = time.monotonic() + args.duration if args.duration > 0 else float("inf")
    try:
        while bridge.is_alive() and time.monotonic() < end_time:
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        if bridge.is_alive():
            bridge.stop()

    snap = bridge.get_stats()
    print(
        f"\n  ┌─────────────────────────────────────────────────┐\n"
        f"  │  Modbus Bridge Self-Test Results                │\n"
        f"  ├─────────────────────────────────────────────────┤\n"
        f"  │  Polls OK       : {snap['polls_ok']:<6}                      │\n"
        f"  │  Polls Failed   : {snap['polls_failed']:<6}                      │\n"
        f"  │  NH₃ Errors     : {snap['nh3_errors']:<6}                      │\n"
        f"  │  T/H Errors     : {snap['th_errors']:<6}                      │\n"
        f"  │  Bounds Rejected: {snap['bounds_rejected']:<6}                      │\n"
        f"  │  MQTT Published : {snap['mqtt_publishes']:<6}                      │\n"
        f"  └─────────────────────────────────────────────────┘\n"
    )

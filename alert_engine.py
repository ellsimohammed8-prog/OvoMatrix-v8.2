"""
OvoMatrix v8.0 — Phase 1, Step 4
alert_engine.py: Safety-Critical Alert & Decision Engine

Role (system context):
  This module is the 'brain' of OvoMatrix.  It runs an independent
  monitoring loop that:

    1.  Fetches the latest telemetry reading from SQLite every second.
    2.  Classifies NH₃ concentration against Ross 308 thresholds.
    3.  Evaluates a per-level cooldown before every dispatch decision.
    4.  Logs every evaluation outcome to `alert_log` (including suppressed
        alerts) to maintain a complete, auditable event history.
    5.  Dispatches active alerts to the GSM layer (gsm_layer.py) — currently
        implemented as a terminal stub until Step 5 is built.

Ross 308 NH₃ thresholds (industry standard):
  < 20 ppm  →  NORMAL
  20–35 ppm →  WARNING   (mucous membrane irritation risk)
  35–50 ppm →  CRITICAL  (welfare & production impact)
  > 50 ppm  →  EMERGENCY (acute toxicity, immediate evacuation risk)

Cooldown windows (configurable):
  WARNING   → 30 minutes
  CRITICAL  →  5 minutes
  EMERGENCY →  2 minutes

Design guarantees:
  - Never raises an unhandled exception in the monitoring thread.
  - DB errors are individually caught, logged, and the loop continues.
  - Thread-safe shutdown via threading.Event.
  - QThread-compatible: expose EngineWorker.run() as the thread target.
"""

from __future__ import annotations

import logging
import signal
import sqlite3
import sys
import threading
import time
import os
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Callable, Optional

import paho.mqtt.client as mqtt
from db_manager import DatabaseManager

# ---------------------------------------------------------------------------
# GSM layer integration — import real dispatcher, fall back to stub if absent
# ---------------------------------------------------------------------------
try:
    from gsm_layer import gsm_dispatch as _gsm_real_dispatcher
    _DEFAULT_DISPATCHER = _gsm_real_dispatcher
    _DISPATCHER_SOURCE  = "gsm_layer.gsm_dispatch (REAL)"
except ImportError:
    _DEFAULT_DISPATCHER = None   # resolved after _gsm_dispatch_stub is defined
    _DISPATCHER_SOURCE  = "_gsm_dispatch_stub (FALLBACK)"

# ---------------------------------------------------------------------------
# Logging — coloured, consistent with OvoMatrix house style
# ---------------------------------------------------------------------------

class _ColourFormatter(logging.Formatter):
    _COLOURS: dict[int, str] = {
        logging.DEBUG:    "\033[90m",    # dark grey
        logging.INFO:     "\033[97m",    # bright white
        logging.WARNING:  "\033[93m",    # yellow
        logging.ERROR:    "\033[91m",    # red
        logging.CRITICAL: "\033[41;97m", # white on red background
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        colour = self._COLOURS.get(record.levelno, "")
        record.levelname = f"{colour}{record.levelname:<8}{self._RESET}"
        return super().format(record)


def _build_logger(name: str = "ovomatrix.alert_engine") -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(
            _ColourFormatter(
                fmt="%(asctime)s  %(levelname)s  %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        log.addHandler(h)
    return log


logger = _build_logger()

# ---------------------------------------------------------------------------
# Alert level enumeration
# ---------------------------------------------------------------------------

class AlertLevel(str, Enum):
    """
    NH₃ severity classification aligned to Ross 308 broiler welfare standards.

    Inherits from str so instances can be passed directly to DatabaseManager
    string parameters without explicit .value access.
    """
    NORMAL    = "NORMAL"
    WARNING   = "WARNING"
    CRITICAL  = "CRITICAL"
    EMERGENCY = "EMERGENCY"


# ---------------------------------------------------------------------------
# Threshold & cooldown configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ThresholdProfile:
    """
    Immutable definition of a single alert level's parameters.

    Attributes:
        level:           AlertLevel enum member.
        nh3_min:         Lower NH₃ bound (inclusive, ppm).  None = no lower bound.
        nh3_max:         Upper NH₃ bound (exclusive).       None = no upper bound.
        cooldown:        Minimum time between un-suppressed alerts.
        log_label:       ANSI-decorated label for terminal output.
        dispatch_action: Verb phrase used in the alert_log `action_taken` field.
    """
    level:           AlertLevel
    nh3_min:         Optional[float]
    nh3_max:         Optional[float]
    cooldown:        timedelta
    log_label:       str
    dispatch_action: str


# Ordered from most to least severe so the first match wins.
_THRESHOLDS: tuple[ThresholdProfile, ...] = (
    ThresholdProfile(
        level           = AlertLevel.EMERGENCY,
        nh3_min         = 50.0,
        nh3_max         = None,
        cooldown        = timedelta(minutes=2),
        log_label       = "\033[41;97m EMERGENCY \033[0m",
        dispatch_action = "EMERGENCY_DISPATCH: SMS + automated ventilation override triggered",
    ),
    ThresholdProfile(
        level           = AlertLevel.CRITICAL,
        nh3_min         = 35.0,
        nh3_max         = 50.0,
        cooldown        = timedelta(minutes=5),
        log_label       = "\033[91m CRITICAL  \033[0m",
        dispatch_action = "CRITICAL_DISPATCH: SMS sent to farm manager + ventilation increase command",
    ),
    ThresholdProfile(
        level           = AlertLevel.WARNING,
        nh3_min         = 20.0,
        nh3_max         = 35.0,
        cooldown        = timedelta(minutes=30),
        log_label       = "\033[93m WARNING   \033[0m",
        dispatch_action = "WARNING_DISPATCH: SMS notification sent to supervisor",
    ),
    ThresholdProfile(
        level           = AlertLevel.NORMAL,
        nh3_min         = None,
        nh3_max         = 20.0,
        cooldown        = timedelta(hours=24),   # effectively never re-fires
        log_label       = "\033[92m NORMAL    \033[0m",
        dispatch_action = "NORMAL: no action required",
    ),
)

# Fast lookup: AlertLevel → ThresholdProfile
_PROFILE_MAP: dict[AlertLevel, ThresholdProfile] = {
    p.level: p for p in _THRESHOLDS
}

# How often (in seconds) the monitoring loop polls the database.
POLL_INTERVAL_SECONDS: float = 1.0

# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def classify_nh3(nh3_ppm: float) -> ThresholdProfile:
    """
    Map an NH₃ reading to its Ross 308 severity profile.

    Traverses thresholds from most to least severe so that a value of
    51 ppm is labelled EMERGENCY, not CRITICAL/WARNING.

    Args:
        nh3_ppm: Ammonia concentration in parts-per-million.

    Returns:
        The matching ThresholdProfile.

    Raises:
        ValueError: If nh3_ppm is negative (physically impossible).
    """
    if nh3_ppm < 0:
        raise ValueError(f"NH₃ reading cannot be negative; got {nh3_ppm!r}")

    for profile in _THRESHOLDS:
        above_min = (profile.nh3_min is None) or (nh3_ppm >= profile.nh3_min)
        below_max = (profile.nh3_max is None) or (nh3_ppm < profile.nh3_max)
        if above_min and below_max:
            return profile

    # Fallback — should be unreachable with current threshold definitions.
    return _PROFILE_MAP[AlertLevel.NORMAL]


# ---------------------------------------------------------------------------
# GSM dispatch stub (replaced by gsm_layer.py in Step 5)
# ---------------------------------------------------------------------------

def _gsm_dispatch_stub(
    level: AlertLevel,
    nh3_ppm: float,
    temp_c: float,
    action_taken: str,
) -> None:
    """
    Temporary terminal-only dispatch stub.

    This function will be replaced by a real GSM/SMS call in Step 5
    (gsm_layer.py).  Its signature is intentionally identical to what
    the real dispatcher will expose so that the swap is a single-line change.

    Args:
        level:        Severity level of the alert.
        nh3_ppm:      NH₃ reading that triggered the alert.
        temp_c:       Temperature at time of alert.
        action_taken: Human-readable description of the intended action.
    """
    border = "═" * 62
    # Use different colours per severity for visual immediacy.
    _colour = {
        AlertLevel.EMERGENCY: "\033[41;97m",
        AlertLevel.CRITICAL:  "\033[91m",
        AlertLevel.WARNING:   "\033[93m",
        AlertLevel.NORMAL:    "\033[92m",
    }.get(level, "")
    _reset = "\033[0m"

    print(
        f"\n{_colour}{border}\n"
        f"  🚨 GSM DISPATCH [STUB]  |  Level: {level.value}\n"
        f"  NH₃ : {nh3_ppm:.2f} ppm\n"
        f"  Temp: {temp_c:.2f} °C\n"
        f"  Action: {action_taken}\n"
        f"  Time : {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        f"{border}{_reset}\n",
        flush=True,
    )
    logger.warning(
        "GSM stub dispatched | level=%s | NH₃=%.2f ppm | action=%s",
        level.value,
        nh3_ppm,
        action_taken,
    )


# ---------------------------------------------------------------------------
# Alert engine statistics
# ---------------------------------------------------------------------------

@dataclass
class EngineStats:
    """Thread-safe counters for the monitoring dashboard."""

    _lock:             threading.Lock = field(default_factory=threading.Lock, repr=False)
    evaluations:       int = 0    # total DB polls
    alerts_fired:      int = 0    # alerts dispatched to GSM layer
    alerts_suppressed: int = 0    # cooldown-blocked alerts
    db_errors:         int = 0    # any SQLite exception in the loop
    normal_readings:   int = 0    # readings classified as NORMAL
    warn_readings:     int = 0
    critical_readings: int = 0
    emergency_readings: int = 0

    def bump(self, **kwargs: int) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "evaluations":        self.evaluations,
                "alerts_fired":       self.alerts_fired,
                "alerts_suppressed":  self.alerts_suppressed,
                "db_errors":          self.db_errors,
                "normal_readings":    self.normal_readings,
                "warn_readings":      self.warn_readings,
                "critical_readings":  self.critical_readings,
                "emergency_readings": self.emergency_readings,
            }


# ---------------------------------------------------------------------------
# Core alert evaluation logic (pure functions, easily unit-tested)
# ---------------------------------------------------------------------------

@dataclass
class EvaluationResult:
    """
    The outcome of a single alert evaluation cycle.

    Attributes:
        profile:      Matched ThresholdProfile for this reading.
        suppress:     True  → cooldown active; alert is blocked.
                      False → cooldown cleared; alert is dispatched.
        last_alert_ts: Timestamp of the previous unsuppressed alert
                       (None if no prior alert exists for this level).
        cooldown_remaining: Seconds remaining in cooldown (0 if cleared).
    """
    profile:            ThresholdProfile
    suppress:           bool
    last_alert_ts:      Optional[datetime]
    cooldown_remaining: float


def evaluate_cooldown(
    profile: ThresholdProfile,
    last_alert_ts: Optional[datetime],
    now: datetime,
) -> EvaluationResult:
    """
    Determine whether a new alert should fire or be suppressed.

    Logic:
      - If no previous unsuppressed alert exists for this level → FIRE.
      - If the elapsed time since the last alert ≥ cooldown window → FIRE.
      - Otherwise → SUPPRESS.

    Args:
        profile:       ThresholdProfile for the current NH₃ level.
        last_alert_ts: UTC datetime of the last unsuppressed alert
                       at this level (from DB), or None.
        now:           Current UTC datetime (injected for testability).

    Returns:
        EvaluationResult describing the dispatch decision.
    """
    if last_alert_ts is None:
        return EvaluationResult(
            profile            = profile,
            suppress           = False,
            last_alert_ts      = None,
            cooldown_remaining = 0.0,
        )

    elapsed      = now - last_alert_ts
    window       = profile.cooldown
    remaining    = (window - elapsed).total_seconds()

    if remaining <= 0:
        return EvaluationResult(
            profile            = profile,
            suppress           = False,
            last_alert_ts      = last_alert_ts,
            cooldown_remaining = 0.0,
        )

    return EvaluationResult(
        profile            = profile,
        suppress           = True,
        last_alert_ts      = last_alert_ts,
        cooldown_remaining = remaining,
    )


# ---------------------------------------------------------------------------
# EngineWorker — the public worker class
# ---------------------------------------------------------------------------

class EngineWorker:
    """
    Safety-critical monitoring loop for OvoMatrix v8.0.

    Threading model
    ---------------
    * ``start()``  → spawns a daemon ``threading.Thread``.
    * ``stop()``   → sets ``_stop_event``; the loop exits within one poll cycle.
    * ``run()``    → the blocking method; compatible with PyQt6 QThread.

    Callback hooks (for future GUI integration)
    -------------------------------------------
    * ``on_alert_fired(level, nh3_ppm, temp_c, action)``
    * ``on_suppressed(level, remaining_seconds)``
    * ``on_normal_reading(nh3_ppm)``

    Example (standalone)
    --------------------
    ::
        db = DatabaseManager("ovomatrix.db")
        db.setup_database()
        engine = EngineWorker(db)
        engine.start()
        ...
        engine.stop()

    Example (PyQt6)
    ---------------
    ::
        self._engine_thread = QThread()
        self._engine = EngineWorker(db)
        self._engine_thread.started.connect(self._engine.run)
        self._engine.moveToThread(self._engine_thread)
        self._engine_thread.start()
    """

    def __init__(
        self,
        db:              DatabaseManager,
        poll_interval:   float = POLL_INTERVAL_SECONDS,
        gsm_dispatcher:  Optional[Callable[[AlertLevel, float, float, str], None]] = None,
        on_alert_fired:  Optional[Callable[[AlertLevel, float, float, str], None]] = None,
        on_suppressed:   Optional[Callable[[AlertLevel, float], None]] = None,
        on_normal_reading: Optional[Callable[[float], None]] = None,
    ) -> None:
        """
        Args:
            db:               Initialised DatabaseManager instance.
            poll_interval:    Seconds between consecutive DB polls.
            gsm_dispatcher:   Callable matching the GSM dispatch signature.
                              Defaults to the terminal stub; swap for
                              ``gsm_layer.send_alert`` once Step 5 is built.
            on_alert_fired:   GUI callback — (level, nh3, temp, action).
            on_suppressed:    GUI callback — (level, remaining_s).
            on_normal_reading: GUI callback — (nh3_ppm,).
        """
        self.db             = db
        self.poll_interval  = poll_interval
        # Resolve dispatcher: caller-supplied → gsm_layer → built-in stub
        if gsm_dispatcher is not None:
            self.gsm_dispatcher = gsm_dispatcher
        elif _DEFAULT_DISPATCHER is not None:
            self.gsm_dispatcher = _DEFAULT_DISPATCHER
        else:
            self.gsm_dispatcher = _gsm_dispatch_stub
        self.stats          = EngineStats()

        # Optional GUI callbacks.
        self.on_alert_fired    = on_alert_fired
        self.on_suppressed     = on_suppressed
        self.on_normal_reading = on_normal_reading

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Hysteresis Filter: Moving average buffer for NH3
        self._nh3_buffer: list[float] = []
        self._buffer_size = 5
        self._bypass_cooldown = False

        logger.info(
            "AlertEngine initialised | poll=%.1fs | dispatcher=%s | "
            "thresholds: NORMAL<20 | WARNING 20–35 | CRITICAL 35–50 | EMERGENCY>50 (ppm)",
            self.poll_interval,
            _DISPATCHER_SOURCE,
        )

    # ------------------------------------------------------------------
    # Public control API
    # ------------------------------------------------------------------

    def clear_hysteresis(self) -> None:
        """Instantly clear the NH3 hysteresis buffer and allow cooldown bypass."""
        self._nh3_buffer.clear()
        self._bypass_cooldown = True
        logger.info("AlertEngine: Hysteresis buffer cleared (instant reaction + cooldown bypass)")

    def start(self, daemon: bool = True) -> None:
        """Launch the monitoring loop in a background thread."""
        if self._thread and self._thread.is_alive():
            logger.warning("start() called but engine is already running.")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target = self.run,
            name   = "AlertEngine",
            daemon = daemon,
        )
        self._thread.start()
        logger.info("AlertEngine thread started (daemon=%s).", daemon)

    def stop(self, timeout: float = 6.0) -> None:
        """Request graceful shutdown and wait up to *timeout* seconds."""
        logger.info("AlertEngine stopping …")
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning(
                    "AlertEngine thread did not exit within %.1f s.", timeout
                )
            else:
                logger.info("AlertEngine thread stopped cleanly.")

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def get_stats(self) -> dict:
        return self.stats.snapshot()

    # ------------------------------------------------------------------
    # Core run loop (call directly or via QThread.started signal)
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Blocking monitoring loop.

        Each iteration:
          1. Poll ``get_last_reading()`` from the DB.
          2. Guard against duplicate reads (same UUID as last cycle).
          3. Classify the NH₃ value.
          4. Skip cooldown check & dispatch for NORMAL readings.
          5. For WARNING / CRITICAL / EMERGENCY:
               a. Query ``get_last_unsuppressed_alert_time(level)``
               b. Call ``evaluate_cooldown()``
               c. Insert an ``alert_log`` row (suppressed=0 or 1)
               d. If not suppressed: call GSM dispatcher + GUI callback
          6. Sleep for the remainder of the poll interval.

        Exceptions from any DB call are individually caught so the loop
        is resilient to transient SQLite lock contention.
        """
        logger.info("AlertEngine monitoring loop started.")
        
        # Setup MQTT client specifically for CLEAR_HYSTERESIS
        broker_host = os.environ.get("MQTT_BROKER_HOST", "localhost")
        broker_port = int(os.environ.get("MQTT_BROKER_PORT", 1883))
        
        try:
            self._mqtt_client = mqtt.Client(client_id=f"alertengine-control-{os.getpid()}")
            def _on_msg(client, userdata, msg):
                try:
                    payload = json.loads(msg.payload.decode())
                    if payload.get("command") == "CLEAR_HYSTERESIS":
                        self.clear_hysteresis()
                except Exception:
                    pass
            
            self._mqtt_client.on_message = _on_msg
            self._mqtt_client.connect(broker_host, broker_port, keepalive=60)
            self._mqtt_client.subscribe("farm/telemetry/control")
            self._mqtt_client.loop_start()
            logger.info(f"AlertEngine: Subscribed to farm/telemetry/control on {broker_host}:{broker_port}")
        except Exception as e:
            logger.error(f"AlertEngine: Failed to connect to MQTT broker for control topic: {e}")
            self._mqtt_client = None

        while not self._stop_event.is_set():
            cycle_start = time.monotonic()
            now_utc     = datetime.now(timezone.utc)

            # ── Step 1: Fetch latest reading ──────────────────────────────
            try:
                reading = self.db.get_last_reading()
            except sqlite3.Error as exc:
                self.stats.bump(db_errors=1, evaluations=1)
                logger.error(
                    "DB error fetching last reading: %s | "
                    "engine loop continues.", exc
                )
                self._sleep_remaining(cycle_start)
                continue

            if reading is None:
                logger.debug("No readings in DB yet — waiting …")
                self._sleep_remaining(cycle_start)
                continue

            # ── Step 2: Duplicate-read guard ──────────────────────────────
            current_uuid = reading.get("uuid")
            if current_uuid == self._last_evaluated_uuid:
                logger.debug(
                    "No new reading since last poll (uuid=%.8s…) — skipping.",
                    current_uuid or "??",
                )
                self._sleep_remaining(cycle_start)
                continue

            self._last_evaluated_uuid = current_uuid
            self.stats.bump(evaluations=1)

            # ── Step 3: Classify ──────────────────────────────────────────
            try:
                nh3_ppm   = float(reading["nh3_ppm"])
                temp_c    = float(reading["temp_c"])
                humid_pct = float(reading["humid_pct"])
            except (KeyError, TypeError, ValueError) as exc:
                logger.error(
                    "Malformed reading row — cannot extract numeric fields: %s",
                    exc,
                )
                self._sleep_remaining(cycle_start)
                continue

            # ── Hysteresis Filtering ──────────────────────────────────────
            self._nh3_buffer.append(nh3_ppm)
            if len(self._nh3_buffer) > self._buffer_size:
                self._nh3_buffer.pop(0)
            
            filtered_nh3 = sum(self._nh3_buffer) / len(self._nh3_buffer)

            try:
                profile = classify_nh3(filtered_nh3)
            except ValueError as exc:
                logger.error("Classification error: %s", exc)
                self._sleep_remaining(cycle_start)
                continue

            self._bump_level_counter(profile.level)

            logger.info(
                "📊 Reading evaluated | NH₃=%6.2f ppm (filtered=%6.2f) | Temp=%5.2f°C | "
                "Humid=%5.2f%% | Level=%s%s\033[0m",
                nh3_ppm,
                filtered_nh3,
                temp_c,
                humid_pct,
                profile.log_label,
                "",
            )

            # ── Step 4: NORMAL → no alert logic needed ────────────────────
            if profile.level == AlertLevel.NORMAL:
                if self.on_normal_reading:
                    self._safe_callback("on_normal_reading", self.on_normal_reading, nh3_ppm)
                self._sleep_remaining(cycle_start)
                continue

            # ── Step 5a: Query last unsuppressed alert for this level ──────
            try:
                last_alert_ts: Optional[datetime] = (
                    self.db.get_last_unsuppressed_alert_time(profile.level.value)
                )
            except sqlite3.Error as exc:
                self.stats.bump(db_errors=1)
                logger.error(
                    "DB error querying cooldown for level=%s: %s | "
                    "defaulting to fire (fail-safe).",
                    profile.level.value,
                    exc,
                )
                # Fail-safe: if we can't read the cooldown, dispatch anyway
                # to avoid silent failures in a safety-critical system.
                last_alert_ts = None

            # ── Step 5b: Evaluate cooldown ────────────────────────────────
            result = evaluate_cooldown(profile, last_alert_ts, now_utc)

            if self._bypass_cooldown:
                logger.info("AlertEngine: Bypassing cooldown due to manual scenario switch.")
                self._bypass_cooldown = False
                result = EvaluationResult(
                    profile=profile,
                    suppress=False,
                    last_alert_ts=last_alert_ts,
                    cooldown_remaining=0.0
                )

            if result.suppress:
                # ── Suppressed path ───────────────────────────────────────
                mins = result.cooldown_remaining / 60
                logger.debug(
                    "⏸  Alert SUPPRESSED | level=%s | cooldown remaining=%.1f min",
                    profile.level.value,
                    mins,
                )

                # Persist suppressed event for full audit trail.
                self._safe_insert_alert(
                    level        = profile.level.value,
                    nh3_ppm      = nh3_ppm,
                    temp_c       = temp_c,
                    action_taken = f"SUPPRESSED — cooldown {mins:.1f} min remaining",
                    suppressed   = 1,
                )
                self.stats.bump(alerts_suppressed=1)

                if self.on_suppressed:
                    self._safe_callback(
                        "on_suppressed",
                        self.on_suppressed,
                        profile.level,
                        result.cooldown_remaining,
                    )

            else:
                # ── Dispatch path ─────────────────────────────────────────
                action = profile.dispatch_action
                logger.warning(
                    "🚨 Alert FIRED | level=%s | NH₃=%.2f ppm | action=%s",
                    profile.level.value,
                    nh3_ppm,
                    action,
                )

                # Persist BEFORE dispatching (ensures audit trail even if
                # the GSM call raises an exception).
                self._safe_insert_alert(
                    level        = profile.level.value,
                    nh3_ppm      = nh3_ppm,
                    temp_c       = temp_c,
                    action_taken = action,
                    suppressed   = 0,
                )
                self.stats.bump(alerts_fired=1)

                # Call GSM dispatcher (stub in this phase).
                try:
                    self.gsm_dispatcher(profile.level, nh3_ppm, temp_c, action)
                except Exception as exc:      # noqa: BLE001
                    logger.error(
                        "GSM dispatcher raised an exception: %s | "
                        "alert was already logged to DB.", exc,
                        exc_info=True,
                    )

                if self.on_alert_fired:
                    self._safe_callback(
                        "on_alert_fired",
                        self.on_alert_fired,
                        profile.level,
                        nh3_ppm,
                        temp_c,
                        action,
                    )

            self._sleep_remaining(cycle_start)

        # ── Loop exited ───────────────────────────────────────────────────
        if hasattr(self, '_mqtt_client') and self._mqtt_client:
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
            
        snap = self.stats.snapshot()
        logger.info(
            "AlertEngine stopped. Final stats → "
            "evaluations=%d | fired=%d | suppressed=%d | "
            "db_errors=%d | NORMAL=%d | WARN=%d | CRIT=%d | EMRG=%d",
            snap["evaluations"],
            snap["alerts_fired"],
            snap["alerts_suppressed"],
            snap["db_errors"],
            snap["normal_readings"],
            snap["warn_readings"],
            snap["critical_readings"],
            snap["emergency_readings"],
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _sleep_remaining(self, cycle_start: float) -> None:
        """
        Sleep for whatever fraction of the poll interval remains.

        This keeps the loop cadence close to ``poll_interval`` seconds
        regardless of how long the DB queries and dispatch took.
        """
        elapsed    = time.monotonic() - cycle_start
        sleep_time = max(0.0, self.poll_interval - elapsed)
        self._stop_event.wait(timeout=sleep_time)

    def _safe_insert_alert(
        self,
        level:        str,
        nh3_ppm:      float,
        temp_c:       float,
        action_taken: str,
        suppressed:   int,
    ) -> None:
        """
        Persist an alert_log row, swallowing DB exceptions to keep the
        monitoring loop alive even under SQLite contention.
        """
        try:
            rowid = self.db.insert_alert(
                level        = level,
                nh3_ppm      = nh3_ppm,
                temp_c       = temp_c,
                action_taken = action_taken,
                suppressed   = suppressed,
            )
            logger.debug(
                "alert_log: rowid=%d | level=%s | suppressed=%d",
                rowid, level, suppressed,
            )
        except (sqlite3.Error, ValueError, TypeError) as exc:
            self.stats.bump(db_errors=1)
            logger.error(
                "Failed to persist alert_log row: %s | "
                "level=%s suppressed=%d", exc, level, suppressed,
                exc_info=True,
            )

    def _safe_callback(self, name: str, fn: Callable, *args) -> None:
        """
        Call a GUI/test callback, catching all exceptions to prevent
        a buggy callback from crashing the monitoring thread.
        """
        try:
            fn(*args)
        except Exception as exc:      # noqa: BLE001
            logger.debug("Callback '%s' raised an exception: %s", name, exc)

    def _bump_level_counter(self, level: AlertLevel) -> None:
        """Update the per-level statistics counter."""
        _map = {
            AlertLevel.NORMAL:    "normal_readings",
            AlertLevel.WARNING:   "warn_readings",
            AlertLevel.CRITICAL:  "critical_readings",
            AlertLevel.EMERGENCY: "emergency_readings",
        }
        if level in _map:
            self.stats.bump(**{_map[level]: 1})


# ---------------------------------------------------------------------------
# CLI entry-point (standalone mode)
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Run the alert engine as a standalone process.

    Monitors `ovomatrix.db` indefinitely; Ctrl-C or SIGTERM triggers
    a graceful shutdown with final statistics printed to the terminal.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="OvoMatrix v8.0 — Alert & Decision Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default="ovomatrix.db",
        help="Path to the SQLite database file.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=POLL_INTERVAL_SECONDS,
        help="Polling interval in seconds.",
    )
    args = parser.parse_args()

    db = DatabaseManager(args.db)
    db.setup_database()

    engine      = EngineWorker(db=db, poll_interval=args.interval)
    main_stop   = threading.Event()

    def _sig(signum: int, _frame) -> None:
        logger.info("Signal %d received — shutting down.", signum)
        engine.stop()
        main_stop.set()

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    engine.start(daemon=False)

    logger.info(
        "AlertEngine running standalone.\n"
        "  DB       : %s\n"
        "  Interval : %.1f s\n"
        "  Levels   : NORMAL<20 | WARNING 20–35 | CRITICAL 35–50 | EMERGENCY>50 ppm\n"
        "  Press Ctrl-C to stop.",
        args.db,
        args.interval,
    )

    main_stop.wait()
    logger.info("AlertEngine main thread exiting.")


if __name__ == "__main__":
    main()

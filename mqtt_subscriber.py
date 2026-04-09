"""
OvoMatrix v8.0 — Phase 1, Step 3
mqtt_subscriber.py: Telemetry Ingestion Layer

Responsibilities:
  - Subscribe to `farm/telemetry` on the local Mosquitto broker.
  - Perform strict schema & type validation on every incoming JSON message
    BEFORE touching the database (security-first ingestion principle).
  - Persist valid readings via DatabaseManager.insert_reading().
  - Run entirely inside a background worker (SubscriberWorker) that is
    *both* usable as a plain threading.Thread AND as a PyQt6 QRunnable/QThread
    target — so the GUI layer can attach without refactoring this file.
  - Auto-reconnect when the broker disappears or restarts.
  - Never crash on malformed input; always log and discard bad messages.

Dependencies:
    pip install paho-mqtt
    (db_manager.py must be in the same package / on sys.path)
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import sys
import threading
import time
import uuid as _uuid_module
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt

# Load .env before any os.environ access (graceful if python-dotenv absent)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", override=False)
except ImportError:
    pass   # python-dotenv not installed; fall back to raw environment

from db_manager import DatabaseManager

# ---------------------------------------------------------------------------
# Logging — coloured, human-readable, consistent with the rest of OvoMatrix
# ---------------------------------------------------------------------------

class _ColourFormatter(logging.Formatter):
    """ANSI-coloured log levels for terminal readability."""

    _COLOURS: dict[int, str] = {
        logging.DEBUG:    "\033[90m",   # dark grey
        logging.INFO:     "\033[96m",   # cyan  (distinguishable from simulator)
        logging.WARNING:  "\033[93m",   # yellow
        logging.ERROR:    "\033[91m",   # red
        logging.CRITICAL: "\033[95m",   # magenta
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        colour = self._COLOURS.get(record.levelno, "")
        record.levelname = f"{colour}{record.levelname:<8}{self._RESET}"
        return super().format(record)


def _build_logger(name: str = "ovomatrix.subscriber") -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            _ColourFormatter(
                fmt="%(asctime)s  %(levelname)s  %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        log.addHandler(handler)
    return log


logger = _build_logger()

# ---------------------------------------------------------------------------
# MQTT configuration constants
# ---------------------------------------------------------------------------

MQTT_BROKER_HOST:       str   = os.environ.get("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT:       int   = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC:             str   = os.environ.get("MQTT_TOPIC", "farm/telemetry")
MQTT_CLIENT_ID:         str   = "ovomatrix-subscriber"
MQTT_QOS:               int   = 1          # at-least-once delivery
MQTT_KEEPALIVE:         int   = 60         # seconds
MQTT_RECONNECT_DELAY:   float = 5.0        # seconds between reconnect attempts
MQTT_MAX_RECONNECT_DELAY: float = 120.0    # exponential back-off ceiling (s)

# ── SEC-04: MQTT Authentication credentials (read from .env, never hardcoded)
# Leave MQTT_USER blank in .env to connect anonymously (dev/test only).
MQTT_USER:     str = os.environ.get("MQTT_USER", "")
MQTT_PASSWORD: str = os.environ.get("MQTT_PASSWORD", "")

# Return-code values from Mosquitto that indicate a credentials rejection.
_AUTH_FAILURE_CODES: frozenset[int] = frozenset({4, 5})  # 4=bad creds, 5=not authorised

# ---------------------------------------------------------------------------
# Validation configuration
# ---------------------------------------------------------------------------

# Required top-level JSON fields.
_REQUIRED_FIELDS: frozenset[str] = frozenset(
    {"timestamp", "nh3_ppm", "temp_c", "humid_pct", "source", "uuid"}
)

# Numeric field boundaries (inclusive).  Values outside these ranges are
# physically impossible and indicate sensor corruption or a spoofing attempt.
_FLOAT_BOUNDS: dict[str, tuple[float, float]] = {
    "nh3_ppm":   (0.0,  500.0),   # ppm;  >500 is lethal & sensor-saturated
    "temp_c":    (-40.0, 85.0),   # °C;   hardware operating range
    "humid_pct": (0.0,  100.0),   # %RH;  physical maximum
}

# Maximum length for string fields (DoS / injection guard).
_STRING_MAX_LEN: dict[str, int] = {
    "source":    128,
    "uuid":      36,
    "timestamp": 40,
}

# ---------------------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Outcome of a single message validation pass."""
    valid:   bool
    payload: Optional[dict] = None   # sanitised dict when valid=True
    reason:  str            = ""     # human-readable rejection reason


# ---------------------------------------------------------------------------
# Strict message validator
# ---------------------------------------------------------------------------

class MessageValidator:
    """
    Stateless, pure-function validator for MQTT telemetry payloads.

    All checks are performed in a single pass to produce a single, structured
    ValidationResult — no exceptions bubble to the caller.
    """

    @staticmethod
    def validate(raw: bytes) -> ValidationResult:
        """
        Validate a raw MQTT payload.

        Pipeline:
          1. UTF-8 decode
          2. JSON parse
          3. Required-field presence check
          4. String field type & length checks
          5. Numeric field type, NaN/Inf, and bounds checks
          6. ISO-8601 timestamp parse
          7. UUID format check

        Args:
            raw: Raw bytes from the MQTT message.

        Returns:
            ValidationResult with ``valid=True`` and a sanitised ``payload``
            dict on success, or ``valid=False`` and a ``reason`` on failure.
        """
        # ── Step 1: UTF-8 decode ────────────────────────────────────────────
        try:
            text = raw.decode("utf-8")
        except (UnicodeDecodeError, AttributeError) as exc:
            return ValidationResult(False, reason=f"UTF-8 decode error: {exc}")

        # ── Step 2: JSON parse ──────────────────────────────────────────────
        try:
            data: Any = json.loads(text)
        except json.JSONDecodeError as exc:
            return ValidationResult(False, reason=f"JSON parse error: {exc}")

        if not isinstance(data, dict):
            return ValidationResult(
                False, reason=f"Top-level JSON must be an object; got {type(data).__name__!r}."
            )

        # ── Step 3: Required fields ─────────────────────────────────────────
        missing = _REQUIRED_FIELDS - data.keys()
        if missing:
            return ValidationResult(
                False, reason=f"Missing required fields: {sorted(missing)}"
            )

        sanitised: dict = {}

        # ── Step 4: String fields ───────────────────────────────────────────
        for field_name, max_len in _STRING_MAX_LEN.items():
            value = data[field_name]
            if not isinstance(value, str):
                return ValidationResult(
                    False,
                    reason=(
                        f"Field '{field_name}' must be a string; "
                        f"got {type(value).__name__!r}."
                    ),
                )
            stripped = value.strip()
            if not stripped:
                return ValidationResult(
                    False,
                    reason=f"Field '{field_name}' must not be empty.",
                )
            if len(stripped) > max_len:
                return ValidationResult(
                    False,
                    reason=(
                        f"Field '{field_name}' exceeds max length "
                        f"({len(stripped)} > {max_len})."
                    ),
                )
            sanitised[field_name] = stripped

        # ── Step 5: Numeric fields ──────────────────────────────────────────
        for field_name, (low, high) in _FLOAT_BOUNDS.items():
            raw_val = data[field_name]
            # Accept int or float; reject bool (bool is a subclass of int in Python)
            if isinstance(raw_val, bool) or not isinstance(raw_val, (int, float)):
                return ValidationResult(
                    False,
                    reason=(
                        f"Field '{field_name}' must be a numeric (float); "
                        f"got {type(raw_val).__name__!r}."
                    ),
                )
            fval = float(raw_val)
            if math.isnan(fval) or math.isinf(fval):
                return ValidationResult(
                    False,
                    reason=f"Field '{field_name}' must be finite; got {fval!r}.",
                )
            if not (low <= fval <= high):
                return ValidationResult(
                    False,
                    reason=(
                        f"Field '{field_name}' value {fval} is outside "
                        f"physical bounds [{low}, {high}]."
                    ),
                )
            sanitised[field_name] = round(fval, 4)

        # ── Step 6: Timestamp ───────────────────────────────────────────────
        ts_raw = sanitised["timestamp"]
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            # Re-serialise to canonical UTC form.
            sanitised["timestamp"] = (
                dt.astimezone(timezone.utc)
                  .strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
            )
        except ValueError as exc:
            return ValidationResult(
                False,
                reason=f"Field 'timestamp' is not a valid ISO-8601 value: {exc}",
            )

        # ── Step 7: UUID ────────────────────────────────────────────────────
        try:
            _uuid_module.UUID(sanitised["uuid"])
        except ValueError:
            return ValidationResult(
                False,
                reason=f"Field 'uuid' is not a valid RFC-4122 UUID: {sanitised['uuid']!r}",
            )

        return ValidationResult(valid=True, payload=sanitised)


# ---------------------------------------------------------------------------
# Subscriber statistics (thread-safe counters)
# ---------------------------------------------------------------------------

@dataclass
class SubscriberStats:
    """Running counters for operational monitoring and dashboards."""

    lock:              threading.Lock = field(default_factory=threading.Lock, repr=False)
    messages_received: int = 0
    messages_accepted: int = 0
    messages_rejected: int = 0
    db_inserts_ok:     int = 0
    db_duplicates:     int = 0
    db_errors:         int = 0
    reconnect_count:   int = 0

    def increment(self, **kwargs: int) -> None:
        """Thread-safe batch increment."""
        with self.lock:
            for attr, delta in kwargs.items():
                setattr(self, attr, getattr(self, attr) + delta)

    def snapshot(self) -> dict:
        """Return an immutable copy of all counters."""
        with self.lock:
            return {
                "messages_received": self.messages_received,
                "messages_accepted": self.messages_accepted,
                "messages_rejected": self.messages_rejected,
                "db_inserts_ok":     self.db_inserts_ok,
                "db_duplicates":     self.db_duplicates,
                "db_errors":         self.db_errors,
                "reconnect_count":   self.reconnect_count,
            }


# ---------------------------------------------------------------------------
# MQTT callbacks (module-level to keep SubscriberWorker lean)
# ---------------------------------------------------------------------------

def _on_connect(
    client: mqtt.Client,
    userdata: "SubscriberWorker",
    flags: dict,
    rc: int,
) -> None:
    """Callback fired when the MQTT broker acknowledges (or rejects) the connection."""
    if rc == 0:
        logger.info(
            "✅ MQTT connected to %s:%d — subscribing to '%s' (QoS=%d)",
            userdata.broker_host,
            userdata.broker_port,
            userdata.topic,
            MQTT_QOS,
        )
        client.subscribe(userdata.topic, qos=MQTT_QOS)
        userdata._connected.set()
    else:
        _RC_MESSAGES = {
            1: "unacceptable protocol version",
            2: "identifier rejected",
            3: "server unavailable",
            4: "bad username or password",
            5: "not authorised",
        }
        reason = _RC_MESSAGES.get(rc, f"unknown rc={rc}")

        # SEC-04: Authentication failures are CRITICAL — log prominently and
        # signal the worker to stop retrying (prevent credential lockout).
        if rc in _AUTH_FAILURE_CODES:
            logger.critical(
                "\033[41;97m [SECURITY] AUTHENTICATION_FAILED \033[0m "
                "MQTT broker at %s:%d rejected our credentials (rc=%d: %s). "
                "Check MQTT_USER / MQTT_PASSWORD in .env and the Mosquitto "
                "passwd file. Subscriber will NOT retry — fix credentials first.",
                userdata.broker_host, userdata.broker_port, rc, reason,
            )
            # Signal the worker to abort the reconnect loop immediately.
            userdata._auth_failed.set()
            userdata._stop_event.set()
        else:
            logger.error(
                "❌ MQTT connection refused: %s (rc=%d). "
                "Verify Mosquitto is running on %s:%d.",
                reason, rc, userdata.broker_host, userdata.broker_port,
            )


def _on_disconnect(
    client: mqtt.Client,
    userdata: "SubscriberWorker",
    rc: int,
) -> None:
    """Callback fired on disconnect; triggers the auto-reconnect loop."""
    userdata._connected.clear()
    if rc == 0:
        logger.info("MQTT disconnected cleanly.")
    else:
        userdata.stats.increment(reconnect_count=1)
        logger.warning(
            "⚠️  MQTT unexpected disconnect (rc=%d). "
            "Auto-reconnect will begin in %.1f s …",
            rc,
            MQTT_RECONNECT_DELAY,
        )


def _on_subscribe(
    client: mqtt.Client,
    userdata: "SubscriberWorker",
    mid: int,
    granted_qos: tuple,
) -> None:
    """Callback confirming broker accepted our subscription."""
    logger.info(
        "📡 Subscription confirmed for '%s' — granted QoS=%s (mid=%d)",
        userdata.topic,
        granted_qos,
        mid,
    )


def _on_message(
    client: mqtt.Client,
    userdata: "SubscriberWorker",
    message: mqtt.MQTTMessage,
) -> None:
    """
    Core ingestion callback — validation → database.

    This callback runs in paho's network thread (loop_start()).  It is
    deliberately kept short: validate, log, persist — nothing else.

    Security contract
    -----------------
    Any message that fails validation is LOGGED at WARNING level and
    DISCARDED.  The subscriber process continues running; malformed or
    malicious payloads never reach the database.
    """
    userdata.stats.increment(messages_received=1)

    logger.debug(
        "📨 Message received on '%s' (%d bytes)",
        message.topic,
        len(message.payload),
    )

    # ── Validation ──────────────────────────────────────────────────────────
    result = MessageValidator.validate(message.payload)
    if not result.valid:
        userdata.stats.increment(messages_rejected=1)
        logger.warning(
            "🔒 SECURITY WARNING — invalid message discarded | "
            "topic=%s | reason=%s | raw=%r",
            message.topic,
            result.reason,
            message.payload[:120],   # truncated for log safety
        )
        if userdata.on_validation_failure:
            userdata.on_validation_failure(result.reason, message.payload)
        return

    userdata.stats.increment(messages_accepted=1)

    payload = result.payload   # type: ignore[assignment]

    logger.info(
        "✔  Valid reading | NH₃=%6.2f ppm | Temp=%6.2f°C | "
        "Humid=%6.2f%% | src=%s | uuid=%.8s…",
        payload["nh3_ppm"],
        payload["temp_c"],
        payload["humid_pct"],
        payload["source"],
        payload["uuid"],
    )

    # ── Database persistence ─────────────────────────────────────────────────
    try:
        inserted = userdata.db.insert_reading(payload)
        if inserted:
            userdata.stats.increment(db_inserts_ok=1)
            logger.debug("💾 Row saved to DB | uuid=%.8s…", payload["uuid"])
        else:
            userdata.stats.increment(db_duplicates=1)
            logger.debug(
                "⏭  Duplicate UUID detected — row skipped | uuid=%.8s…",
                payload["uuid"],
            )
    except Exception as exc:                          # noqa: BLE001
        userdata.stats.increment(db_errors=1)
        logger.error(
            "🛑 DB error while persisting reading | uuid=%.8s… | error=%s",
            payload["uuid"],
            exc,
            exc_info=True,
        )

    # Notify optional listener (used by the GUI thread in later phases).
    if userdata.on_reading_saved:
        userdata.on_reading_saved(payload)


# ---------------------------------------------------------------------------
# SubscriberWorker — the main public class
# ---------------------------------------------------------------------------

class SubscriberWorker:
    """
    Background MQTT subscriber with automatic reconnect.

    Design
    ------
    * Runs inside a ``threading.Thread`` (daemon=True by default).
    * Can also be used as the ``target`` of a PyQt6 ``QThread.started``
      signal, or called directly from ``QRunnable.run()`` — no Qt imports
      are required here, so there is zero coupling to the GUI layer.
    * Exposes two optional callback hooks for the GUI:
        - ``on_reading_saved(payload: dict)``     — fired after each DB write.
        - ``on_validation_failure(reason, raw)``  — fired on bad messages.

    Thread-safety
    -------------
    All ``stats`` mutations are lock-protected.
    ``stop()`` sets ``_stop_event``; the reconnect loop checks it to exit.

    Usage (standalone)
    ------------------
    ::
        db  = DatabaseManager("ovomatrix.db")
        db.setup_database()
        sub = SubscriberWorker(db)
        sub.start()              # returns immediately
        ...
        sub.stop()               # graceful shutdown

    Usage (PyQt6)
    -------------
    ::
        self._thread = QThread()
        self._worker = SubscriberWorker(db)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._thread.start()
    """

    def __init__(
        self,
        db: DatabaseManager,
        broker_host: str = MQTT_BROKER_HOST,
        broker_port: int = MQTT_BROKER_PORT,
        topic:       str = MQTT_TOPIC,
        on_reading_saved:      Optional[Callable[[dict], None]] = None,
        on_validation_failure: Optional[Callable[[str, bytes], None]] = None,
    ) -> None:
        """
        Initialise the worker.

        Args:
            db:                    Fully initialised DatabaseManager instance.
            broker_host:           MQTT broker hostname or IP.
            broker_port:           MQTT broker TCP port.
            topic:                 Topic to subscribe to.
            on_reading_saved:      Optional callback; called after each
                                   successful DB insert. Receives the
                                   sanitised payload dict.
            on_validation_failure: Optional callback; called when a message
                                   is rejected. Receives (reason, raw_bytes).
        """
        self.db           = db
        self.broker_host  = broker_host
        self.broker_port  = broker_port
        self.topic        = topic
        self.stats        = SubscriberStats()

        # Callbacks (set by the GUI layer in later phases).
        self.on_reading_saved:      Optional[Callable[[dict], None]]       = on_reading_saved
        self.on_validation_failure: Optional[Callable[[str, bytes], None]] = on_validation_failure

        # Internal synchronisation primitives.
        self._stop_event  = threading.Event()
        self._connected   = threading.Event()
        # SEC-04: dedicated event set on auth failure to halt the reconnect loop.
        self._auth_failed = threading.Event()

        # Thread handle (set in start()).
        self._thread: Optional[threading.Thread] = None

        # Build and configure the paho client.
        self._client = mqtt.Client(
            client_id   = MQTT_CLIENT_ID,
            clean_session = True,
            userdata    = self,
        )

        # SEC-04: Inject credentials if provided (no-op when both are empty).
        if MQTT_USER:
            self._client.username_pw_set(MQTT_USER, MQTT_PASSWORD or None)
            logger.info(
                "MQTT auth configured — user='%s' (password: %s)",
                MQTT_USER,
                "set" if MQTT_PASSWORD else "not set (user only)",
            )
        else:
            logger.warning(
                "MQTT_USER is not set — connecting anonymously. "
                "Set MQTT_USER and MQTT_PASSWORD in .env for production."
            )

        self._client.on_connect    = _on_connect
        self._client.on_disconnect = _on_disconnect
        self._client.on_subscribe  = _on_subscribe
        self._client.on_message    = _on_message

        # paho v2 reconnect back-off settings.
        self._client.reconnect_delay_set(
            min_delay = int(MQTT_RECONNECT_DELAY),
            max_delay = int(MQTT_MAX_RECONNECT_DELAY),
        )

        logger.info(
            "SubscriberWorker initialised | broker=%s:%d | topic=%s | auth=%s",
            self.broker_host,
            self.broker_port,
            self.topic,
            f"user={MQTT_USER}" if MQTT_USER else "anonymous",
        )

    # ------------------------------------------------------------------
    # Public control API
    # ------------------------------------------------------------------

    def start(self, daemon: bool = True) -> None:
        """
        Launch the subscriber in a background thread.

        Args:
            daemon: If True, the thread is killed automatically when the
                    main program exits (recommended for CLI usage).
        """
        if self._thread and self._thread.is_alive():
            logger.warning("start() called but worker is already running.")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target = self.run,
            name   = "MQTTSubscriber",
            daemon = daemon,
        )
        self._thread.start()
        logger.info("Subscriber thread started (daemon=%s).", daemon)

    def stop(self, timeout: float = 8.0) -> None:
        """
        Request a graceful shutdown and wait up to *timeout* seconds.

        Safe to call from any thread, including the main thread and
        PyQt6's GUI thread.

        Args:
            timeout: Maximum seconds to wait for the thread to finish.
        """
        logger.info("Stopping subscriber worker …")
        self._stop_event.set()
        self._client.disconnect()
        self._client.loop_stop()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning(
                    "Subscriber thread did not exit within %.1f s.", timeout
                )
            else:
                logger.info("Subscriber thread stopped cleanly.")

    def is_running(self) -> bool:
        """Return True if the background thread is alive."""
        return bool(self._thread and self._thread.is_alive())

    def is_connected(self) -> bool:
        """Return True if currently connected to the MQTT broker."""
        return self._connected.is_set()

    def get_stats(self) -> dict:
        """Return a snapshot of the current statistics counters."""
        return self.stats.snapshot()

    # ------------------------------------------------------------------
    # Core run method (called by thread or QThread)
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Main subscriber loop.

        This method:
          1. Attempts the initial MQTT connection.
          2. Starts paho's background network loop (loop_start).
          3. Enters a reconnect-supervision loop that detects disconnection
             and re-establishes the connection with exponential back-off.
          4. Exits cleanly when ``_stop_event`` is set.

        **This method blocks the calling thread** until ``stop()`` is called.
        When used with PyQt6 QThread, connect ``QThread.started`` to this
        method on the worker object.
        """
        logger.info(
            "Subscriber run() started — connecting to %s:%d …",
            self.broker_host,
            self.broker_port,
        )

        reconnect_delay = MQTT_RECONNECT_DELAY

        while not self._stop_event.is_set():

            # ── Connect ───────────────────────────────────────────────────
            try:
                # Abort immediately if a previous auth failure was detected.
                if self._auth_failed.is_set():
                    logger.critical(
                        "[SECURITY] AUTHENTICATION_FAILED — aborting reconnect loop. "
                        "Fix MQTT_USER / MQTT_PASSWORD in .env and restart."
                    )
                    break

                self._client.connect(
                    self.broker_host,
                    self.broker_port,
                    keepalive=MQTT_KEEPALIVE,
                )
                self._client.loop_start()

                # Wait for on_connect to confirm the connection.
                connected = self._connected.wait(timeout=10.0)
                if not connected:
                    # Auth failure sets _stop_event, which exits the outer loop.
                    if self._auth_failed.is_set():
                        break
                    logger.warning(
                        "Connection timeout — broker did not respond within 10 s. "
                        "Retrying in %.1f s …",
                        reconnect_delay,
                    )
                    self._client.loop_stop()
                    self._client.disconnect()
                    self._wait_or_stop(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, MQTT_MAX_RECONNECT_DELAY)
                    continue

                # Reset back-off on successful connection.
                reconnect_delay = MQTT_RECONNECT_DELAY
                logger.info(
                    "Entering message-receive loop. "
                    "Stats: %s", self.stats.snapshot()
                )

            except OSError as exc:
                logger.error(
                    "Cannot reach MQTT broker at %s:%d — %s | "
                    "retrying in %.1f s …",
                    self.broker_host,
                    self.broker_port,
                    exc,
                    reconnect_delay,
                )
                self._wait_or_stop(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, MQTT_MAX_RECONNECT_DELAY)
                continue

            # ── Supervision loop (checks for disconnect or stop) ───────────
            while not self._stop_event.is_set():
                if not self._connected.is_set():
                    # Broker disappeared; break inner loop to reconnect.
                    logger.warning(
                        "Lost MQTT connection. Attempting reconnect in %.1f s …",
                        reconnect_delay,
                    )
                    self._client.loop_stop()
                    break
                self._stop_event.wait(timeout=1.0)   # 1-second heartbeat check

        # ── Cleanup ───────────────────────────────────────────────────────
        self._client.loop_stop()
        self._client.disconnect()

        snap = self.stats.snapshot()
        logger.info(
            "Subscriber stopped. Final stats → received=%d  accepted=%d  "
            "rejected=%d  db_ok=%d  db_dup=%d  db_err=%d  reconnects=%d",
            snap["messages_received"],
            snap["messages_accepted"],
            snap["messages_rejected"],
            snap["db_inserts_ok"],
            snap["db_duplicates"],
            snap["db_errors"],
            snap["reconnect_count"],
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wait_or_stop(self, delay: float) -> None:
        """
        Sleep for *delay* seconds but wake immediately if stop() is called.

        Args:
            delay: Maximum seconds to sleep.
        """
        self._stop_event.wait(timeout=delay)


# ---------------------------------------------------------------------------
# CLI entry-point (standalone mode)
# ---------------------------------------------------------------------------

def _handle_signal(signum: int, _frame, stop_event: threading.Event) -> None:
    logger.info("Signal %d received — initiating graceful shutdown.", signum)
    stop_event.set()


def main() -> None:
    """
    Run the subscriber as a standalone process.

    Connects to the local Mosquitto broker, listens indefinitely on
    `farm/telemetry`, and saves valid readings to `ovomatrix.db`.

    Ctrl-C (SIGINT) or SIGTERM triggers a graceful shutdown.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="OvoMatrix v8.0 — MQTT Telemetry Subscriber",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host",   default=MQTT_BROKER_HOST, help="MQTT broker hostname.")
    parser.add_argument("--port",   type=int, default=MQTT_BROKER_PORT, help="MQTT broker port.")
    parser.add_argument("--topic",  default=MQTT_TOPIC, help="MQTT topic to subscribe to.")
    parser.add_argument("--db",     default="ovomatrix.db", help="Path to SQLite database file.")
    args = parser.parse_args()

    # ── Database setup ──────────────────────────────────────────────────────
    db = DatabaseManager(args.db)
    db.setup_database()

    # ── Subscriber setup ────────────────────────────────────────────────────
    worker = SubscriberWorker(
        db          = db,
        broker_host = args.host,
        broker_port = args.port,
        topic       = args.topic,
    )

    # ── Signal handling ─────────────────────────────────────────────────────
    main_stop = threading.Event()

    def _sig(signum: int, frame) -> None:
        _handle_signal(signum, frame, main_stop)
        worker.stop()

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    # ── Launch (non-daemon so the process stays alive) ──────────────────────
    worker.start(daemon=False)

    logger.info(
        "OvoMatrix subscriber running. Press Ctrl-C to stop.\n"
        "  Broker : %s:%d\n"
        "  Topic  : %s\n"
        "  DB     : %s",
        args.host, args.port, args.topic, args.db,
    )

    # Block main thread; signal handler will call worker.stop() when ready.
    main_stop.wait()
    logger.info("Main thread exiting.")


if __name__ == "__main__":
    main()

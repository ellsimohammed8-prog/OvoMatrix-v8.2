"""
OvoMatrix v8.0 — Phase 1, Step 6  (FINAL)
main_backend.py: Central Backend Orchestrator

Role:
  This is the 'Maestro' — the single entry point that bootstraps, connects,
  and supervises every backend component, then tears them all down cleanly
  on shutdown.

Component graph:
  ┌──────────────────────────────────────────────────────────────┐
  │                     main_backend.py                          │
  │  ┌──────────────┐  ┌─────────────────┐  ┌────────────────┐  │
  │  │  Simulator   │  │ MQTT Subscriber │  │  Alert Engine  │  │
  │  │ (optional)   │  │  (ingestion)    │  │  (decision)    │  │
  │  └──────┬───────┘  └────────┬────────┘  └───────┬────────┘  │
  │         │  MQTT publish     │ insert_reading()   │           │
  │         └──────────────────►│                   │           │
  │                             └────────────────────►           │
  │                                  SQLite (ovomatrix.db)       │
  │                                         │                    │
  │                                         ▼                    │
  │                                   gsm_layer.py               │
  └──────────────────────────────────────────────────────────────┘

Shutdown contract (Ctrl-C / SIGTERM / SIGHUP):
  1. _shutdown_event is set.
  2. Alert engine: stop() → thread joins (≤ 6 s).
  3. MQTT subscriber: stop() → thread joins (≤ 8 s).
  4. Simulator thread: stop_event set → daemon thread exits with process.
  5. Watchdog thread exits via same event.
  6. Final stats printed; process exits cleanly.

Designed for weeks-long unattended operation (Raspberry Pi / Ubuntu Server).
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Unified logging — source-tagged, coloured, central handler
# ---------------------------------------------------------------------------

class _SourceTagFilter(logging.Filter):
    """
    Inject a right-aligned [SOURCE] tag into every log record.

    Each logger whose name starts with 'ovomatrix.' is matched to a tag.
    Records from unknown loggers fall back to '[SYS ]'.
    """

    _TAG_MAP: dict[str, str] = {
        "ovomatrix.simulator":   "\033[90m[SIM ] \033[0m",
        "ovomatrix.subscriber":  "\033[96m[MQTT] \033[0m",
        "ovomatrix.alert_engine":"\033[97m[ALRT] \033[0m",
        "ovomatrix.gsm_layer":   "\033[94m[GSM ] \033[0m",
        "ovomatrix.main":        "\033[95m[MAIN] \033[0m",
        "ovomatrix.ups_monitor": "\033[33m[UPS ] \033[0m",
        "ovomatrix.modbus_bridge":"\033[35m[MODB] \033[0m",
    }
    _FALLBACK = "\033[90m[SYS ] \033[0m"

    def filter(self, record: logging.LogRecord) -> bool:
        tag = self._FALLBACK
        for prefix, t in self._TAG_MAP.items():
            if record.name == prefix or record.name.startswith(prefix + "."):
                tag = t
                break
        record.source_tag = tag
        return True


# Module-level constants — eliminate Sonarqube literal-duplication warnings.
_ALIVE       = "✔ ALIVE"
_DOWN        = "✘ DOWN"
_LOGGER_NAME = "ovomatrix.main"


class _MaestroFormatter(logging.Formatter):
    """Comprehensive formatter: timestamp · level · source tag · message."""

    _LEVEL_COLOURS: dict[int, str] = {
        logging.DEBUG:    "\033[90m",
        logging.INFO:     "\033[92m",
        logging.WARNING:  "\033[93m",
        logging.ERROR:    "\033[91m",
        logging.CRITICAL: "\033[41;97m",
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        colour  = self._LEVEL_COLOURS.get(record.levelno, "")
        level   = f"{colour}{record.levelname:<8}{self._RESET}"
        tag     = getattr(record, "source_tag", "\033[90m[SYS ] \033[0m")
        ts      = self.formatTime(record, self.datefmt)
        message = record.getMessage()
        base    = f"{ts}  {level}  {tag}{message}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def setup_unified_logging(level: int = logging.INFO) -> logging.Logger:
    """
    Configure the root 'ovomatrix' logger with a single stream handler
    that carries source-tag, colour, and level information.

    All child loggers (ovomatrix.simulator, ovomatrix.subscriber, …) inherit
    this handler automatically; no per-module handler setup is required.

    Args:
        level: Minimum log level (e.g. logging.DEBUG / logging.INFO).

    Returns:
        The configured 'ovomatrix.main' logger for use in this module.
    """
    root = logging.getLogger("ovomatrix")
    root.setLevel(level)

    # Avoid duplicate handlers if setup_unified_logging is called twice.
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(
            _MaestroFormatter(
                fmt    = "%(asctime)s  %(levelname)s  %(source_tag)s%(message)s",
                datefmt= "%Y-%m-%dT%H:%M:%S",
            )
        )
        handler.addFilter(_SourceTagFilter())
        root.addHandler(handler)

    return logging.getLogger("ovomatrix.main")


# ---------------------------------------------------------------------------
# Configuration (overridable via CLI / environment)
# ---------------------------------------------------------------------------

DEFAULT_DB_PATH:       str   = "ovomatrix.db"
DEFAULT_BROKER_HOST:   str   = "localhost"
DEFAULT_BROKER_PORT:   int   = 1883
DEFAULT_MQTT_TOPIC:    str   = "farm/telemetry"
DEFAULT_RUN_SIMULATOR: bool  = True
DEFAULT_SIM_SCENARIO:  str   = "NORMAL"
DEFAULT_SIM_INTERVAL:  float = 1.0

# Watchdog: seconds between health-pulse log lines (0 = disabled).
WATCHDOG_PULSE_INTERVAL: int = 60

# MQTT broker connectivity probe parameters.
BROKER_PROBE_TIMEOUT:  float = 5.0
BROKER_PROBE_RETRIES:  int   = 3
BROKER_PROBE_DELAY:    float = 2.0

# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

def probe_mqtt_broker(host: str, port: int, timeout: float = BROKER_PROBE_TIMEOUT) -> bool:
    """
    Attempt a raw TCP connection to the MQTT broker to verify it is listening.

    This does NOT perform an MQTT handshake — it only confirms the port is
    reachable, which is sufficient to gate startup.

    Args:
        host:    Broker hostname or IP.
        port:    Broker port.
        timeout: Connection timeout in seconds.

    Returns:
        True if the TCP connection succeeded, False otherwise.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def health_check(
    db_path:     str,
    broker_host: str,
    broker_port: int,
    logger:      logging.Logger,
) -> bool:
    """
    Run all pre-flight checks before starting the backend components.

    Checks performed:
      1. Parent directory of the DB path is writable.
      2. MQTT broker is reachable (retried up to BROKER_PROBE_RETRIES times).

    Args:
        db_path:     Path to the SQLite database file.
        broker_host: MQTT broker hostname.
        broker_port: MQTT broker TCP port.
        logger:      Logger to write results to.

    Returns:
        True if ALL checks pass, False if any check fails.
    """
    all_ok = True

    # ── 1. Database directory write-access ──────────────────────────────────
    db = Path(db_path).resolve()
    db_dir = db.parent
    logger.info("Health check: DB path → %s", db)

    if not db_dir.exists():
        try:
            db_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Health check: Created DB directory %s ✔", db_dir)
        except OSError as exc:
            logger.critical(
                "Health check FAILED: Cannot create DB directory %s — %s", db_dir, exc
            )
            all_ok = False
    elif not os.access(db_dir, os.W_OK):
        logger.critical(
            "Health check FAILED: DB directory is not writable: %s", db_dir
        )
        all_ok = False
    else:
        logger.info("Health check: DB directory writable ✔")

    # ── 2. MQTT broker reachability ─────────────────────────────────────────
    logger.info(
        "Health check: Probing MQTT broker at %s:%d …", broker_host, broker_port
    )
    broker_ok = False
    for attempt in range(1, BROKER_PROBE_RETRIES + 1):
        if probe_mqtt_broker(broker_host, broker_port, BROKER_PROBE_TIMEOUT):
            broker_ok = True
            logger.info("Health check: MQTT broker reachable ✔")
            break
        logger.warning(
            "Health check: MQTT probe attempt %d/%d failed. "
            "Retrying in %.1f s …",
            attempt, BROKER_PROBE_RETRIES, BROKER_PROBE_DELAY,
        )
        time.sleep(BROKER_PROBE_DELAY)

    if not broker_ok:
        if broker_host in ("localhost", "127.0.0.1"):
            logger.warning(
                "MQTT broker unreachable at %s:%d. Attempting to start embedded dev-broker...",
                broker_host, broker_port
            )
            # This is handled in OvoMatrixBackend.start() to ensure thread lifecycle
            return True # Allow start() to continue and try launching its own
            
        logger.critical(
            "Health check FAILED: MQTT broker unreachable at %s:%d.\n"
            "  ▶  Ensure Mosquitto is running: mosquitto -v\n"
            "  ▶  Or install it:  winget install Mosquitto  (Windows)\n"
            "  ▶  Or:            sudo apt install mosquitto  (Linux)",
            broker_host, broker_port,
        )
        all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# Simulator thread wrapper
# ---------------------------------------------------------------------------

class _SimulatorThread(threading.Thread):
    """
    Wraps simulator.run_simulator() in a daemon thread.

    The simulator is intentionally run as a daemon so it is automatically
    killed if the main process exits (e.g., on uncaught exception), preventing
    orphaned MQTT publishers.
    """

    def __init__(
        self,
        broker_host:   str,
        broker_port:   int,
        interval:      float,
        scenario_name: str,
    ) -> None:
        super().__init__(name="SimulatorThread", daemon=True)
        self._broker_host   = broker_host
        self._broker_port   = broker_port
        self._interval      = interval
        self._scenario_name = scenario_name
        self._exc: Optional[BaseException] = None   # captures crash for logging

    def run(self) -> None:
        try:
            # Import here so an import error is captured, not raised at startup.
            from simulator import run_simulator, ScenarioMode
            logger_sim = logging.getLogger("ovomatrix.simulator")
            logger_sim.info(
                "Simulator thread starting | scenario=%s interval=%.1fs",
                self._scenario_name, self._interval,
            )
            run_simulator(
                broker_host      = self._broker_host,
                broker_port      = self._broker_port,
                publish_interval = self._interval,
                initial_scenario = ScenarioMode[self._scenario_name],
                initial_paused   = True   # User manually controls start/stop
            )
        except Exception as exc:    # noqa: BLE001
            self._exc = exc
            logging.getLogger("ovomatrix.main").error(
                "Simulator thread crashed: %s", exc, exc_info=True
            )

    @property
    def crashed(self) -> bool:
        return self._exc is not None


# ---------------------------------------------------------------------------
# Watchdog — periodic health pulse and component supervision
# ---------------------------------------------------------------------------

class Watchdog(threading.Thread):
    """
    Background supervisor that runs every WATCHDOG_PULSE_INTERVAL seconds.

    Responsibilities:
      - Log a system-health heartbeat (uptime, component liveness, stats).
      - Detect crashed components and flag them for restart (future concern).
      - Set the shutdown event if a critical component has died unexpectedly.
    """

    def __init__(
        self,
        shutdown_event: threading.Event,
        subscriber,
        engine,
        simulator_thread: Optional[_SimulatorThread],
        start_time:       datetime,
        pulse_interval:   int = WATCHDOG_PULSE_INTERVAL,
    ) -> None:
        super().__init__(name="Watchdog", daemon=True)
        self._shutdown       = shutdown_event
        self._subscriber     = subscriber
        self._engine         = engine
        self._sim_thread     = simulator_thread
        self._start_time     = start_time
        self._pulse_interval = pulse_interval
        self._logger         = logging.getLogger(_LOGGER_NAME)

    def run(self) -> None:
        self._logger.debug("Watchdog started (pulse=%ds).", self._pulse_interval)
        while not self._shutdown.wait(timeout=self._pulse_interval):
            self._pulse()

        self._logger.debug("Watchdog exiting.")

    def _pulse(self) -> None:
        """Emit a structured health pulse and check component liveness."""
        now     = datetime.now(timezone.utc)
        uptime  = now - self._start_time
        up_h    = int(uptime.total_seconds() // 3600)
        up_m    = int((uptime.total_seconds() % 3600) // 60)
        up_s    = int(uptime.total_seconds() % 60)

        sub_alive = self._subscriber.is_running()
        eng_alive = self._engine.is_running()
        sim_alive = self._sim_thread.is_alive() if self._sim_thread else None

        sub_stats = self._subscriber.get_stats()
        eng_stats = self._engine.get_stats()

        modb_alive   = self._modbus_bridge and self._modbus_bridge.is_alive()
        modb_status  = _ALIVE if modb_alive else "disabled/stopped"
        if sim_alive is None:
            sim_status = "disabled"
        else:
            sim_status = _ALIVE if sim_alive else _DOWN
        ups_alive    = self._ups_monitor and self._ups_monitor.is_alive()
        ups_status   = _ALIVE if ups_alive else "disabled/stopped"

        self._logger.info(
            "━━━ HEARTBEAT ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  Uptime       : %02dh %02dm %02ds\n"
            "  Subscriber   : %s | rcv=%d acc=%d rej=%d reconnects=%d\n"
            "  AlertEngine  : %s | evals=%d fired=%d suppressed=%d\n"
            "  Simulator    : %s\n"
            "  UPS Monitor  : %s\n"
            "  Modbus Bridge: %s\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            up_h, up_m, up_s,
            _ALIVE if sub_alive else _DOWN,
            sub_stats.get("messages_received", 0),
            sub_stats.get("messages_accepted", 0),
            sub_stats.get("messages_rejected", 0),
            sub_stats.get("reconnect_count",   0),
            _ALIVE if eng_alive else _DOWN,
            eng_stats.get("evaluations",       0),
            eng_stats.get("alerts_fired",      0),
            eng_stats.get("alerts_suppressed", 0),
            sim_status,
            ups_status,
            modb_status,
        )

        # Critical component failure → trigger graceful shutdown.
        if not sub_alive or not eng_alive:
            self._logger.critical(
                "Watchdog detected a critical component failure! "
                "Initiating emergency shutdown …"
            )
            self._shutdown.set()

        if self._sim_thread and self._sim_thread.crashed:
            self._logger.error(
                "Simulator thread has crashed. "
                "System continues in subscriber+engine mode."
            )


# ---------------------------------------------------------------------------
# OvoMatrixBackend — the Maestro class
# ---------------------------------------------------------------------------

class OvoMatrixBackend:
    """
    Central orchestrator for OvoMatrix v8.0 backend.

    Lifecycle:
      1. ``__init__``            : store configuration.
      2. ``start()``             : health check → DB setup → component launch.
      3. ``wait_for_shutdown()`` : block main thread; handles SIGINT/SIGTERM.
      4. ``shutdown()``          : ordered teardown of all components.

    Thread structure when running:
      Main         → holds signal handlers; calls wait_for_shutdown()
      WatchdogThread → periodic health pulse
      AlertEngine  → polling DB every 1 s
      MQTTSubscriber → paho network thread (internal) + supervision thread
      SimulatorThread → optional; runs run_simulator() (daemon)
    """

    def __init__(self, config: argparse.Namespace) -> None:
        self._cfg            = config
        self._shutdown_event = threading.Event()
        self._start_time     = datetime.now(timezone.utc)
        self._logger         = logging.getLogger(_LOGGER_NAME)

        self._db         = None
        self._subscriber = None
        self._engine     = None
        self._sim_thread:    Optional[_SimulatorThread]  = None
        self._ups_monitor:   Optional[threading.Thread]  = None
        self._modbus_bridge: Optional[threading.Thread]  = None
        self._broker_thread: Optional[threading.Thread]  = None
        self._cleanup_thread: Optional[threading.Thread] = None
        self._watchdog:      Optional[Watchdog]           = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> bool:
        """Execute all startup steps. Returns False if a fatal step fails."""
        self._print_banner()

        if not self._run_health_checks():
            return False

        self._maybe_start_embedded_broker()

        if not self._init_database():
            return False

        if not self._start_subscriber():
            return False

        if not self._start_engine():
            return False

        self._start_aux_components()
        self._start_cleanup_task()
        self._start_watchdog()
        self._setup_signals()

        self._logger.info("OvoMatrix backend fully operational ✔")
        return True

    def _run_health_checks(self) -> bool:
        self._logger.info("Running pre-flight health checks …")
        if not health_check(self._cfg.db, self._cfg.host, self._cfg.port, self._logger):
            self._logger.critical("Pre-flight checks FAILED — aborting startup.")
            return False
        return True

    def _maybe_start_embedded_broker(self) -> None:
        if self._cfg.host in ("localhost", "127.0.0.1"):
            if not probe_mqtt_broker(self._cfg.host, self._cfg.port, timeout=1.0):
                self._logger.info("Starting embedded OvoBroker on %s:%d …", self._cfg.host, self._cfg.port)
                try:
                    import asyncio
                    from run_dev_broker import main as broker_main
                    def _run():
                        try:
                            asyncio.run(broker_main(self._cfg.host, self._cfg.port))
                        except Exception as e:
                            logging.getLogger(_LOGGER_NAME).error("Embedded broker crashed: %s", e)
                    self._broker_thread = threading.Thread(target=_run, name="EmbeddedBroker", daemon=True)
                    self._broker_thread.start()
                    for _ in range(5):
                        time.sleep(1.0)
                        if probe_mqtt_broker(self._cfg.host, self._cfg.port, timeout=1.0):
                            self._logger.info("Embedded OvoBroker ready ✔")
                            break
                except Exception as e:
                    self._logger.error("Failed to start embedded broker: %s", e)

    def _init_database(self) -> bool:
        self._logger.info("Initialising database at '%s' …", self._cfg.db)
        try:
            from db_manager import DatabaseManager
            self._db = DatabaseManager(self._cfg.db)
            self._db.setup_database()
            return True
        except Exception as exc:
            self._logger.critical("Database initialisation failed: %s", exc, exc_info=True)
            return False

    def _start_subscriber(self) -> bool:
        self._logger.info("Starting MQTT subscriber …")
        try:
            from mqtt_subscriber import SubscriberWorker
            self._subscriber = SubscriberWorker(db=self._db, broker_host=self._cfg.host, broker_port=self._cfg.port, topic=self._cfg.topic)
            self._subscriber.start(daemon=True)
            time.sleep(2.0)
            return self._subscriber.is_running()
        except Exception as exc:
            self._logger.critical("Subscriber startup failed: %s", exc, exc_info=True)
            return False

    def _start_engine(self) -> bool:
        self._logger.info("Starting alert engine …")
        try:
            from alert_engine import EngineWorker
            self._engine = EngineWorker(db=self._db)
            self._engine.start(daemon=True)
            return True
        except Exception as exc:
            self._logger.critical("Alert engine startup failed: %s", exc, exc_info=True)
            return False

    def _start_aux_components(self) -> None:
        self._sim_thread = None # Obsolete, functionality migrated to modbus_bridge
        
        try:
            from ups_monitor import UPSMonitorWorker
            self._ups_monitor = UPSMonitorWorker(db=self._db, broker_host=self._cfg.host, broker_port=self._cfg.port, shutdown_event=self._shutdown_event)
            self._ups_monitor.start()
        except Exception: pass

        try:
            from modbus_bridge import ModbusBridgeWorker
            self._modbus_bridge = ModbusBridgeWorker(broker_host=self._cfg.host, broker_port=self._cfg.port)
            self._modbus_bridge.start()
        except Exception: pass

    def _start_watchdog(self) -> None:
        if WATCHDOG_PULSE_INTERVAL > 0:
            self._watchdog = Watchdog(self._shutdown_event, self._subscriber, self._engine, None, self._start_time, WATCHDOG_PULSE_INTERVAL)
            self._watchdog.start()

    def _start_cleanup_task(self) -> None:
        """Start a daily background task to prune old records."""
        def _run():
            # Initial cleanup on boot
            if self._db:
                self._db.cleanup_old_readings(days=30)
            
            # Then once every 24 hours
            while not self._shutdown_event.wait(timeout=86400):
                if self._db:
                    self._db.cleanup_old_readings(days=30)
        
        self._cleanup_thread = threading.Thread(target=_run, name="CleanupTask", daemon=True)
        self._cleanup_thread.start()
        self._logger.info("Database cleanup task scheduled (30-day retention).")

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        if hasattr(signal, "SIGHUP"):
            signal.signal(signal.SIGHUP, self._handle_signal)

    def wait_for_shutdown(self) -> None:
        """
        Block the main thread until a shutdown signal is received.

        This uses threading.Event.wait() with 5-second slices so that
        KeyboardInterrupt is handled even on Windows where signal delivery
        to threads can be delayed.
        """
        self._logger.info(
            "Backend running. Press Ctrl-C or send SIGTERM to stop.\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        try:
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(timeout=5.0)
        except KeyboardInterrupt:
            self._logger.info("KeyboardInterrupt received.")
            self._shutdown_event.set()

    def shutdown(self) -> None:
        """
        Ordered, graceful teardown of all components.

        Order (most to least dependent):
          1. Alert engine    (depends on DB)
          2. Subscriber      (depends on MQTT + DB)
          3. Watchdog        (daemon; exits on its own)
          Simulator thread is a daemon — garbage-collected with the process.
        """
        log = self._logger
        log.info(
            "\n━━━ GRACEFUL SHUTDOWN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )

        # ── Alert engine ───────────────────────────────────────────────────
        if self._engine and self._engine.is_running():
            log.info("[1/4] Stopping alert engine …")
            self._engine.stop(timeout=6.0)
            log.info("[1/4] Alert engine stopped ✔")
        else:
            log.info("[1/4] Alert engine already stopped.")

        # ── UPS monitor ────────────────────────────────────────────────────
        if self._ups_monitor and self._ups_monitor.is_alive():
            log.info("[2/5] Stopping UPS monitor …")
            self._ups_monitor.stop(timeout=5.0)
            log.info("[2/5] UPS monitor stopped ✔")
        else:
            log.info("[2/5] UPS monitor already stopped.")

        # ── Modbus bridge ───────────────────────────────────────────────────
        if self._modbus_bridge and self._modbus_bridge.is_alive():
            log.info("[3/5] Stopping Modbus bridge …")
            self._modbus_bridge.stop(timeout=5.0)
            log.info("[3/5] Modbus bridge stopped ✔")
        else:
            log.info("[3/5] Modbus bridge already stopped.")

        # ── MQTT subscriber ────────────────────────────────────────────────
        if self._subscriber and self._subscriber.is_running():
            log.info("[4/5] Stopping MQTT subscriber …")
            self._subscriber.stop(timeout=8.0)
            log.info("[4/5] MQTT subscriber stopped ✔")
        else:
            log.info("[4/5] MQTT subscriber already stopped.")

        # ── Final stats ────────────────────────────────────────────────────
        self._print_final_stats()

        log.info("[5/5] All components stopped. Goodbye.\n"
                 "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _handle_signal(self, signum: int, _frame) -> None:
        sig_names = {
            signal.SIGINT:  "SIGINT (Ctrl-C)",
            signal.SIGTERM: "SIGTERM",
        }
        if hasattr(signal, "SIGHUP"):
            sig_names[signal.SIGHUP] = "SIGHUP"
        name = sig_names.get(signum, f"signal {signum}")
        self._logger.info("Signal received: %s — initiating shutdown …", name)
        self._shutdown_event.set()

    def _print_banner(self) -> None:
        ts = self._start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        cfg = self._cfg
        banner = (
            "\n\033[94m"
            "╔══════════════════════════════════════════════════════════╗\n"
            "║          OvoMatrix v8.0  —  Backend System               ║\n"
            "║          Safety-Critical Poultry Telemetry               ║\n"
            "╠══════════════════════════════════════════════════════════╣\n"
           f"║  Started  : {ts:<44} ║\n"
           f"║  DB       : {cfg.db:<44} ║\n"
           f"║  Broker   : {cfg.host}:{cfg.port:<41} ║\n"
           f"║  Topic    : {cfg.topic:<44} ║\n"
            "╚══════════════════════════════════════════════════════════╝"
            "\033[0m\n"
        )
        print(banner, flush=True)

    def _print_final_stats(self) -> None:
        """Print a final summary of all component statistics."""
        log = self._logger
        uptime = datetime.now(timezone.utc) - self._start_time
        up_h   = int(uptime.total_seconds() // 3600)
        up_m   = int((uptime.total_seconds() % 3600) // 60)

        lines = [
            "\n  ┌─────────────────────────────────────────────────┐",
            f"  │  SESSION SUMMARY   Uptime: {up_h:02d}h {up_m:02d}m             │",
            "  ├─────────────────────────────────────────────────┤",
        ]

        if self._subscriber:
            s = self._subscriber.get_stats()
            lines += [
                f"  │  [MQTT] received={s['messages_received']:<6} "
                f"accepted={s['messages_accepted']:<6} "
                f"rejected={s['messages_rejected']:<4}  │",
                f"  │         db_ok={s['db_inserts_ok']:<6} "
                f"duplicates={s['db_duplicates']:<5} "
                f"db_err={s['db_errors']:<4}  │",
            ]

        if self._engine:
            e = self._engine.get_stats()
            lines += [
                f"  │  [ALRT] evals={e['evaluations']:<7} "
                f"fired={e['alerts_fired']:<6} "
                f"suppressed={e['alerts_suppressed']:<3}  │",
                f"  │         NORMAL={e['normal_readings']:<5} "
                f"WARN={e['warn_readings']:<4} "
                f"CRIT={e['critical_readings']:<4} "
                f"EMRG={e['emergency_readings']:<3}  │",
            ]

        lines.append("  └─────────────────────────────────────────────────┘")
        log.info("\n".join(lines))


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog        = "main_backend.py",
        description = "OvoMatrix v8.0 — Headless Backend Orchestrator",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
        epilog = (
            "Examples:\n"
            "  python main_backend.py                       "
            "# full stack with simulator\n"
            "  python main_backend.py --no-simulator        "
            "# subscriber + engine only (real hardware)\n"
            "  python main_backend.py --scenario EMERGENCY  "
            "# force emergency from start\n"
            "  python main_backend.py --log-level DEBUG     "
            "# verbose output\n"
        ),
    )

    # ── Connection ───────────────────────────────────────────────────────
    conn = parser.add_argument_group("MQTT Connection")
    conn.add_argument(
        "--host",  default=DEFAULT_BROKER_HOST,
        help="MQTT broker hostname or IP.",
    )
    conn.add_argument(
        "--port",  type=int, default=DEFAULT_BROKER_PORT,
        help="MQTT broker TCP port.",
    )
    conn.add_argument(
        "--topic", default=DEFAULT_MQTT_TOPIC,
        help="MQTT topic to publish/subscribe.",
    )

    # ── Database ─────────────────────────────────────────────────────────
    db_grp = parser.add_argument_group("Database")
    db_grp.add_argument(
        "--db",  default=DEFAULT_DB_PATH,
        help="Path to the SQLite database file.",
    )

    # ── Logging ──────────────────────────────────────────────────────────
    log_grp = parser.add_argument_group("Logging")
    log_grp.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Minimum log level for the unified console output.",
    )

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    """
    Orchestrate the full OvoMatrix v8.0 backend.

    Exit codes:
      0 — Clean shutdown (Ctrl-C / SIGTERM).
      1 — Startup failure (health check or component init failed).
    """
    parser = build_arg_parser()
    cfg    = parser.parse_args()

    # ── Unified logging must be configured FIRST, before any import of
    #    the component modules so their loggers inherit the root handler. ──
    log_level  = getattr(logging, cfg.log_level, logging.INFO)
    setup_unified_logging(log_level)

    # ── Create and start the backend ────────────────────────────────────────
    backend = OvoMatrixBackend(cfg)

    if not backend.start():
        return 1

    # ── Block until shutdown signal ──────────────────────────────────────────
    backend.wait_for_shutdown()

    # ── Ordered teardown ─────────────────────────────────────────────────────
    backend.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())

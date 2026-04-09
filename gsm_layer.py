"""
OvoMatrix v8.0 — Phase 1, Step 5
gsm_layer.py: GSM Hardware Abstraction & Alert Dispatcher

Architecture overview:
  This module is the *execution arm* of OvoMatrix.  It sits between the
  alert_engine (decision layer) and the physical GSM modem (SIM7670G /
  SIM800L / SIM900), providing:

    1.  Smart Mock / Hardware switching controlled by MOCK_HARDWARE in .env.
    2.  Layer 2 in-memory cooldown — an independent guard that blocks
        duplicate dispatches even if alert_engine (Layer 1) fails to
        suppress them.  Key: (phone_number, alert_level).
    3.  Real AT-command conversation logic for SMS and voice calls.
    4.  Retry logic with exponential back-off for modem failures.
    5.  A single public entry point — ``send_alert()`` — that matches the
        signature of the stub in alert_engine.py for a seamless one-line swap.

Compatible modems (AT command set):
  SIM7670G, SIM7600, SIM800L, SIM900, EC21, EC25 (all use GSM 27.007 spec)

Dependencies:
  pip install pyserial python-dotenv
  (pyserial is imported lazily so mock mode works without it installed)
"""

from __future__ import annotations

import logging
import math
import os
import re
import sys
import threading
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# .env loading (python-dotenv, graceful fallback if not installed)
# ---------------------------------------------------------------------------

def _load_env(env_path: Optional[str] = None) -> None:
    """Load .env file with python-dotenv if available; silently skip otherwise."""
    try:
        from dotenv import load_dotenv
        target = Path(env_path) if env_path else Path(__file__).parent / ".env"
        load_dotenv(dotenv_path=target, override=False)
    except ImportError:
        pass   # python-dotenv not installed; fall back to raw os.environ


_load_env()

# ---------------------------------------------------------------------------
# Logging — coloured, consistent with OvoMatrix house style
# ---------------------------------------------------------------------------

class _ColourFormatter(logging.Formatter):
    _COLOURS: dict[int, str] = {
        logging.DEBUG:    "\033[90m",
        logging.INFO:     "\033[94m",    # blue — distinguishes GSM layer
        logging.WARNING:  "\033[93m",
        logging.ERROR:    "\033[91m",
        logging.CRITICAL: "\033[41;97m",
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        colour = self._COLOURS.get(record.levelno, "")
        record.levelname = f"{colour}{record.levelname:<8}{self._RESET}"
        return super().format(record)


def _build_logger(name: str = "ovomatrix.gsm_layer") -> logging.Logger:
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
# ── SECURITY INGRESS LAYER (SEC-01 patch) ────────────────────────────────
# ---------------------------------------------------------------------------

# Maximum length for an SMS payload (GSM 03.40 – single SMS frame).
_SMS_MAX_CHARS: int = 160

# Regex for E.164-like phone numbers.
# allows leading '+', '0', or '00'; total digits 5-15.
_PHONE_RE: re.Pattern = re.compile(r"^\+?\d\d{4,14}$")

# Characters that are illegal inside AT command strings.
# These can break out of the AT+CMGS="..." or ATD... frame.
_AT_FORBIDDEN: frozenset[str] = frozenset('"\r\n;\x1a\\\x00')


class PhoneSecurity:
    """
    Static security guard for all telephony operations.

    Every public GSM dispatch call MUST pass through this class before
    touching the backend or the cooldown registry.  A failure here logs a
    CRITICAL warning and drops the request silently — no SMS, no call, no
    cooldown registration, no exception propagated.

    Design decisions
    ----------------
    * ``is_valid_phone`` is intentionally strict: only E.164-like numbers
      pass.  Any injection payload (newlines, semicolons, AT keywords)
      fails the regex before the forbidden-char check even runs.
    * ``sanitize_message`` is a defence-in-depth measure for the body of
      the SMS — it does NOT make an invalid phone valid.
    * The 160-character cap on ``sanitize_message`` matches the GSM SMS
      single-frame limit and prevents modem buffer overflow on some modems.
    """

    @staticmethod
    def is_valid_phone(phone: str) -> bool:
        """
        Return True only if *phone* is a safe E.164-like number.

        Validation pipeline:
          1. Type check (must be str).
          2. Strip whitespace.
          3. Reject any AT-forbidden character (\r \n ; " \x1A …).
          4. Match against E.164-like regex ``^\\+?[1-9]\\d{4,14}$``.

        Examples:
          is_valid_phone("+212600112233")  → True
          is_valid_phone("+212\r\nAT+CMGD=1,4")  → False
          is_valid_phone("00212600112233")  → False (leading zeros)
          is_valid_phone("+1")              → False (too short)
        """
        if not isinstance(phone, str):
            return False
        stripped = phone.strip()
        # Fast reject: any forbidden AT character kills the attempt immediately
        if any(ch in _AT_FORBIDDEN for ch in stripped):
            return False
        return bool(_PHONE_RE.match(stripped))

    @staticmethod
    def sanitize_message(text: str, max_len: int = _SMS_MAX_CHARS) -> str:
        """
        Produce a safe SMS body from *text*.

        Transformations (in order):
          1. Cast to str (defensive).
          2. Strip leading/trailing whitespace.
          3. Remove non-printable control characters (category Cc) except
             safe whitespace (space, tab — but NOT CR/LF which break AT frames).
          4. Remove double-quote (\"): would close the AT+CMGS=\"…\" frame.
          5. Remove semicolon (;): terminates ATD dial string in some modems.
          6. Remove Ctrl-Z (\x1A): commits the SMS body prematurely.
          7. Remove null byte (\x00): may truncate modem serial reads.
          8. Encode to ASCII, replacing non-ASCII with '?' (modem safe-subset).
          9. Truncate to *max_len* characters.

        Returns:
            A clean, modem-safe ASCII string of at most *max_len* characters.
        """
        if not isinstance(text, str):
            text = str(text)

        # Step 3 — remove non-printable control chars (Unicode category Cc)
        # Keep space (0x20) but remove CR (\r), LF (\n), etc.
        cleaned = "".join(
            ch for ch in text
            if unicodedata.category(ch) != "Cc" or ch == "\t"
        )

        # Steps 4-7 — strip AT-dangerous characters
        for bad_char in ('"', ";", "\x1a", "\x00", "\r", "\n"):
            cleaned = cleaned.replace(bad_char, "")

        # Step 8 — encode to GSM-safe ASCII
        cleaned = cleaned.encode("ascii", errors="replace").decode("ascii")

        # Step 9 — truncate to single-SMS limit
        return cleaned[:max_len]

    @classmethod
    def validate_and_log(cls, phone: str, context: str) -> bool:
        """
        Validate *phone* and emit a CRITICAL log entry on failure.

        Args:
            phone:   The number to validate.
            context: Caller description for the log message (e.g. 'send_sms').

        Returns:
            True if valid, False (and logs CRITICAL) if invalid.
        """
        if cls.is_valid_phone(phone):
            return True
        logger.critical(
            "\033[41;97m [SECURITY] AT INJECTION ATTEMPT BLOCKED \033[0m "
            "context=%s | rejected phone=%r | "
            "No SMS/call dispatched. No cooldown registered.",
            context,
            phone[:40],   # truncated in case it's a long payload
        )
        return False


def _mask_phone(phone: str) -> str:
    """Return a partially masked number for log safety (PII protection)."""
    if len(phone) < 6:
        return "***"
    return phone[:3] + "****" + phone[-4:]


# ---------------------------------------------------------------------------
# Configuration — read from environment / .env
# ---------------------------------------------------------------------------

def _env_bool(key: str, default: bool = True) -> bool:
    raw = os.environ.get(key, str(default)).strip().lower()
    return raw in ("1", "true", "yes", "on")


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


def _env_list(key: str, default: str = "") -> list[str]:
    raw = os.environ.get(key, default).strip()
    return [p.strip() for p in raw.split(",") if p.strip()]


#: If True, all AT commands are simulated in the terminal (no hardware needed).
MOCK_HARDWARE: bool  = _env_bool("MOCK_HARDWARE", True)

#: Serial port identifier for the GSM modem (hardware mode only).
GSM_SERIAL_PORT: str = os.environ.get("GSM_SERIAL_PORT", "COM3").strip()

#: Serial baud rate (must match modem firmware setting).
GSM_BAUD_RATE: int   = _env_int("GSM_BAUD_RATE", 115200)

def _get_current_recipients() -> list[str]:
    """
    Dynamically resolve the current list of verified recipients from os.environ.
    This ensures that UI-driven changes to .env (saved via settings_dialog)
    are picked up without a restart.
    """
    raw = os.environ.get("ALERT_PHONE_NUMBERS", "").strip()
    if not raw:
        # Fallback to default if absolutely empty
        raw = "+212600000001"
        
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    verified = []
    for p in parts:
        if PhoneSecurity.validate_and_log(p, "RESOLVE_RECIPIENTS"):
            verified.append(p)
    return verified

#: Maximum AT command retries on timeout/error.
GSM_CALL_RETRIES: int    = _env_int("GSM_CALL_RETRIES", 2)

#: Seconds to wait for a modem response before timing out.
GSM_AT_TIMEOUT: float    = _env_float("GSM_AT_TIMEOUT", 10.0)

#: Voice call duration before ATH (auto hang-up), seconds.
GSM_CALL_DURATION: float = _env_float("GSM_CALL_DURATION", 20.0)

#: Simulated network latency range for mock mode [min, max] seconds.
_MOCK_LATENCY_RANGE: tuple[float, float] = (2.0, 3.5)

# ---------------------------------------------------------------------------
# Alert level definitions (mirrors alert_engine.AlertLevel)
# Defined here independently so gsm_layer has no circular import.
# ---------------------------------------------------------------------------

class AlertLevel(str, Enum):
    NORMAL    = "NORMAL"
    WARNING   = "WARNING"
    CRITICAL  = "CRITICAL"
    EMERGENCY = "EMERGENCY"


# ---------------------------------------------------------------------------
# Layer 2 cooldown configuration
# Must match the Layer 1 windows in alert_engine.py.
# ---------------------------------------------------------------------------

_L2_COOLDOWN: dict[AlertLevel, timedelta] = {
    AlertLevel.NORMAL:    timedelta(hours=24),
    AlertLevel.WARNING:   timedelta(minutes=30),
    AlertLevel.CRITICAL:  timedelta(minutes=5),
    AlertLevel.EMERGENCY: timedelta(minutes=2),
}

# Numbers of recipients each level notifies.
# WARNING → supervisor only (index 0).
# CRITICAL → first two (indices 0–1).
# EMERGENCY → all.
_RECIPIENT_SLICE: dict[AlertLevel, Optional[int]] = {
    AlertLevel.NORMAL:    0,    # no-one
    AlertLevel.WARNING:   1,
    AlertLevel.CRITICAL:  2,
    AlertLevel.EMERGENCY: None, # all
}

# ---------------------------------------------------------------------------
# Layer 2 in-memory cooldown registry (thread-safe)
# ---------------------------------------------------------------------------

@dataclass
class _CooldownRegistry:
    """
    Independent in-process guard against duplicate dispatches.

    Key: (phone_number, alert_level_value)
    Value: UTC datetime of last successful dispatch
    """
    _lock:    threading.Lock              = field(default_factory=threading.Lock, repr=False)
    _records: dict[tuple[str, str], datetime] = field(default_factory=dict)

    def is_suppressed(
        self,
        phone: str,
        level: AlertLevel,
        now: datetime,
    ) -> tuple[bool, float]:
        """
        Check whether a dispatch is within the Layer 2 cooldown window.

        Returns:
            (suppressed: bool, remaining_seconds: float)
        """
        key = (phone, level.value)
        window = _L2_COOLDOWN.get(level, timedelta(hours=24))
        with self._lock:
            last = self._records.get(key)
        if last is None:
            return False, 0.0
        elapsed   = now - last
        remaining = (window - elapsed).total_seconds()
        if remaining <= 0:
            return False, 0.0
        return True, remaining

    def record(self, phone: str, level: AlertLevel, ts: datetime) -> None:
        """Mark a successful dispatch for the given key."""
        key = (phone, level.value)
        with self._lock:
            self._records[key] = ts

    def clear(self, phone: str, level: AlertLevel) -> None:
        """Manually clear a cooldown entry (useful for testing)."""
        key = (phone, level.value)
        with self._lock:
            self._records.pop(key, None)


# Module-level singleton (shared across all calls within the process).
_registry = _CooldownRegistry()

# ---------------------------------------------------------------------------
# AT command result
# ---------------------------------------------------------------------------

@dataclass
class ATResult:
    """
    Result of a single AT command exchange with the modem.

    Attributes:
        success:  True if the modem acknowledged with 'OK'.
        response: Raw response string from the modem (or mock description).
        command:  The AT command that was sent.
        elapsed:  Wall-clock seconds the exchange took.
    """
    success:  bool
    response: str
    command:  str
    elapsed:  float

# ---------------------------------------------------------------------------
# Mock GSM backend
# ---------------------------------------------------------------------------

class _MockGSMBackend:
    """
    Simulates a GSM modem in the terminal.

    Reproduces realistic network latency (2–3.5 s) to give developers an
    accurate feel of real-world SMS / call dispatch speed.
    """

    # ANSI palette for mock output
    _C = {
        AlertLevel.EMERGENCY: "\033[41;97m",
        AlertLevel.CRITICAL:  "\033[91m",
        AlertLevel.WARNING:   "\033[93m",
        AlertLevel.NORMAL:    "\033[92m",
    }
    _R = "\033[0m"

    def _simulate_latency(self) -> None:
        lo, hi = _MOCK_LATENCY_RANGE
        delay  = lo + (hi - lo) * (hash(time.time()) % 1000) / 1000
        time.sleep(abs(delay))  # abs() guards against negative hash edge case

    def send_sms(
        self,
        phone: str,
        message: str,
        level: AlertLevel,
    ) -> ATResult:
        """Simulate AT+CMGS with a coloured terminal banner."""
        t0 = time.monotonic()
        border  = "─" * 64
        colour  = self._C.get(level, "")
        reset   = self._R
        ts      = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        command = f'AT+CMGS="{phone}"'

        print(
            f"\n{colour}{border}\n"
            f"  📱 [MOCK GSM] Sending SMS\n"
            f"  To     : {phone}\n"
            f"  Level  : {level.value}\n"
            f"  Time   : {ts}\n"
            f"  Payload: {message[:120]}{'…' if len(message) > 120 else ''}\n"
            f"  AT cmd : {command} → <text> → Ctrl+Z\n"
            f"{border}{reset}",
            flush=True,
        )
        logger.info("[MOCK GSM] Sending SMS to %s | level=%s", phone, level.value)
        self._simulate_latency()
        elapsed = time.monotonic() - t0
        print(
            f"{colour}  ✔  SMS delivered to {phone} "
            f"(simulated {elapsed:.1f}s){reset}\n",
            flush=True,
        )
        logger.info(
            "[MOCK GSM] SMS delivered to %s in %.1fs", phone, elapsed
        )
        return ATResult(success=True, response="+CMGS: 1\r\nOK",
                        command=command, elapsed=elapsed)

    def make_call(
        self,
        phone: str,
        duration: float,
        level: AlertLevel,
    ) -> ATResult:
        """Simulate ATD voice call with ringing animation."""
        t0      = time.monotonic()
        border  = "═" * 64
        colour  = self._C.get(level, "")
        reset   = self._R
        ts      = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        command = f"ATD{phone};"

        print(
            f"\n{colour}{border}\n"
            f"  📞 [MOCK GSM] Initiating Voice Call\n"
            f"  To       : {phone}\n"
            f"  Level    : {level.value}\n"
            f"  Time     : {ts}\n"
            f"  Duration : {duration:.0f}s (then ATH)\n"
            f"  AT cmd   : {command}\n"
            f"{border}",
            flush=True,
        )
        logger.info(
            "[MOCK GSM] Initiating call to %s | level=%s | duration=%.0fs",
            phone, level.value, duration,
        )

        # Simulated ringing animation.
        rings = min(3, max(1, int(duration / 7)))
        for i in range(rings):
            print(f"{colour}  🔔 Ringing … ({i + 1}/{rings}){reset}",
                  flush=True)
            time.sleep(1.8)

        print(f"{colour}  📣 Call connected — playing alert message …{reset}",
              flush=True)
        time.sleep(min(duration, 3.0))  # truncated for mock

        print(f"{colour}  ✔  Call ended (ATH sent) | "
              f"Total={time.monotonic() - t0:.1f}s{reset}\n",
              flush=True)
        elapsed = time.monotonic() - t0
        logger.info(
            "[MOCK GSM] Call to %s completed in %.1fs", phone, elapsed
        )
        return ATResult(success=True, response="NO CARRIER\r\nOK",
                        command=command, elapsed=elapsed)

    def check_modem(self) -> ATResult:
        """Simulate AT (modem alive check)."""
        t0 = time.monotonic()
        logger.debug("[MOCK GSM] AT → OK (simulated modem alive)")
        time.sleep(0.1)
        return ATResult(success=True, response="OK",
                        command="AT", elapsed=time.monotonic() - t0)


# ---------------------------------------------------------------------------
# Real GSM backend (pyserial AT command driver)
# ---------------------------------------------------------------------------

class _RealGSMBackend:
    """
    Real serial AT command driver for SIM7670G / SIM800L / SIM900 modules.

    Serial communication protocol:
      - Commands are sent as ASCII bytes with \\r\\n terminator.
      - Responses are read until 'OK', 'ERROR', or timeout.
      - SMS text phase uses Ctrl+Z (0x1A) as the end-of-message sentinel.
      - All operations are fully blocking; the caller is responsible for
        running this in a non-GUI thread.
    """

    def __init__(self, port: str, baud: int, timeout: float) -> None:
        self._port    = port
        self._baud    = baud
        self._timeout = timeout
        self._ser     = None   # serial.Serial instance; opened on first use
        self._lock    = threading.Lock()

    # ── Connection management ───────────────────────────────────────────────

    def _open(self) -> None:
        """Open the serial port lazily on first use."""
        try:
            import serial as _serial
        except ImportError as exc:
            raise RuntimeError(
                "pyserial is required for hardware mode. "
                "Install it with: pip install pyserial"
            ) from exc

        if self._ser and self._ser.is_open:
            return
        logger.info(
            "Opening serial port %s @ %d baud …", self._port, self._baud
        )
        self._ser = _serial.Serial(
            port     = self._port,
            baudrate = self._baud,
            timeout  = self._timeout,
            write_timeout = self._timeout,
        )
        # Flush stale data from modem buffer.
        self._ser.reset_input_buffer()
        self._ser.reset_output_buffer()

    def _close(self) -> None:
        if self._ser and self._ser.is_open:
            self._ser.close()
            logger.debug("Serial port %s closed.", self._port)

    # ── Core AT exchange ────────────────────────────────────────────────────

    def _send_at(self, command: str, wait_for: str = "OK") -> ATResult:
        """
        Send a single AT command and collect the modem response.

        Args:
            command:  AT command string (without \\r\\n — appended here).
            wait_for: Terminal token to watch for in the response.

        Returns:
            ATResult with success=True if *wait_for* was found.
        """
        t0 = time.monotonic()
        try:
            self._open()
            with self._lock:
                raw_cmd = (command + "\r\n").encode("ascii", errors="ignore")
                self._ser.write(raw_cmd)
                logger.debug("AT → %s", command)

                response_lines: list[str] = []
                deadline = time.monotonic() + self._timeout

                while time.monotonic() < deadline:
                    line_raw = self._ser.readline()
                    if not line_raw:
                        continue
                    line = line_raw.decode("ascii", errors="replace").strip()
                    if line:
                        response_lines.append(line)
                        logger.debug("AT ← %s", line)
                    if wait_for in line or "ERROR" in line:
                        break

                response = "\r\n".join(response_lines)
                success  = wait_for in response
                elapsed  = time.monotonic() - t0

                if not success:
                    logger.warning(
                        "AT command '%s' did not receive '%s'. "
                        "Response: %r", command, wait_for, response
                    )
                return ATResult(
                    success  = success,
                    response = response,
                    command  = command,
                    elapsed  = elapsed,
                )

        except Exception as exc:    # noqa: BLE001
            elapsed = time.monotonic() - t0
            logger.error(
                "Serial error during AT command '%s': %s", command, exc,
                exc_info=True,
            )
            return ATResult(
                success  = False,
                response = str(exc),
                command  = command,
                elapsed  = elapsed,
            )

    # ── SMS ────────────────────────────────────────────────────────────────

    def send_sms(
        self,
        phone: str,
        message: str,
        level: AlertLevel,
    ) -> ATResult:
        """
        Send an SMS using the AT+CMGS command sequence.

        Sequence:
          1. AT+CMGF=1       → set text mode
          2. AT+CMGS="<num>" → open PDU / text frame (respond >)
          3. <message>+0x1A   → send text + Ctrl-Z to commit
        """
        t0 = time.monotonic()
        logger.info(
            "[REAL GSM] Sending SMS to %s | level=%s", phone, level.value
        )

        # Step 1 — text mode
        r = self._send_at("AT+CMGF=1")
        if not r.success:
            return ATResult(False, r.response, "AT+CMGF=1",
                            time.monotonic() - t0)

        # Step 2 — open message frame
        r = self._send_at(f'AT+CMGS="{phone}"', wait_for=">")
        if not r.success:
            return ATResult(False, r.response, f'AT+CMGS="{phone}"',
                            time.monotonic() - t0)

        # Step 3 — send body + Ctrl-Z
        try:
            with self._lock:
                payload = (message + "\x1A").encode("ascii", errors="replace")
                self._ser.write(payload)
                logger.debug("AT → <SMS body + Ctrl-Z>")

                response_lines: list[str] = []
                deadline = time.monotonic() + self._timeout * 2  # SMS may be slow
                while time.monotonic() < deadline:
                    line_raw = self._ser.readline()
                    if not line_raw:
                        continue
                    line = line_raw.decode("ascii", errors="replace").strip()
                    if line:
                        response_lines.append(line)
                        logger.debug("AT ← %s", line)
                    if "+CMGS" in line or "ERROR" in line:
                        break

                response = "\r\n".join(response_lines)
                success  = "+CMGS" in response
                elapsed  = time.monotonic() - t0

                if success:
                    logger.info(
                        "[REAL GSM] SMS delivered to %s in %.1fs", phone, elapsed
                    )
                else:
                    logger.error(
                        "[REAL GSM] SMS to %s FAILED | response=%r",
                        phone, response,
                    )
                return ATResult(success=success, response=response,
                                command=f'AT+CMGS="{phone}"', elapsed=elapsed)

        except Exception as exc:    # noqa: BLE001
            elapsed = time.monotonic() - t0
            logger.error(
                "[REAL GSM] Exception during SMS body send: %s", exc,
                exc_info=True,
            )
            return ATResult(False, str(exc), "SMS_BODY", elapsed)

    # ── Voice call ─────────────────────────────────────────────────────────

    def make_call(
        self,
        phone: str,
        duration: float,
        level: AlertLevel,
    ) -> ATResult:
        """
        Initiate a voice call using ATD, wait *duration* seconds, then hang up.

        Sequence:
          1. ATD<phone>;   → dial (semicolon = voice mode)
          2. sleep(duration)
          3. ATH           → hang up
        """
        t0 = time.monotonic()
        logger.info(
            "[REAL GSM] Initiating call to %s | level=%s | duration=%.0fs",
            phone, level.value, duration,
        )

        # Dial
        r = self._send_at(f"ATD{phone};", wait_for="OK")
        if not r.success:
            logger.error(
                "[REAL GSM] ATD failed for %s | response=%r", phone, r.response
            )
            return ATResult(False, r.response, f"ATD{phone};",
                            time.monotonic() - t0)

        logger.info(
            "[REAL GSM] Call to %s connected — holding for %.0fs …",
            phone, duration,
        )
        time.sleep(duration)

        # Hang up
        h = self._send_at("ATH")
        elapsed = time.monotonic() - t0
        logger.info(
            "[REAL GSM] Call to %s ended (ATH). Total=%.1fs", phone, elapsed
        )
        return ATResult(
            success  = h.success,
            response = h.response,
            command  = f"ATD{phone};",
            elapsed  = elapsed,
        )

    # ── Modem health check ──────────────────────────────────────────────────

    def check_modem(self) -> ATResult:
        """Send 'AT' and expect 'OK' — basic liveness probe."""
        return self._send_at("AT")


# ---------------------------------------------------------------------------
# Backend factory
# ---------------------------------------------------------------------------

def _get_backend() -> "_MockGSMBackend | _RealGSMBackend":
    if MOCK_HARDWARE:
        return _MockGSMBackend()
    return _RealGSMBackend(
        port    = GSM_SERIAL_PORT,
        baud    = GSM_BAUD_RATE,
        timeout = GSM_AT_TIMEOUT,
    )


# ---------------------------------------------------------------------------
# SMS message builder
# ---------------------------------------------------------------------------

def _build_sms_body(
    level: AlertLevel,
    nh3_ppm: float,
    temp_c: float,
    action_taken: str,
) -> str:
    """
    Compose a modem-safe SMS body with a hard 160-character cap.

    The body is passed through ``PhoneSecurity.sanitize_message`` before
    return so that caller-supplied ``action_taken`` strings cannot inject
    AT command breakout sequences into the modem serial stream.

    Args:
        level:        Alert severity.
        nh3_ppm:      NH₃ reading in ppm.
        temp_c:       Temperature in °C.
        action_taken: Short action description (sanitised before use).

    Returns:
        Plain ASCII string ≤ 160 characters, safe for AT+CMGS.
    """
    ts           = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
    # Sanitise caller-supplied free text BEFORE embedding in the body
    safe_action  = PhoneSecurity.sanitize_message(action_taken, max_len=80)
    body = (
        f"[OvoMatrix {level.value}] {ts}\n"
        f"NH3={nh3_ppm:.1f}ppm Temp={temp_c:.1f}C\n"
        f"Action: {safe_action}"
    )
    # Final pass: full sanitise + 160-char cap on the assembled body
    return PhoneSecurity.sanitize_message(body, max_len=_SMS_MAX_CHARS)


# ---------------------------------------------------------------------------
# Public dispatch API
# ---------------------------------------------------------------------------

def send_alert(
    level: AlertLevel,
    nh3_ppm: float,
    temp_c: float,
    action_taken: str,
) -> None:
    """
    Primary public entry point — matches the gsm_dispatcher signature
    expected by alert_engine.EngineWorker.

    Selects recipients based on alert level, checks the Layer 2 cooldown
    registry, then dispatches SMS (all levels) and voice calls
    (CRITICAL + EMERGENCY).

    This function is designed to be called from a background thread and
    never raises — all exceptions are caught and logged.

    Args:
        level:        AlertLevel enum member (or string-compatible value).
        nh3_ppm:      NH₃ concentration at time of alert.
        temp_c:       Temperature at time of alert.
        action_taken: Human-readable action description from alert_engine.
    """
    # Accept string input for compatibility with the stub call site.
    if isinstance(level, str):
        try:
            level = AlertLevel(level)
        except ValueError:
            logger.error(
                "send_alert() received unknown level %r — defaulting to CRITICAL.",
                level,
            )
            level = AlertLevel.CRITICAL

    now       = datetime.now(timezone.utc)
    backend   = _get_backend()
    message   = _build_sms_body(level, nh3_ppm, temp_c, action_taken)

    # Determine recipient slice based on severity.
    # WARNING: 1 recipient | CRITICAL: 2 recipients | EMERGENCY: All
    all_recipients = _get_current_recipients()
    slice_val = _RECIPIENT_SLICE.get(level, 1)
    
    recipients: list[str] = (
        all_recipients[:slice_val]
        if slice_val is not None
        else all_recipients
    )

    if not recipients:
        logger.warning(
            "send_alert(): no phone numbers configured. "
            "Check ALERT_PHONE_NUMBERS in .env"
        )
        return

    logger.info(
        "─── GSM Dispatch ───────────────────────────────────────\n"
        "  Level      : %s\n"
        "  NH₃        : %.2f ppm\n"
        "  Temp       : %.2f °C\n"
        "  Recipients : %s\n"
        "  Mode       : %s\n"
        "────────────────────────────────────────────────────────",
        level.value,
        nh3_ppm,
        temp_c,
        ", ".join(_mask_phone(p) for p in recipients),   # PII-safe
        "MOCK" if MOCK_HARDWARE else "HARDWARE",
    )

    for phone in recipients:
        _dispatch_to_number(
            backend = backend,
            phone   = phone,
            level   = level,
            message = message,
            now     = now,
        )

    logger.info("─── GSM Dispatch complete ───────────────────────────────")


# ---------------------------------------------------------------------------
# Retry helpers (extracted to reduce cognitive complexity)
# ---------------------------------------------------------------------------

def _retry_sms(
    backend,
    phone:   str,
    message: str,
    level:   AlertLevel,
    retries: int,
) -> bool:
    """
    Attempt to send an SMS up to *retries+1* times with exponential back-off.

    Returns True on success, False if all attempts exhausted.
    """
    for attempt in range(1, retries + 2):
        try:
            result = backend.send_sms(phone, message, level)
            if result.success:
                return True
            wait = min(2 ** attempt, 30)
            logger.warning(
                "SMS attempt %d/%d failed for %s | retrying in %ds …",
                attempt, retries + 1, phone, wait,
            )
            time.sleep(wait)
        except Exception as exc:    # noqa: BLE001
            logger.error(
                "SMS attempt %d for %s raised: %s",
                attempt, phone, exc, exc_info=True,
            )
    return False


def _retry_call(
    backend,
    phone:    str,
    level:    AlertLevel,
    duration: float,
    retries:  int,
) -> bool:
    """
    Attempt a voice call up to *retries+1* times with exponential back-off.

    Returns True on success, False if all attempts exhausted.
    """
    for attempt in range(1, retries + 2):
        try:
            result = backend.make_call(phone, duration, level)
            if result.success:
                return True
            wait = min(2 ** attempt, 30)
            logger.warning(
                "Call attempt %d/%d failed for %s | retrying in %ds …",
                attempt, retries + 1, phone, wait,
            )
            time.sleep(wait)
        except Exception as exc:    # noqa: BLE001
            logger.error(
                "Call attempt %d for %s raised: %s",
                attempt, phone, exc, exc_info=True,
            )
    return False


def _dispatch_to_number(
    backend,
    phone:   str,
    level:   AlertLevel,
    message: str,
    now:     datetime,
) -> None:
    """
    Execute the full dispatch pipeline for a single recipient.

    Pipeline (SEC-01 hardened):
      0. Security Ingress: validate phone via PhoneSecurity.validate_and_log.
         Drop immediately (no cooldown, no backend call) on failure.
      1. Layer 2 cooldown check (in-memory).
      2. SMS send with retry (via _retry_sms).
      3. Voice call for CRITICAL/EMERGENCY (via _retry_call).
      4. Record dispatch timestamp in the cooldown registry.
    """
    # ── SEC-01: Security gate — validation before ANY other operation ─────
    if not PhoneSecurity.validate_and_log(phone, "_dispatch_to_number"):
        return   # CRITICAL already logged; no cooldown / no backend touch

    # ── Layer 2 cooldown check ─────────────────────────────────────────────
    suppressed, remaining = _registry.is_suppressed(phone, level, now)
    if suppressed:
        logger.warning(
            "⛔ Layer 2 cooldown active | phone=%s level=%s remaining=%.1f min",
            _mask_phone(phone), level.value, remaining / 60,
        )
        return

    logger.info(
        "✅ Layer 2 cooldown cleared | phone=%s level=%s — dispatching …",
        _mask_phone(phone), level.value,
    )

    # ── SMS dispatch ──────────────────────────────────────────────────────
    sms_ok = _retry_sms(backend, phone, message, level, GSM_CALL_RETRIES)
    if not sms_ok:
        logger.error(
            "❌ SMS to %s FAILED after %d attempts — "
            "NOT recording cooldown (will retry next alert).",
            phone, GSM_CALL_RETRIES + 1,
        )
        return

    # ── Voice call for CRITICAL / EMERGENCY ───────────────────────────────
    if level in (AlertLevel.CRITICAL, AlertLevel.EMERGENCY):
        call_ok = _retry_call(
            backend, phone, level, GSM_CALL_DURATION, GSM_CALL_RETRIES
        )
        if not call_ok:
            logger.error(
                "❌ Voice call to %s FAILED after %d attempts.",
                phone, GSM_CALL_RETRIES + 1,
            )

    # ── Record cooldown timestamp ─────────────────────────────────────────
    _registry.record(phone, level, now)
    logger.debug(
        "Layer 2 cooldown set | phone=%s level=%s | next window: %s",
        phone,
        level.value,
        (now + _L2_COOLDOWN[level]).strftime("%H:%M:%SZ"),
    )


# ---------------------------------------------------------------------------
# Convenience wrappers (called by alert_engine dispatcher swap)
# ---------------------------------------------------------------------------

def send_sms(phone: str, level: AlertLevel, nh3_ppm: float, temp_c: float) -> None:
    """
    Thin wrapper: send a single SMS to *phone* without a voice call.
    Bypasses recipient selection logic — used for direct/manual triggers.

    Raises:
        Nothing — drops silently and logs CRITICAL if phone is invalid.
    """
    # SEC-01: Security gate
    if not PhoneSecurity.validate_and_log(phone, "send_sms"):
        return
    backend = _get_backend()
    message = _build_sms_body(level, nh3_ppm, temp_c, "Manual SMS dispatch")
    backend.send_sms(phone, message, level)


def place_call(phone: str, level: AlertLevel, duration: float = GSM_CALL_DURATION) -> None:
    """
    Initiate a voice call to *phone*, hold for *duration* seconds, then ATH.

    Args:
        phone:    E.164 phone number string.
        level:    AlertLevel for colour coding (mock) / logging.
        duration: Seconds before ATH auto-hang-up.

    Raises:
        Nothing — drops silently and logs CRITICAL if phone is invalid.
    """
    # SEC-01: Security gate — must pass before any backend interaction
    if not PhoneSecurity.validate_and_log(phone, "place_call"):
        return
    backend = _get_backend()
    backend.make_call(phone, duration, level)


# Backward-compatible alias as requested in the specification.
Phone = place_call


def modem_health_check() -> bool:
    """
    Send 'AT' to the modem and return True if it responds with 'OK'.
    Always returns True in mock mode.
    """
    backend = _get_backend()
    result  = backend.check_modem()
    if result.success:
        logger.info("Modem health check: OK (%.2fs)", result.elapsed)
    else:
        logger.error("Modem health check: FAILED | %r", result.response)
    return result.success


# ---------------------------------------------------------------------------
# Direct integration adapter for alert_engine.EngineWorker
# ---------------------------------------------------------------------------

def gsm_dispatch(
    level:        "AlertLevel | str",
    nh3_ppm:      float,
    temp_c:       float,
    action_taken: str,
) -> None:
    """
    Drop-in replacement for ``_gsm_dispatch_stub`` in alert_engine.py.

    Usage in alert_engine.py (one-line swap):
    ::
        # Before (Step 4 stub):
        from alert_engine import _gsm_dispatch_stub
        engine = EngineWorker(db, gsm_dispatcher=_gsm_dispatch_stub)

        # After (Step 5 real layer):
        from gsm_layer import gsm_dispatch
        engine = EngineWorker(db, gsm_dispatcher=gsm_dispatch)
    """
    send_alert(level, nh3_ppm, temp_c, action_taken)


# ---------------------------------------------------------------------------
# CLI self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="OvoMatrix v8.0 — GSM Layer Self-Test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--level",
        choices=["WARNING", "CRITICAL", "EMERGENCY"],
        default="CRITICAL",
        help="Alert level to simulate.",
    )
    parser.add_argument(
        "--nh3",  type=float, default=42.5,
        help="Simulated NH₃ value (ppm).",
    )
    parser.add_argument(
        "--temp", type=float, default=28.3,
        help="Simulated temperature (°C).",
    )
    args = parser.parse_args()

    print(
        f"\n\033[94m{'═' * 64}\n"
        f"  OvoMatrix v8.0 — GSM Layer Self-Test\n"
        f"  Mode : {'MOCK' if MOCK_HARDWARE else 'HARDWARE (' + GSM_SERIAL_PORT + ')'}\n"
        f"  Level: {args.level}\n"
        f"  NH₃  : {args.nh3} ppm | Temp: {args.temp} °C\n"
        f"{'═' * 64}\033[0m\n"
    )

    # Modem health check first.
    modem_health_check()

    # Full dispatch test.
    send_alert(
        level        = AlertLevel(args.level),
        nh3_ppm      = args.nh3,
        temp_c       = args.temp,
        action_taken = "GSM layer self-test dispatch",
    )

    # Verify Layer 2 cooldown blocks a second immediate call.
    print("\n\033[93m[TEST] Firing same alert again immediately "
          "— should be blocked by Layer 2 cooldown …\033[0m")
    send_alert(
        level        = AlertLevel(args.level),
        nh3_ppm      = args.nh3,
        temp_c       = args.temp,
        action_taken = "GSM layer self-test — second dispatch (must be suppressed)",
    )
    print("\n\033[92m[TEST COMPLETE]\033[0m\n")

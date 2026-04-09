"""
OvoMatrix v8.0 — Phase 1, Step 1
db_manager.py: Safety-Critical SQLite Database Manager

Architecture:
  - WAL journal mode for concurrent read performance.
  - Parameterized queries throughout; no string interpolation on user data.
  - Thread-safe via threading.Lock + per-call connection (check_same_thread=False).
  - Strict input validation before any database operation.
  - Graceful busy-timeout handling with logged SQLite errors.
"""

import sqlite3
import logging
import threading
import uuid as _uuid_module
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DEFAULT_DB_PATH: str = "ovomatrix.db"

_VALID_ALERT_LEVELS: frozenset = frozenset({"NORMAL", "WARNING", "CRITICAL", "EMERGENCY"})

# SQLite PRAGMAs applied on every new connection.
_PRAGMAS: tuple = (
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA cache_size=-64000;",
    "PRAGMA busy_timeout=5000;",
)

# DDL — readings table
_DDL_READINGS: str = """
CREATE TABLE IF NOT EXISTS readings (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT    NOT NULL,
    nh3_ppm   REAL    NOT NULL,
    temp_c    REAL    NOT NULL,
    humid_pct REAL    NOT NULL,
    source    TEXT    NOT NULL,
    uuid      TEXT    NOT NULL UNIQUE
);
"""

# DDL — alert_log table
_DDL_ALERT_LOG: str = """
CREATE TABLE IF NOT EXISTS alert_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp    TEXT    NOT NULL,
    level        TEXT    NOT NULL,
    nh3_ppm      REAL    NOT NULL,
    temp_c       REAL    NOT NULL,
    action_taken TEXT    NOT NULL,
    suppressed   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ai_log (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT    NOT NULL,
    advice    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS ups_status (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    timestamp    TEXT    NOT NULL,
    status       TEXT    NOT NULL,
    battery_pct  REAL    NOT NULL,
    runtime_left INTEGER NOT NULL,
    load_pct     REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_readings_timestamp ON readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp   ON alert_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_ai_timestamp       ON ai_log(timestamp);
"""

# ---------------------------------------------------------------------------
# Helper: input validation
# ---------------------------------------------------------------------------

def _validate_float(value: object, name: str) -> float:
    """Cast *value* to float, raising ValueError with a descriptive message."""
    import math
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Field '{name}' must be a numeric value; got {type(value).__name__!r}: {value!r}"
        ) from exc
    if math.isnan(result):  # NaN guard (cleaner than result != result idiom)
        raise ValueError(f"Field '{name}' must not be NaN.")
    return result


def _validate_string(value: object, name: str, max_length: int = 512) -> str:
    """Validate that *value* is a non-empty string within *max_length*."""
    if not isinstance(value, str):
        raise TypeError(
            f"Field '{name}' must be a string; got {type(value).__name__!r}."
        )
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"Field '{name}' must not be empty or whitespace.")
    if len(stripped) > max_length:
        raise ValueError(
            f"Field '{name}' exceeds maximum length of {max_length} characters."
        )
    return stripped


def _validate_iso8601(value: object, name: str = "timestamp") -> str:
    """
    Accept a datetime object or an ISO-8601 string.
    Returns a canonical UTC ISO-8601 string (with 'Z' suffix).
    """
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError(f"Field '{name}' timestamp string must not be empty.")
        # Support trailing 'Z' as UTC marker.
        try:
            dt = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                f"Field '{name}' is not a valid ISO-8601 timestamp: {stripped!r}"
            ) from exc
    else:
        raise TypeError(
            f"Field '{name}' must be a datetime object or ISO-8601 string; "
            f"got {type(value).__name__!r}."
        )
    # Store everything as UTC.
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _validate_uuid(value: object, name: str = "uuid") -> str:
    """Validate that *value* is a well-formed UUID string."""
    if not isinstance(value, str):
        raise TypeError(
            f"Field '{name}' must be a UUID string; got {type(value).__name__!r}."
        )
    stripped = value.strip()
    try:
        _uuid_module.UUID(stripped)  # Raises ValueError if malformed.
    except ValueError as exc:
        raise ValueError(
            f"Field '{name}' is not a valid UUID: {stripped!r}"
        ) from exc
    return stripped


def _validate_suppressed(value: object) -> int:
    """Ensure *suppressed* is a boolean-like integer (0 or 1)."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and value in (0, 1):
        return value
    raise ValueError(
        f"'suppressed' must be 0 or 1 (or a bool); got {value!r}."
    )


# ---------------------------------------------------------------------------
# DatabaseManager
# ---------------------------------------------------------------------------

class DatabaseManager:
    """
    Thread-safe SQLite wrapper for OvoMatrix telemetry data.

    Usage:
        db = DatabaseManager("/path/to/ovomatrix.db")
        db.setup_database()
        db.insert_reading({...})
    """

    def __init__(self, db_path: str = _DEFAULT_DB_PATH) -> None:
        """
        Initialise the manager.

        Args:
            db_path: Filesystem path to the SQLite database file.
                     The parent directory is created automatically if absent.
        """
        path = Path(db_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path: str = str(path)
        self._lock: threading.Lock = threading.Lock()
        logger.info("DatabaseManager initialised. DB path: %s", self._db_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        """
        Open a new SQLite connection with all required PRAGMAs applied.

        Returns:
            A configured sqlite3.Connection.

        Raises:
            sqlite3.Error: Propagated after logging if connection or PRAGMA
                           execution fails.
        """
        try:
            conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES,
            )
            conn.row_factory = sqlite3.Row
            for pragma in _PRAGMAS:
                conn.execute(pragma)
            logger.debug("SQLite connection opened; PRAGMAs applied.")
            return conn
        except sqlite3.Error as exc:
            logger.error(
                "Failed to open/configure SQLite connection to '%s': %s",
                self._db_path,
                exc,
                exc_info=True,
            )
            raise

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def setup_database(self) -> None:
        """
        Apply all PRAGMAs and create the `readings` and `alert_log` tables
        if they do not already exist.

        This method is idempotent and safe to call on every application start.

        Raises:
            sqlite3.Error: If table creation fails (logged before re-raise).
        """
        logger.info("Setting up database schema …")
        with self._lock:
            try:
                conn = self._connect()
                with conn:
                    conn.executescript(_DDL_READINGS)
                    conn.executescript(_DDL_ALERT_LOG)
                conn.close()
                logger.info("Database schema is ready.")
            except sqlite3.Error as exc:
                logger.error(
                    "setup_database() failed: %s", exc, exc_info=True
                )
                raise

    def insert_reading(self, data_dict: dict) -> bool:
        """
        Insert a telemetry reading into the `readings` table.

        Duplicate UUIDs are silently ignored (INSERT OR IGNORE semantics),
        making this operation safe to retry on network re-delivery.

        Args:
            data_dict: A mapping that MUST contain the following keys:
                - ``timestamp``  (datetime | ISO-8601 str) – measurement time
                - ``nh3_ppm``    (float-compatible)        – ammonia level
                - ``temp_c``     (float-compatible)        – temperature °C
                - ``humid_pct``  (float-compatible)        – relative humidity %
                - ``source``     (str, ≤512 chars)         – sensor/device ID
                - ``uuid``       (str)                     – RFC-4122 UUID

        Returns:
            ``True`` if the row was inserted, ``False`` if a duplicate UUID
            was detected and the row was skipped.

        Raises:
            KeyError:     If a required key is absent from *data_dict*.
            TypeError:    If a field has an incompatible type.
            ValueError:   If a field fails validation (range, format, etc.).
            sqlite3.Error: On unrecoverable database errors (logged + re-raised).
        """
        if not isinstance(data_dict, dict):
            raise TypeError(
                f"insert_reading() expects a dict; got {type(data_dict).__name__!r}."
            )

        # --- Validate all fields up-front (fail-fast, before acquiring lock) ---
        required_keys = ("timestamp", "nh3_ppm", "temp_c", "humid_pct", "source", "uuid")
        for key in required_keys:
            if key not in data_dict:
                raise KeyError(f"Missing required key in data_dict: '{key}'.")

        timestamp = _validate_iso8601(data_dict["timestamp"], "timestamp")
        nh3_ppm   = _validate_float(data_dict["nh3_ppm"],   "nh3_ppm")
        temp_c    = _validate_float(data_dict["temp_c"],    "temp_c")
        humid_pct = _validate_float(data_dict["humid_pct"], "humid_pct")
        source    = _validate_string(data_dict["source"],   "source")
        row_uuid  = _validate_uuid(data_dict["uuid"],       "uuid")

        sql = """
            INSERT OR IGNORE INTO readings
                (timestamp, nh3_ppm, temp_c, humid_pct, source, uuid)
            VALUES
                (?, ?, ?, ?, ?, ?)
        """
        params = (timestamp, nh3_ppm, temp_c, humid_pct, source, row_uuid)

        with self._lock:
            try:
                conn = self._connect()
                with conn:
                    cursor = conn.execute(sql, params)
                    inserted = cursor.rowcount == 1
                conn.close()
                if inserted:
                    logger.debug(
                        "insert_reading(): row inserted. UUID=%s source=%s",
                        row_uuid,
                        source,
                    )
                else:
                    logger.warning(
                        "insert_reading(): duplicate UUID detected — row skipped. UUID=%s",
                        row_uuid,
                    )
                return inserted
            except sqlite3.Error as exc:
                logger.error(
                    "insert_reading() DB error: %s | params=%r",
                    exc,
                    params,
                    exc_info=True,
                )
                raise

    def get_last_reading(self) -> Optional[dict]:
        """
        Fetch the most recent row from the `readings` table by primary key.

        Returns:
            A dict with keys ``{id, timestamp, nh3_ppm, temp_c, humid_pct,
            source, uuid}`` if a row exists, otherwise ``None``.

        Raises:
            sqlite3.Error: On database errors (logged + re-raised).
        """
        sql = """
            SELECT id, timestamp, nh3_ppm, temp_c, humid_pct, source, uuid
            FROM   readings
            ORDER  BY id DESC
            LIMIT  1
        """
        with self._lock:
            try:
                conn = self._connect()
                cursor = conn.execute(sql)
                row = cursor.fetchone()
                conn.close()
                if row is None:
                    logger.debug("get_last_reading(): table is empty.")
                    return None
                result = dict(row)
                logger.debug("get_last_reading(): id=%s uuid=%s", result["id"], result["uuid"])
                return result
            except sqlite3.Error as exc:
                logger.error(
                    "get_last_reading() DB error: %s", exc, exc_info=True
                )
                raise

    def insert_alert(
        self,
        level: str,
        nh3_ppm: float,
        temp_c: float,
        action_taken: str,
        suppressed: int = 0,
    ) -> int:
        """
        Append a new entry to the `alert_log` table.

        Args:
            level:        Alert severity — one of ``"INFO"``, ``"WARNING"``,
                          or ``"CRITICAL"`` (case-sensitive).
            nh3_ppm:      Ammonia concentration at time of alert.
            temp_c:       Temperature at time of alert.
            action_taken: Human-readable description of the action taken
                          (≤512 chars, must not be empty).
            suppressed:   ``0`` (active) or ``1`` (suppressed/de-duplicated).
                          Defaults to ``0``.

        Returns:
            The ``rowid`` (integer primary key) of the newly inserted row.

        Raises:
            TypeError:    If a field has an incompatible type.
            ValueError:   If a field fails validation.
            sqlite3.Error: On database errors (logged + re-raised).
        """
        # --- Validate ---
        level_v       = _validate_string(level, "level", max_length=32)
        if level_v not in _VALID_ALERT_LEVELS:
            raise ValueError(
                f"'level' must be one of {sorted(_VALID_ALERT_LEVELS)}; got {level_v!r}."
            )
        nh3_ppm_v     = _validate_float(nh3_ppm,      "nh3_ppm")
        temp_c_v      = _validate_float(temp_c,       "temp_c")
        action_v      = _validate_string(action_taken, "action_taken")
        suppressed_v  = _validate_suppressed(suppressed)
        timestamp_v   = _validate_iso8601(
            datetime.now(timezone.utc), "timestamp"
        )

        sql = """
            INSERT INTO alert_log
                (timestamp, level, nh3_ppm, temp_c, action_taken, suppressed)
            VALUES
                (?, ?, ?, ?, ?, ?)
        """
        params = (timestamp_v, level_v, nh3_ppm_v, temp_c_v, action_v, suppressed_v)

        with self._lock:
            try:
                conn = self._connect()
                with conn:
                    cursor = conn.execute(sql, params)
                    rowid = cursor.lastrowid
                conn.close()
                logger.info(
                    "insert_alert(): level=%s suppressed=%d rowid=%d",
                    level_v,
                    suppressed_v,
                    rowid,
                )
                return rowid
            except sqlite3.Error as exc:
                logger.error(
                    "insert_alert() DB error: %s | params=%r",
                    exc,
                    params,
                    exc_info=True,
                )
                raise

    def get_last_unsuppressed_alert_time(
        self, level: str
    ) -> Optional[datetime]:
        """
        Return the timestamp of the most recent *unsuppressed* alert for a
        given severity level.

        Args:
            level: Alert level to query — one of ``"INFO"``, ``"WARNING"``,
                   or ``"CRITICAL"`` (case-sensitive).

        Returns:
            A timezone-aware :class:`datetime` (UTC) if a matching row is
            found, otherwise ``None``.

        Raises:
            TypeError:    If *level* is not a string.
            ValueError:   If *level* is not a recognised alert level.
            sqlite3.Error: On database errors (logged + re-raised).
        """
        level_v = _validate_string(level, "level", max_length=32)
        if level_v not in _VALID_ALERT_LEVELS:
            raise ValueError(
                f"'level' must be one of {sorted(_VALID_ALERT_LEVELS)}; got {level_v!r}."
            )

        sql = """
            SELECT timestamp
            FROM   alert_log
            WHERE  level      = ?
              AND  suppressed  = 0
            ORDER  BY id DESC
            LIMIT  1
        """
        with self._lock:
            try:
                conn = self._connect()
                cursor = conn.execute(sql, (level_v,))
                row = cursor.fetchone()
                conn.close()

                if row is None:
                    logger.debug(
                        "get_last_unsuppressed_alert_time(): no unsuppressed "
                        "'%s' alert found.",
                        level_v,
                    )
                    return None

                raw_ts: str = row["timestamp"]
                # Canonical stored format ends with 'Z'; convert to UTC datetime.
                dt = datetime.fromisoformat(
                    raw_ts.replace("Z", "+00:00")
                )
                logger.debug(
                    "get_last_unsuppressed_alert_time(): level=%s ts=%s",
                    level_v,
                    dt.isoformat(),
                )
                return dt

            except sqlite3.Error as exc:
                logger.error(
                    "get_last_unsuppressed_alert_time() DB error: %s",
                    exc,
                    exc_info=True,
                )
                raise

    def clear_data(self) -> None:
        """
        Truncate ALL operational tables (readings, alerts, AI advice, UPS status).
        
        This is used for development resets or when the user wants to start
        with a fresh dashboard as if it were the first run.
        """
        logger.info("Clearing all data from database tables …")
        with self._lock:
            try:
                conn = self._connect()
                # Run the deletions within a transaction
                with conn:
                    conn.execute("DELETE FROM readings;")
                    conn.execute("DELETE FROM alert_log;")
                    conn.execute("DELETE FROM ai_log;")
                    conn.execute("DELETE FROM ups_status;")
                
                # VACUUM must be run OUTSIDE of a transaction block
                conn.execute("VACUUM;")
                conn.close()
                logger.info("Database tables cleared and vacuumed successfully.")
            except sqlite3.Error as exc:
                logger.error("clear_data() failed: %s", exc, exc_info=True)
                raise

    def insert_ai_advice(self, advice: str) -> bool:
        """Log expert AI advisor results to the permanent archive."""
        sql = "INSERT INTO ai_log (timestamp, advice) VALUES (?, ?);"
        ts = _validate_iso8601(datetime.now(timezone.utc))
        with self._lock:
            try:
                conn = self._connect()
                with conn:
                    conn.execute(sql, (ts, advice))
                conn.close()
                return True
            except sqlite3.Error as e:
                logger.error("Failed to insert AI advice: %s", e)
                return False

    def get_ai_history(self, limit: int = 10) -> list[dict]:
        """Fetch the most recent archived AI technical advice."""
        sql = "SELECT timestamp, advice FROM ai_log ORDER BY id DESC LIMIT ?;"
        with self._lock:
            try:
                conn = self._connect()
                cursor = conn.execute(sql, (limit,))
                rows = cursor.fetchall()
                conn.close()
                return [dict(r) for r in rows]
            except sqlite3.Error:
                return []

    def get_full_alert_history(self, limit: int = 100) -> list[dict]:
        """Fetch a larger window of incidents for auditing."""
        sql = "SELECT timestamp, level, nh3_ppm, action_taken FROM alert_log ORDER BY id DESC LIMIT ?;"
        with self._lock:
            try:
                conn = self._connect()
                cursor = conn.execute(sql, (limit,))
                rows = cursor.fetchall()
                conn.close()
                return [dict(r) for r in rows]
            except sqlite3.Error:
                return []

    def cleanup_old_readings(self, days: int = 30) -> int:
        """
        Delete telemetry readings older than *days*.
        
        Returns the number of deleted rows.
        """
        sql = "DELETE FROM readings WHERE timestamp < datetime('now', ?);"
        param = f"-{days} days"
        
        logger.info("Cleaning up readings older than %d days …", days)
        with self._lock:
            try:
                conn = self._connect()
                with conn:
                    cursor = conn.execute(sql, (param,))
                    deleted = cursor.rowcount
                # Periodic VACUUM to reclaim space if many rows deleted
                if deleted > 1000:
                    conn.execute("VACUUM;")
                conn.close()
                logger.info("Cleanup complete. Deleted %d rows.", deleted)
                return deleted
            except sqlite3.Error as exc:
                logger.error("cleanup_old_readings() failed: %s", exc, exc_info=True)
                return 0

    # ---------------------------------------------------------------------------
    # UPS / Power Status
    # ---------------------------------------------------------------------------

    def update_ups_status(
        self,
        status:       str,
        battery_pct:  float,
        runtime_left: int,
        load_pct:     float
    ) -> None:
        """Upsert the single-row UPS status record."""
        ts = datetime.now(timezone.utc).isoformat()
        query = """
            INSERT OR REPLACE INTO ups_status 
            (id, timestamp, status, battery_pct, runtime_left, load_pct)
            VALUES (1, ?, ?, ?, ?, ?)
        """
        try:
            self.execute_write(query, (ts, status, battery_pct, runtime_left, load_pct))
        except sqlite3.Error as exc:
            logger.error("Failed to update UPS status: %s", exc)

    def get_last_ups_status(self) -> Optional[dict]:
        """Fetch the current UPS status record."""
        query = "SELECT * FROM ups_status WHERE id = 1"
        try:
            row = self.execute_read(query, one=True)
            if not row:
                return None
            return {
                "timestamp":    row["timestamp"],
                "status":       row["status"],
                "battery_pct":  row["battery_pct"],
                "runtime_left": row["runtime_left"],
                "load_pct":     row["load_pct"]
            }
        except sqlite3.Error:
            return None

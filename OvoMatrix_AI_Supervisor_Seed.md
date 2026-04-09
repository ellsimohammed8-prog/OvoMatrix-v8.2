# OvoMatrix AI Supervisor Seed (Comprehensive Overview)

## 1. Project Essence
**OvoMatrix** is a high-reliability IoT ecosystem for smart poultry farm monitoring. It integrates real-time sensors (NH3, Temp, Humid), power infrastructure awareness (UPS), and fail-safe communications (MQTT + GSM).

## 2. Core Orchestration (`OvoMatrix_App.py`)
- **Unified Launcher:** Manages the lifecycle of multiple processes (Backend, UI, Broker).
- **Environment Management:** Cold-starts with automated `.env` generation if missing.
- **Resilience:** Monitors child processes and ensures clean database flushing on shutdown.

## 3. Hardware & Reliability Layer
- **Modbus Bridge (`modbus_bridge.py`):** 
    - Real-world interfacing via RS-485.
    - Features: 3-cycle retry logic, sanity bounds validation (rejecting noise), and sensor-heartbeat monitoring.
- **UPS Monitor (`ups_monitor.py`):** 
    - Monitors power stability via SimpleNUTClient (Python 3.13+ compatible).
    - Features: Sequential shutdown grace period to prevent SQLite corruption during battery loss.
- **GSM Layer (`gsm_layer.py`):** 
    - Secondary fail-safe via SMS/Voice.
    - Security: Strict AT-injection protection and Layer-2 in-memory cooldown.

## 4. Intelligent Logic (`alert_engine.py`)
- **Ross 308 Standards:** Native support for industry thresholds.
- **Signal Processing:** Uses a moving-average Hysteresis buffer to filter out environmental noise.
- **Hysteresis Bypass:** Supports immediate re-evaluation on manual scenario changes.

## 5. Persistence & UI
- **Database (`db_manager.py`):** SQLite persistence with WAL mode for high-concurrency read-write (Backend/UI separation).
- **Dashboard (`main_ui.py`):** High-performance PyQt6 dashboard with hardware-accelerated charting (`pyqtgraph`).

## 6. Development & Deployment
- **Simulation:** Full mock mode supported for every hardware component.
- **Distribution:** `build_app.py` for PyInstaller EXE generation.
- **AgroChallenge Submission:** Integrated roadmap and business documentation.

---
**Supervisor Directive:** 
OvoMatrix is designed to be autonomous and failsafe. Priority must always be given to data integrity and constant monitoring throughput. When modifying code, ensure process isolation is maintained.

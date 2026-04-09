"""
OvoMatrix — Build Script
Runs PyInstaller without CMD path-quoting issues on paths with spaces.
"""
import subprocess
import sys
import os
import shutil

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)

print("=" * 60)
print("  OvoMatrix v8.2 — Python Build Script")
print("=" * 60)

# ── Step 1: Install dependencies ──────────────────────────────
print("\n[1/4] Installing / updating requirements ...")
packages = [
    "pyinstaller", "PyQt6", "pyqtgraph", "paho-mqtt",
    "pyserial", "requests", "python-dotenv", "Pillow"
]
result = subprocess.run(
    [sys.executable, "-m", "pip", "install", "--quiet"] + packages
)
if result.returncode != 0:
    print("  [FAIL] pip install failed!")
    sys.exit(1)
print("  [OK] Requirements installed.")

# ── Step 2: Clean previous build ──────────────────────────────
print("\n[2/4] Cleaning old build artifacts ...")
for d in ["build", os.path.join(ROOT, "dist", "ovomatrix.exe"), "ovomatrix.spec"]:
    full = os.path.join(ROOT, d) if not os.path.isabs(d) else d
    if os.path.isdir(full):
        shutil.rmtree(full, ignore_errors=True)
        print(f"  Removed: {full}")
    elif os.path.isfile(full):
        os.remove(full)
        print(f"  Removed: {full}")
print("  [OK] Clean done.")

# ── Step 3: PyInstaller ───────────────────────────────────────
print("\n[3/4] Running PyInstaller (may take a few minutes) ...")

icon_arg = []
ico_path = os.path.join(ROOT, "logo.ico")
if os.path.exists(ico_path):
    icon_arg = [f"--icon={ico_path}"]

cmd = [
    sys.executable, "-m", "PyInstaller",
    "--noconsole",
    "--onefile",
    f"--add-data={os.path.join(ROOT, '.env')};.",
    "--hidden-import=paho.mqtt.client",
    "--hidden-import=paho.mqtt.publish",
    "--hidden-import=paho.mqtt.enums",
    "--hidden-import=serial",
    "--hidden-import=requests",
    "--hidden-import=pyqtgraph",
    "--hidden-import=sqlite3",
    "--hidden-import=dotenv",
    "--name=ovomatrix",
    *icon_arg,
    os.path.join(ROOT, "OvoMatrix_App.py"),
]

result = subprocess.run(cmd, cwd=ROOT)
if result.returncode != 0:
    print("\n  [FAIL] PyInstaller build failed! See errors above.")
    sys.exit(1)
print("  [OK] PyInstaller build complete.")

# ── Step 4: Copy .env to dist\ ───────────────────────────────
print("\n[4/4] Copying runtime files to dist\\ ...")
dist_dir = os.path.join(ROOT, "dist")
shutil.copy(os.path.join(ROOT, ".env"), os.path.join(dist_dir, ".env"))
if os.path.exists(ico_path):
    shutil.copy(ico_path, os.path.join(dist_dir, "logo.ico"))

# Cleanup
shutil.rmtree(os.path.join(ROOT, "build"), ignore_errors=True)
spec = os.path.join(ROOT, "ovomatrix.spec")
if os.path.exists(spec):
    os.remove(spec)

# ── Done ──────────────────────────────────────────────────────
exe = os.path.join(dist_dir, "ovomatrix.exe")
if os.path.exists(exe):
    print("\n" + "=" * 60)
    print("  BUILD SUCCESSFUL!")
    print(f"  EXE: {exe}")
    print("=" * 60)
else:
    print("\n  [FAIL] ovomatrix.exe not found after build!")
    sys.exit(1)

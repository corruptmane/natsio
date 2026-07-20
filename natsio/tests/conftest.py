import sys
from pathlib import Path

# Shared harnesses (fake.py, server.py) live here; make them importable from
# every test subdirectory regardless of pytest's per-file sys.path insertion.
_ROOT = str(Path(__file__).resolve().parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

"""Test Step 1: Module structure and config"""

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from atlas.ctdp import DEFAULT_PLOT_PATH, DEFAULT_N_TICKS, DEFAULT_NUM_COLS

print("✓ Step 1: Module structure created successfully")
print(f"  DEFAULT_PLOT_PATH = {DEFAULT_PLOT_PATH}")
print(f"  DEFAULT_N_TICKS = {DEFAULT_N_TICKS}")
print(f"  DEFAULT_NUM_COLS = {DEFAULT_NUM_COLS}")

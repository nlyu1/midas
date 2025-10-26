"""Test Step 5: Streamlit app - run this with streamlit"""

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from atlas.ctdp.app import run_app

# Use the test state created in step 4
test_cache_dir = Path("./test_ctdp_state")

if not test_cache_dir.exists():
    print("❌ Test state not found!")
    print("   Run test_step4.py first to create the test state")
    sys.exit(1)

print(f"✓ Loading CTDP app from: {test_cache_dir}")
run_app(test_cache_dir)

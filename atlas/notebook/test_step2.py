"""Test Step 2: Type helpers (formatting.py)"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

import polars as pl
from atlas.ctdp.formatting import detect_col_type, format_value, get_col_range

# Create test DataFrame with all supported types
test_df = pl.DataFrame({
    'numeric_int': [1, 2, 3, 4, 5],
    'numeric_float': [1.1, 2.2, 3.3, 4.4, 5.5],
    'duration': [timedelta(seconds=i*10.5) for i in range(1, 6)],
    'timestamp': [datetime(2025, 1, i) for i in range(1, 6)],
    'date_col': [date(2025, 1, i) for i in range(1, 6)],
})

print("✓ Step 2: Testing formatting.py")
print("\n1. Type Detection:")
for col in test_df.columns:
    col_type = detect_col_type(test_df, col)
    print(f"  {col:20} -> {col_type}")

print("\n2. Value Formatting:")
# Test each type
print(f"  numeric_int:  {format_value(test_df['numeric_int'][2], 'numeric')}")
print(f"  numeric_float: {format_value(test_df['numeric_float'][2], 'numeric')}")
print(f"  duration:     {format_value(test_df['duration'][2], 'timedelta')}")
print(f"  timestamp:    {format_value(test_df['timestamp'][2], 'datetime')}")
print(f"  date_col:     {format_value(test_df['date_col'][2], 'date')}")

print("\n3. Range Extraction:")
for col in test_df.columns:
    min_val, max_val = get_col_range(test_df, col)
    col_type = detect_col_type(test_df, col)
    print(f"  {col:20} -> [{format_value(min_val, col_type)}, {format_value(max_val, col_type)}]")

print("\n✓ All type helper tests passed!")

"""Template library for the Midas workspace.

This is a simple template showing how to create a new library in the workspace.
It includes:
- Rust functions exposed to Python via PyO3
- Python helper utilities
- Proper package structure

To create a new library:
1. Copy this entire directory: cp -r template new_library_name
2. Rename all instances of 'template' to your library name in:
   - Cargo.toml
   - pyproject.toml
   - src/lib.rs
   - python/template/__init__.py (and rename the directory)
3. Add to workspace in /home/nlyu/Code/midas/pyproject.toml members list
4. Run: cd /home/nlyu/Code/midas && uv sync
"""

# Import Rust functions from the extension module
from .template import add, multiply, greet

# Python helper utilities
def subtract(a: int, b: int) -> int:
    """Subtract b from a (pure Python example)."""
    return a - b

def divide(a: float, b: float) -> float:
    """Divide a by b (pure Python example)."""
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b

# Expose public API
__all__ = [
    # Rust functions
    "add",
    "multiply",
    "greet",
    # Python functions
    "subtract",
    "divide",
]

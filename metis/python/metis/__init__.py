# Import Rust functions from the extension module
from . import metis as _metis_ext
from .metis import add, multiply, greet
from . import featurepipeline
from . import utils


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
    "featurepipeline",
    "utils"
]

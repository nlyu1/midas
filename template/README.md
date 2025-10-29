# Template Library

This is a template for creating new libraries in the Midas workspace.

## Structure

```
template/
├── Cargo.toml              # Rust package configuration
├── pyproject.toml          # Python package configuration
├── src/
│   └── lib.rs             # Rust code with PyO3 bindings
└── python/
    └── template/
        └── __init__.py    # Python module code
```

## Features

This template includes:

- ✅ **Rust functions** exposed to Python via PyO3 (`add`, `multiply`, `greet`)
- ✅ **Python utilities** for pure Python code
- ✅ **Proper workspace integration** with maturin and uv
- ✅ **Standard configuration** (Python >=3.12, PyO3 0.26.0, maturin 1.9-2.0)

## Creating a New Library

To create a new library from this template:

### 1. Copy the template

```bash
cd /home/nlyu/Code/midas
cp -r template my_new_library
cd my_new_library
```

### 2. Rename all instances

Replace `template` with your library name in these files:

**Cargo.toml:**
```toml
[package]
name = "my_new_library"  # Change this

[lib]
name = "my_new_library"  # Change this
```

**pyproject.toml:**
```toml
[project]
name = "my_new_library"  # Change this
```

**src/lib.rs:**
```rust
#[pymodule]
fn my_new_library(m: &Bound<'_, PyModule>) -> PyResult<()> {  // Change this
    // ...
}
```

**python/template/__init__.py:**
- Rename directory: `mv python/template python/my_new_library`
- Update import: `from .my_new_library import ...`

### 3. Add to workspace

Edit `/home/nlyu/Code/midas/pyproject.toml`:

```toml
[tool.uv.workspace]
members = ["agora", "atlas", "metis", "mnemosyne", "template", "my_new_library"]  # Add here
```

Also add to `/home/nlyu/Code/midas/Cargo.toml` if it exists:

```toml
[workspace]
members = [..., "my_new_library"]  # Add here
```

### 4. Install and test

```bash
cd /home/nlyu/Code/midas

# Sync workspace (builds all libraries including new one)
uv sync

# Test the new library
uv run python -c "import my_new_library; print(my_new_library.add(5, 3))"
```

## Usage Example

```python
import template

# Use Rust functions (fast!)
result = template.add(10, 20)
print(f"10 + 20 = {result}")

product = template.multiply(6, 7)
print(f"6 × 7 = {product}")

greeting = template.greet("Alice")
print(greeting)

# Use Python functions
difference = template.subtract(50, 12)
print(f"50 - 12 = {difference}")

quotient = template.divide(100, 4)
print(f"100 / 4 = {quotient}")
```

## Adding Dependencies

### Python Dependencies

Edit `pyproject.toml`:

```toml
[project]
dependencies = [
    "numpy>=1.24.0",
    "pandas>=2.0.0",
]
```

Then run: `cd /home/nlyu/Code/midas && uv sync`

### Rust Dependencies

Edit `Cargo.toml`:

```toml
[dependencies]
pyo3 = "0.25.0"
serde = { version = "1.0", features = ["derive"] }
```

Then run: `cargo build`

### Depending on Other Workspace Members

In `pyproject.toml`:

```toml
[project]
dependencies = ["atlas"]

[tool.uv.sources]
atlas = { workspace = true }
```

## Tips

1. **Keep Rust fast**: Use Rust for computationally intensive operations
2. **Python for glue**: Use Python for high-level logic and coordination
3. **Test regularly**: Run `cargo test` and `pytest` frequently
4. **Document well**: Add docstrings to both Rust (#[doc = "..."]) and Python functions
5. **Use workspace**: Leverage shared dependencies and consistent versions

## Common Commands

```bash
# Build just this library
cd /home/nlyu/Code/midas
uv run maturin develop -m template/Cargo.toml

# Run Rust tests
cd template
cargo test

# Run Python tests
cd /home/nlyu/Code/midas
uv run pytest template/tests/

# Add a Python dependency
cd template
uv add numpy

# Check types
cd /home/nlyu/Code/midas
uv run mypy template/

# Format/lint
uv run ruff check template/
uv run ruff format template/
```

## License

Part of the Midas workspace.

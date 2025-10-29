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


# Specification

I need a powerful, high-performance interactive web-based parquet dataset explorer, I plan to name it Atlas. 
It's primay inputs are:

1. Path to a parquet on the filesystem. 
2. group_by_col: str
3. weight_col: str
4. cumsum_cols: List[str]
5. feature_cols: List[str]
6. categorical_cols: List[str]

Let's define a cumsum plot(x_col, weight_col, cumsum_cols) as the following:
- Sort (x, weight, cumsum_cols) by x
- Put the x-col on the x-axis, notably "warped" by weight, so that x-rows with larger weight_col get larger share of the x-axis
- Plot the cumsum cols. 

Our atlas engine then makes one cumsum_plot for each feature_col\in feature_cols. 
- Plotting should ideally be parallel, as I'll be working with huge datasets. 
- Final result should be aggregated into one single webpage. 
- For categorical values, just display vertical bar plots. 

Additionally, I want some more custom functionalities: 

1. At the top of the whole plot, I can do a filter of which cumsum_cols to plot. This can be ticked / specified by regex. Can also be cleared. 
2. On top of **each** plot (feature), I can specify a filter of its value ranges. Applying this change will **recompute** the whole plot, equivalently as of on the sub-dataframe where x-values are subsampled. Note that these range filters should be chain-able with each other
3. Similarly, for each categorical subplot, I can specify regex-match / tick match which filters for categorical values. 

# Some more info:

1. I'll be working with dataframe with millions of rows. Ideally I don't want subsampling, so that things should be very fast. Polars is my dataframe engine of choice here, hands down. 
2. I'm extremely proficient in Rust & PyO3 bindings, but I'm not very familiar with web-development. I've heard that a friend did something like this in WebAssembly. 
3. Consider the whole specification and think carefully about potential architecture / tech stack designs. Ultrathink. You will need to convince me about how the whole workflow works, and how **each** of my specifications & features can be achieved by the tech stack. If you're going into webassembly, you should elaborate on more details there, including build, dev, & use workflow on the behavioral & systems level. 
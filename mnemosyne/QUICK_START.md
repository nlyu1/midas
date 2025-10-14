# Mnemosyne Quick Start

## TL;DR Command Reference

```bash
# === FIRST TIME SETUP ===
poetry install                          # Install Python dependencies
poetry run maturin develop --release    # Build Rust + install package

# === DAILY DEVELOPMENT ===
poetry shell                            # Activate venv (interactive work)
poetry run python script.py             # Run script in venv
poetry run maturin develop              # Rebuild after Rust changes

# === DEPENDENCY MANAGEMENT ===
poetry add <package>                    # Add Python dependency
poetry add --group dev <package>        # Add dev dependency
poetry remove <package>                 # Remove dependency
poetry update                           # Update all packages

# === TROUBLESHOOTING ===
unset CONDA_PREFIX                      # Fix conda conflict
poetry env info                         # Show venv location
which python                            # Verify active Python
poetry env list                         # List available venvs
```

## Current Environment

**Poetry venv location:**
```
/home/nlyu/.cache/pypoetry/virtualenvs/mnemosyne-pXsC6e9R-py3.12
```

**Access methods:**
1. `poetry shell` - Activate venv
2. `poetry run <command>` - Run command in venv
3. `source $(poetry env info --path)/bin/activate` - Manual activation

## Understanding Your Setup

### What Poetry Does (Use Case 1: Active Development)
```bash
poetry add numpy
```
1. Adds `numpy = "^1.26.0"` to `pyproject.toml`
2. Resolves all dependencies
3. Updates `poetry.lock` with exact versions
4. Installs into active venv

### What Poetry Does (Use Case 2: Reproducibility)
```bash
# On another machine
git clone repo
poetry install  # Creates venv with EXACT versions from poetry.lock
```
- Everyone gets identical environments
- Works across Linux/Mac/Windows
- Includes transitive dependencies

### Your Rust + Python Setup
```bash
poetry install                    # Python deps only
poetry run maturin develop        # Builds Rust, installs mnemosyne package
```
**Why separate?** `package-mode = false` means Poetry doesn't manage the mnemosyne package itself.

## VSCode Integration

### Option 1: In-Project Venv (Recommended)
```bash
poetry config virtualenvs.in-project true
poetry env remove mnemosyne-pXsC6e9R-py3.12
poetry install  # Creates .venv/ in project root
```
Then in VSCode: `Cmd/Ctrl+Shift+P` → "Python: Select Interpreter" → `./venv/bin/python`

### Option 2: Use Cache Location (Current Setup)
VSCode settings already configured to find it at:
```
${workspaceFolder}/.venv/bin/python
```
(will work after switching to in-project venv)

## Fixing Conda Conflict

You see both `(base)` and `(mnemosyne)` because both are active. Solutions:

**Quick fix:**
```bash
unset CONDA_PREFIX && poetry run maturin develop
```

**Permanent fix:**
```bash
conda config --set auto_activate_base false
# Restart terminal
```

## File Organization

```
pyproject.toml    # Your requirements (human-readable)
poetry.lock       # Exact versions (auto-generated)
Cargo.toml        # Rust dependencies

✓ Commit to Git: pyproject.toml, poetry.lock, Cargo.toml
✗ Don't commit: .venv/, target/, __pycache__/
```

## When Things Go Wrong

```bash
# Python package not found
poetry run maturin develop

# Dependency conflict
poetry update
poetry install

# Wrong Python version
poetry env use 3.12
poetry install

# Start fresh
poetry env remove --all
poetry install
poetry run maturin develop
```

## Next Steps

1. **Fix conda conflict**: `conda config --set auto_activate_base false`
2. **Switch to in-project venv** (optional, better VSCode integration):
   ```bash
   poetry config virtualenvs.in-project true
   poetry env remove mnemosyne-pXsC6e9R-py3.12
   poetry install
   ```
3. **Build your package**: `poetry run maturin develop --release`
4. **Select interpreter in VSCode**: `Cmd/Ctrl+Shift+P` → Python: Select Interpreter

For more details, see `package_management.md`.
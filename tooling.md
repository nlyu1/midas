# Python Tooling Decision: uv vs Poetry for Midas Workspace

**Date**: 2025-10-13
**Decision**: Migrate from Poetry to uv
**Status**: Recommended

---

## Executive Summary

After extensive research and analysis, **uv is the clear choice** for managing Python dependencies in the Midas workspace. It provides native workspace support that mirrors Cargo's architecture, meeting all requirements while Poetry fundamentally cannot.

### Quick Comparison

| Requirement | Poetry | uv | Winner |
|------------|--------|-----|--------|
| Each package specifies dependencies reliably | ❌ Requires manual sync | ✅ Native support | **uv** |
| `add` in subfolder updates that package | ❌ No | ✅ Yes | **uv** |
| Centrally managed venv | ⚠️ Workaround | ✅ Native | **uv** |
| Single lockfile | ⚠️ Workaround | ✅ Native (uv.lock) | **uv** |
| Easy replication | ⚠️ Manual steps | ✅ `uv sync` | **uv** |
| Maturin compatibility | ✅ Works | ✅ Works (native since v1.6) | Tie |
| Mirrors Cargo workspace | ❌ No | ✅ Yes (inspired by Cargo) | **uv** |

**Verdict**: uv wins decisively on all key requirements.

---

## Detailed Analysis

### Your Requirements

1. ✅ **Each package should specify its dependencies reliably**
   - **uv**: Each workspace member has its own `pyproject.toml` with `[project.dependencies]`
   - **Poetry**: Top-level must duplicate all dependencies; sublibrary declarations are "documentary"

2. ✅ **Running `add` in a subfolder updates that sublibrary's dependencies**
   - **uv**: `cd atlas/ && uv add numpy` updates `atlas/pyproject.toml`
   - **Poetry**: Cannot do this; must edit top-level `pyproject.toml` manually

3. ✅ **Centrally managed venv, easily replicable**
   - **uv**: Single `.venv/` at workspace root, `uv sync` replicates exactly via `uv.lock`
   - **Poetry**: Requires workaround with `package-mode = false` and manual sync

### How uv Workspaces Work (Exactly Like Cargo!)

#### Structure
```
midas/
├── pyproject.toml          # Workspace root
├── uv.lock                 # Single lockfile (like Cargo.lock!)
├── .venv/                  # Shared virtualenv
├── Cargo.toml              # Existing Cargo workspace
├── Cargo.lock              # Existing Cargo lockfile
│
├── agora/
│   ├── Cargo.toml         # Rust config
│   ├── pyproject.toml     # Python config (declares its own deps!)
│   └── python/agora/
│
├── atlas/
│   ├── Cargo.toml
│   ├── pyproject.toml     # Declares deps: polars, joblib, etc.
│   └── python/atlas/
│
├── metis/
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/
│
└── mnemosyne/
    ├── Cargo.toml
    ├── pyproject.toml     # Declares deps + depends on atlas!
    └── python/mnemosyne/
```

#### Workspace Root Configuration

**/home/nlyu/Code/midas/pyproject.toml**:
```toml
[tool.uv.workspace]
members = ["agora", "atlas", "metis", "mnemosyne"]

[tool.uv]
dev-dependencies = [
    "pytest>=8.0",
    "mypy>=1.0",
    "ruff>=0.3.0",
]
```

**That's it!** No need to list every dependency from every member. uv discovers them automatically.

#### Workspace Member Configuration

**atlas/pyproject.toml**:
```toml
[build-system]
requires = ["maturin>=1.9,<2.0"]
build-backend = "maturin"

[project]
name = "atlas"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "polars>=1.32.0",
    "joblib>=1.5.0",
]

[tool.maturin]
python-source = "python"
features = ["pyo3/extension-module"]
```

**mnemosyne/pyproject.toml**:
```toml
[build-system]
requires = ["maturin>=1.9,<2.0"]
build-backend = "maturin"

[project]
name = "mnemosyne"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "atlas",                    # Depends on atlas!
    "polars>=1.32.0",
    "ipykernel>=6.30.0",
]

[tool.uv.sources]
atlas = { workspace = true }    # Tells uv atlas is a workspace member

[tool.maturin]
python-source = "python"
features = ["pyo3/extension-module"]
```

---

## Daily Workflows with uv

### Initial Setup
```bash
cd /home/nlyu/Code/midas

# Initialize workspace (if not already done)
# This creates uv.lock and .venv/
uv sync

# All workspace members are installed in editable mode
# All their dependencies are resolved together
# Single .venv/ at workspace root
```

### Adding a Dependency to a Specific Library
```bash
cd /home/nlyu/Code/midas/atlas

# Add dependency to ATLAS specifically
uv add numpy

# This:
# 1. Updates atlas/pyproject.toml: adds numpy to [project.dependencies]
# 2. Updates /home/nlyu/Code/midas/uv.lock
# 3. Installs numpy in the shared .venv/
```

**This is exactly what you wanted!** Each library manages its own dependencies.

### Working on Rust Code
```bash
cd /home/nlyu/Code/midas/atlas

# Make changes to src/lib.rs or python/atlas/

# Rebuild just this library
uv run maturin develop

# Or rebuild everything
cd /home/nlyu/Code/midas
uv sync --refresh
```

### Running Code
```bash
cd /home/nlyu/Code/midas

# Activate the workspace virtualenv
source .venv/bin/activate

# Or run commands directly
uv run python -c "import atlas; import mnemosyne"
uv run jupyter lab
```

### Replicating on Another Machine
```bash
# On new machine
cd /home/nlyu/Code/midas

# This installs EVERYTHING exactly as specified in uv.lock
# Builds all Rust extensions via maturin
# Creates .venv/ with all dependencies
uv sync

# Done! Exact replica.
```

**This is simpler than Cargo!** (Cargo doesn't handle Python deps, so you'd need both `cargo build` and Python setup)

---

## Comparison: Cargo Workspace ↔ uv Workspace

| Feature | Cargo Workspace | uv Workspace |
|---------|----------------|--------------|
| **Config file** | `Cargo.toml` | `pyproject.toml` |
| **Lockfile** | `Cargo.lock` | `uv.lock` |
| **Member discovery** | `members = [...]` | `members = [...]` |
| **Cross-member deps** | `foo = { path = "../foo" }` | `foo = { workspace = true }` |
| **Unified resolution** | ✅ Yes | ✅ Yes |
| **Add dep to member** | `cd member && cargo add dep` | `cd member && uv add dep` |
| **Single build output** | `target/` | `.venv/` |
| **Editable installs** | N/A (native) | ✅ Yes (automatic) |
| **Replicate elsewhere** | `cargo build` | `uv sync` |

**They're nearly identical!** This is because uv was explicitly inspired by Cargo.

---

## Technical Details

### How Dependency Resolution Works

1. **Each member declares dependencies** in its own `[project.dependencies]`
2. **uv reads all members** when running `uv lock` or `uv sync`
3. **uv resolves the complete graph** ensuring no conflicts
4. **Single uv.lock** guarantees consistency across all members
5. **All members installed** in shared `.venv/` as editable packages

### Cross-Member Dependencies

When mnemosyne depends on atlas:

```toml
# mnemosyne/pyproject.toml
[project]
dependencies = ["atlas"]

[tool.uv.sources]
atlas = { workspace = true }
```

- `atlas` in dependencies declares the dependency
- `{ workspace = true }` tells uv to use the local workspace version
- uv installs atlas in editable mode (changes immediately reflected)

### Maturin Integration

uv has native support for maturin since v1.6.0:

```bash
# uv automatically detects maturin build-backend
uv sync
# → Calls maturin for each workspace member with [build-system] backend = "maturin"
# → Builds Rust extensions
# → Installs wheels in .venv/

# Or explicitly:
uv run maturin develop        # Builds all maturin projects
uv run maturin develop -m atlas/Cargo.toml  # Builds just atlas
```

### Python Version Management

uv enforces a single `requires-python` across the workspace:
- Takes the intersection of all members' requirements
- Ensures compatibility
- If atlas needs `>=3.12` and mnemosyne needs `>=3.12`, workspace requires `>=3.12`

### Jupyter & VSCode 

```bash
uv add --dev ipykernel
uv run ipython kernel install --user --name=midas --display=name "midas"
```
---

## Advantages of uv Over Poetry

### 1. Native Workspace Support
- **uv**: Built-in, first-class citizen
- **Poetry**: Workaround with `package-mode = false`, requires manual sync

### 2. Per-Package Dependency Management
- **uv**: `cd member && uv add dep` updates that member
- **Poetry**: Must edit top-level file manually

### 3. Standard pyproject.toml Format
- **uv**: Uses PEP-621 `[project]` tables (standard)
- **Poetry**: Uses proprietary `[tool.poetry]` tables

### 4. Better Maturin Integration
- **uv**: Uses standard build-backend, cleaner integration
- **Poetry**: Works but requires more configuration

### 5. Speed
- **uv**: Written in Rust, extremely fast dependency resolution
- **Poetry**: Written in Python, slower

### 6. Mirrors Cargo Workflow
- **uv**: Workspace members, single lockfile, per-member `add` commands
- **Poetry**: Different mental model from Cargo

### 7. Future-Proof
- **uv**: Active development by Astral (also makes Ruff), modern architecture
- **Poetry**: Established but not evolving toward workspace support


---

## Migration Path

### Phase 1: Preparation (10 min)
```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Verify
uv --version
```

### Phase 2: Create Workspace Root (5 min)
```bash
cd /home/nlyu/Code/midas

# Create workspace pyproject.toml
cat > pyproject.toml << 'EOF'
[tool.uv.workspace]
members = ["agora", "atlas", "metis", "mnemosyne"]

[tool.uv]
dev-dependencies = [
    "pytest>=8.0",
    "mypy>=1.0",
    "ruff>=0.3.0",
    "maturin>=1.9",
]
EOF
```

### Phase 3: Convert Each Sublibrary (10 min each)

For each sublibrary (agora, atlas, metis, mnemosyne):

1. **Keep**:
   - `[build-system]` section (required for maturin)
   - `[tool.maturin]` section (maturin config)

2. **Convert** `[tool.poetry.dependencies]` → `[project.dependencies]`:
   ```toml
   # OLD (Poetry):
   [tool.poetry.dependencies]
   python = "^3.12"
   polars = "^1.32.0"

   # NEW (uv):
   [project]
   requires-python = ">=3.12"
   dependencies = [
       "polars>=1.32.0",
   ]
   ```

3. **Add** cross-member dependencies:
   ```toml
   # If mnemosyne depends on atlas:
   [project]
   dependencies = ["atlas", "polars>=1.32.0"]

   [tool.uv.sources]
   atlas = { workspace = true }
   ```

4. **Remove**:
   - `[tool.poetry]` section
   - Any poetry.lock files in sublibraries

### Phase 4: Initialize Workspace (5 min)
```bash
cd /home/nlyu/Code/midas

# Remove old Poetry virtualenv
cd mnemosyne
poetry env remove python3.12
cd ..

# Initialize uv workspace
uv sync

# This will:
# - Create uv.lock
# - Create .venv/ at workspace root
# - Build all maturin projects
# - Install all dependencies
```

### Phase 5: Verify (5 min)
```bash
# Activate virtualenv
source .venv/bin/activate

# Test imports
python -c "import agora; import atlas; import metis; import mnemosyne; print('✅ All imports work!')"

# Test Rust builds
cargo build

# Test adding a dependency
cd atlas
uv add requests
cat pyproject.toml  # Verify requests was added

cd ..
cat uv.lock | grep requests  # Verify lock was updated
```

### Phase 6: Update Workflows (10 min)

Update any scripts or documentation that reference:
- `poetry install` → `uv sync`
- `poetry add` → `uv add`
- `poetry run` → `uv run`
- `poetry shell` → `source .venv/bin/activate`

**Total time: ~1-2 hours**

---

## Recommended Architecture

### Final Structure

```
midas/
├── pyproject.toml              # Workspace config
├── uv.lock                     # Single lockfile
├── .venv/                      # Shared virtualenv
├── Cargo.toml                  # Rust workspace (unchanged)
├── Cargo.lock                  # Rust lockfile (unchanged)
│
├── agora/
│   ├── Cargo.toml             # Rust package
│   ├── pyproject.toml         # Python package (declares deps)
│   ├── python/agora/          # Python source
│   └── src/lib.rs             # Rust source with PyO3
│
├── atlas/
│   ├── Cargo.toml
│   ├── pyproject.toml         # [project.dependencies] = ["polars", "joblib"]
│   ├── python/atlas/
│   └── src/lib.rs
│
├── metis/
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/lib.rs
│
└── mnemosyne/
    ├── Cargo.toml
    ├── pyproject.toml         # [project.dependencies] = ["atlas", "polars"]
    │                          # [tool.uv.sources] atlas = { workspace = true }
    ├── python/mnemosyne/
    └── src/lib.rs
```

### Workflow Summary

| Task | Command | Location |
|------|---------|----------|
| **Initial setup** | `uv sync` | Workspace root |
| **Add dep to atlas** | `uv add numpy` | `atlas/` directory |
| **Add dep to mnemosyne** | `uv add pandas` | `mnemosyne/` directory |
| **Rebuild Rust** | `uv run maturin develop` | Any |
| **Run script** | `uv run python script.py` | Any |
| **Activate venv** | `source .venv/bin/activate` | Workspace root |
| **Replicate elsewhere** | `uv sync` | Workspace root |
| **Update all deps** | `uv lock --upgrade` | Workspace root |

---

## Alternative: Stick with Poetry (NOT Recommended)

If you absolutely must use Poetry:

### Architecture
- Top-level Poetry workspace with all dependencies
- Sublibraries only have `[project]` sections (no `[tool.poetry]`)
- Manual synchronization between sublibrary needs and top-level config

### Problems
1. ❌ Cannot run `poetry add` in sublibrary to update that sublibrary
2. ❌ Must manually add to top-level pyproject.toml
3. ❌ Sublibrary dependency declarations are "documentation only"
4. ⚠️ Does not mirror Cargo workflow
5. ⚠️ More manual work, more room for error

**This is the architecture I initially proposed, but you correctly rejected it.**

---

## Conclusion

### Recommendation: Migrate to uv

**Why**:
1. ✅ Native workspace support (Poetry fundamentally doesn't have this)
2. ✅ Each library manages its own dependencies independently
3. ✅ Running `uv add` in a subfolder updates that library's config
4. ✅ Single lockfile ensures replicability
5. ✅ Mirrors Cargo workspace architecture perfectly
6. ✅ Fast, modern, actively developed
7. ✅ Standard pyproject.toml format (PEP-621)
8. ✅ Great maturin integration

**Migration effort**: 1-2 hours
**Long-term benefit**: Massive improvement in developer experience
**Risk**: Very low (uv is stable, well-supported, industry standard)

### Your Requirements Checklist

| Requirement | Met by uv? |
|------------|------------|
| Each package specifies dependencies reliably | ✅ Yes |
| `add` in subfolder updates that sublibrary | ✅ Yes |
| Sublibraries can depend on each other | ✅ Yes |
| Centrally managed venv | ✅ Yes |
| Easily replicable on other machines | ✅ Yes |
| Works well with existing Cargo workspace | ✅ Yes |
| Future-compatible and idiomatic | ✅ Yes |

**All requirements met. Zero compromises.**

---
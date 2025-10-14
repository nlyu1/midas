# Midas Workspace: Library & Dependency Management

## Current Architecture Analysis (2025-10-13)

### Rust/Cargo Structure ✅ (Already Optimal)

```
midas/
├── Cargo.toml          # Workspace manifest with resolver = "3"
├── Cargo.lock          # Single unified lock file
└── members/
    ├── agora/Cargo.toml
    ├── argus/Cargo.toml
    ├── atlas/Cargo.toml
    ├── metis/Cargo.toml
    └── mnemosyne/Cargo.toml
```

**How it works:**
- **Unified Dependency Resolution**: Resolver 3 resolves ALL dependencies across ALL workspace members in a single unified graph
- **Single Lock File**: One `Cargo.lock` ensures consistent dependency versions across the entire workspace
- **Shared Build Output**: All builds output to `midas/target/`, maximizing build cache reuse
- **Automatic Conflict Detection**: Incompatible versions (e.g., pyo3 0.25.0 vs 0.26.0) are caught immediately
- **Cross-Workspace Dependencies**: Members can depend on each other with `{ path = "../other" }`

**When you run `cargo build`:**
1. Reads workspace `Cargo.toml` to discover all members
2. Loads all member `Cargo.toml` files and collects dependencies
3. Resolves the complete dependency graph for the entire workspace
4. Ensures no conflicts (e.g., multiple versions of `pyo3-ffi` linking to native `python` library)
5. Builds dependencies once, shared across all members
6. Can be run from ANY directory in the workspace

**Key Insight**: This is the gold standard for monorepo Rust projects. Nothing needs to change here.

---

### Python/Poetry Structure ❌ (Currently Fragmented)

**Current State:**
```
midas/
├── (no top-level Poetry config)
├── agora/
│   └── pyproject.toml           # Has [tool.poetry]? Status unclear
├── atlas/
│   └── pyproject.toml           # Has [tool.poetry] but NO dependencies section = broken
├── metis/
│   └── pyproject.toml           # Minimal config
└── mnemosyne/
    ├── pyproject.toml           # Full Poetry config
    └── poetry.lock              # Only lock file in entire workspace
```

**Problems:**
1. ❌ No unified Python environment
2. ❌ Atlas has Poetry config but can't add dependencies (missing `[tool.poetry.dependencies]`)
3. ❌ Each sublibrary potentially has different dependency versions
4. ❌ No way to work on multiple libraries together in one environment
5. ❌ Currently using mnemosyne's virtualenv: `/home/nlyu/.cache/pypoetry/virtualenvs/mnemosyne-pXsC6e9R-py3.12`

---

## Proposed Architecture: Poetry "Workspace" Pattern

### ⚠️ Critical Limitation: Poetry Has No Workspace Concept

Unlike Cargo, **Poetry does not support workspaces natively**. However, we can achieve similar functionality using the "non-package mode with path dependencies" pattern.

### Proposed Structure

```
midas/
├── pyproject.toml              # 🆕 Top-level Poetry "workspace" config
├── poetry.lock                 # 🆕 Single unified lock file
├── .venv/                      # 🆕 Single virtualenv for entire workspace
├── Cargo.toml                  # Existing Cargo workspace
├── Cargo.lock                  # Existing Cargo lock
│
├── agora/
│   ├── Cargo.toml             # Rust package config
│   ├── pyproject.toml         # [build-system] + [project] only
│   └── python/agora/          # Python source + PyO3 bindings
│
├── atlas/
│   ├── Cargo.toml
│   ├── pyproject.toml         # [build-system] + [project] only
│   └── python/atlas/
│
├── metis/
│   ├── Cargo.toml
│   ├── pyproject.toml         # [build-system] + [project] only
│   └── src/lib.rs             # (Has PyO3 bindings but no python/ dir)
│
└── mnemosyne/
    ├── Cargo.toml
    ├── pyproject.toml         # [build-system] + [project] only
    └── python/mnemosyne/
```

### How This Achieves Your Requirements

#### ✅ Requirement 1: Top-level Poetry virtualenv for the whole project
- Single `pyproject.toml` at `/home/nlyu/Code/midas/`
- One virtualenv: `/home/nlyu/Code/midas/.venv/` (using `virtualenvs.in-project = true`)
- All work happens in this unified environment

#### ✅ Requirement 2: Each sublibrary can declare its own dependencies
- Each sublibrary's `pyproject.toml` has `[project.dependencies]` listing what it needs
- Top-level `pyproject.toml` aggregates these (or Poetry reads them via path dependencies)
- **Clarification**: Sublibraries DECLARE dependencies, top-level Poetry RESOLVES them
  - This is NOT the same as "each sublibrary manages its own dependencies independently"
  - But it's the best possible with Poetry's architecture

#### ✅ Requirement 3: Libraries can be installed in isolation if needed
- Each sublibrary retains `[build-system]` and `[project]` sections
- Can run `maturin develop` or `pip install -e .` in any sublibrary directory
- Will work in ANY Python environment (not just the top-level one)
- Useful for testing individual libraries or using them in other projects

#### ✅ Requirement 4: Libraries should build together
- Top-level Poetry installs all sublibraries as editable path dependencies
- Running `poetry install` builds all Rust extensions via maturin
- Changes to any library are immediately reflected (no reinstall needed)
- Cargo workspace ensures Rust dependencies are consistent

---

## Detailed Implementation Plan

### Top-Level Configuration

**/home/nlyu/Code/midas/pyproject.toml:**
```toml
[tool.poetry]
name = "midas-workspace"
version = "0.1.0"
description = "Unified development environment for Midas workspace"
authors = ["Your Name <your.email@example.com>"]
package-mode = false  # This is a workspace, not a package to publish

[tool.poetry.dependencies]
python = "^3.12"

# Install all sublibraries as editable path dependencies
# These will be built by maturin when you run `poetry install`
agora = {path = "agora", develop = true}
atlas = {path = "atlas", develop = true}
metis = {path = "metis", develop = true}
mnemosyne = {path = "mnemosyne", develop = true}

# Workspace-wide Python dependencies
# (These are collected from all sublibraries - keep in sync with sublibrary needs)
polars = {version = "^1.32.3", extras = ["gpu"]}
ipykernel = "^6.30.1"
graphviz = "^0.21"
joblib = "^1.5.2"
maturin = {extras = ["patchelf"], version = "^1.9.6"}

[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
mypy = "^1.0"
ruff = "^0.3.0"

[[tool.poetry.source]]
name = "nvidia"
url = "https://pypi.nvidia.com"
priority = "explicit"
```

### Sublibrary Configuration Pattern

Each sublibrary's `pyproject.toml` should look like:

**Example: atlas/pyproject.toml**
```toml
[build-system]
requires = ["maturin>=1.9,<2.0"]
build-backend = "maturin"

[project]
name = "atlas"
version = "0.1.0"
requires-python = ">=3.12"
classifiers = [
    "Programming Language :: Rust",
    "Programming Language :: Python :: Implementation :: CPython",
]
dynamic = ["version"]

# Optional: Declare what THIS library needs
# Top-level Poetry will resolve these when installing via path dependency
dependencies = [
    "polars>=1.32.0",
]

[tool.maturin]
python-source = "python"
features = ["pyo3/extension-module"]
```

**Key points:**
- ✅ Keep `[build-system]` - required for maturin to build
- ✅ Keep `[project]` - required metadata
- ✅ Add `[project.dependencies]` - declares what this lib needs (optional but recommended)
- ❌ Remove `[tool.poetry]` sections - no longer needed at sublibrary level
- ❌ Remove any sublibrary-level `poetry.lock` files

---

## Dependency Resolution Flow

### Scenario 1: Working on the entire workspace

```bash
cd /home/nlyu/Code/midas
poetry install
# Poetry resolves ALL dependencies from top-level + all path dependencies
# Maturin builds all Rust extensions
# Everything installed in /home/nlyu/Code/midas/.venv/

poetry shell  # Activate the unified environment
# OR
poetry run jupyter lab  # Run commands in the environment
```

### Scenario 2: Building a single library in isolation

```bash
cd /home/nlyu/Code/midas/atlas
pip install -e .  # OR: maturin develop
# Uses atlas/pyproject.toml [build-system] and [project]
# Installs into whatever environment is currently active
# Can be the top-level .venv OR a completely different environment
```

### Scenario 3: Inter-library dependencies

If mnemosyne needs to use atlas:

**Option A: Already handled via top-level**
- Both installed in top-level environment
- Just `import atlas` in mnemosyne code
- No explicit dependency declaration needed

**Option B: Explicit declaration (for clarity)**
- Add to mnemosyne/pyproject.toml `[project.dependencies]`:
  ```toml
  dependencies = [
      "atlas",  # Note: No version/path here since it's in the workspace
  ]
  ```
- Top-level pyproject.toml already has `atlas = {path = "atlas", develop = true}`
- Poetry resolves this automatically

---

## Cargo ↔ Poetry Integration

### How They Work Together

1. **Cargo Workspace** ensures Rust dependencies are consistent:
   - All members must use compatible PyO3 versions
   - Single `Cargo.lock` prevents version conflicts
   - Shared `target/` directory for compiled artifacts

2. **Poetry "Workspace"** ensures Python dependencies are consistent:
   - All members get dependencies from one `poetry.lock`
   - Single `.venv/` contains all Python packages + built Rust extensions
   - Running `poetry install` triggers maturin for all sublibraries

3. **Maturin** bridges Rust and Python:
   - Reads `[build-system]` and `[tool.maturin]` from each sublibrary's pyproject.toml
   - Calls `cargo build` (which uses the Cargo workspace)
   - Produces Python wheels and installs them in the Poetry virtualenv

### Dependency Types

| **Type** | **Managed By** | **File** | **Example** |
|----------|---------------|----------|-------------|
| Rust deps | Cargo | `Cargo.toml` (member level) | `pyo3 = "0.26.0"` |
| Python deps | Poetry | `pyproject.toml` (top-level) | `polars = "^1.32.3"` |
| Cross-member (Rust) | Cargo | `Cargo.toml` (member level) | `agora = { path = "../agora" }` |
| Cross-member (Python) | Poetry | `pyproject.toml` (top-level) | `agora = {path = "agora", develop = true}` |

---

## Migration Steps

### Step 1: Create top-level Poetry configuration
```bash
cd /home/nlyu/Code/midas
# Create pyproject.toml with content shown above
```

### Step 2: Clean up sublibrary configurations
```bash
# For each sublibrary (agora, atlas, metis, mnemosyne):
# - Remove [tool.poetry] sections from pyproject.toml
# - Keep [build-system], [project], [tool.maturin]
# - Optionally add [project.dependencies] to declare needs
```

### Step 3: Remove mnemosyne's isolated environment
```bash
cd /home/nlyu/Code/midas/mnemosyne
poetry env remove python3.12  # Removes mnemosyne-specific virtualenv
rm poetry.lock                # Will be replaced by top-level lock
cd ..
```

### Step 4: Initialize top-level Poetry workspace
```bash
cd /home/nlyu/Code/midas
poetry install
# This will:
# - Create /home/nlyu/Code/midas/.venv/
# - Install all dependencies from top-level pyproject.toml
# - Build all sublibraries with maturin
# - Generate /home/nlyu/Code/midas/poetry.lock
```

### Step 5: Verify everything works
```bash
poetry shell
python -c "import agora; import atlas; import metis; import mnemosyne"
# Should work without errors

# Test Rust builds
cargo build
# Should compile all workspace members successfully
```

---

## Daily Workflow

### Starting work
```bash
cd /home/nlyu/Code/midas
poetry shell  # Activate workspace environment
```

### Adding a new dependency

**If it's workspace-wide (multiple libraries need it):**
```bash
cd /home/nlyu/Code/midas
poetry add numpy
# Updates top-level pyproject.toml and poetry.lock
```

**If it's specific to one library (for documentation):**
```bash
# Edit that library's pyproject.toml:
# [project.dependencies]
# numpy = ">=1.26.0"

# Then update top-level:
cd /home/nlyu/Code/midas
poetry add numpy  # Must also add to top-level for resolution
```

### Working on a specific library
```bash
cd /home/nlyu/Code/midas/atlas
# Make changes to Rust or Python code
# Changes are immediately reflected (editable install)

# If you changed Rust code, rebuild:
cd /home/nlyu/Code/midas
poetry run maturin develop -m atlas/Cargo.toml
# OR just rebuild everything:
poetry install
```

### Running tests
```bash
cd /home/nlyu/Code/midas
poetry run pytest agora/tests/
poetry run pytest mnemosyne/tests/
# OR run all tests:
poetry run pytest
```

### Using Jupyter notebooks
```bash
cd /home/nlyu/Code/midas
poetry run jupyter lab
# All libraries available in notebooks
```

---

## Comparison: Cargo vs Poetry Workspaces

| **Feature** | **Cargo Workspace** | **Poetry "Workspace"** |
|-------------|---------------------|------------------------|
| **Native Support** | ✅ Yes | ❌ No (using workaround) |
| **Single Lock File** | ✅ Yes (`Cargo.lock`) | ✅ Yes (`poetry.lock`) |
| **Unified Resolution** | ✅ Yes | ✅ Yes (via path dependencies) |
| **Member Independence** | ✅ Can build individually | ✅ Can build individually (via maturin) |
| **Automatic Discovery** | ✅ Yes (via `members = [...]`) | ❌ Must manually list in `[tool.poetry.dependencies]` |
| **Version Conflict Detection** | ✅ Automatic | ✅ Automatic (via unified resolution) |
| **Cross-Member Dependencies** | ✅ `{ path = "../other" }` | ✅ `{path = "other", develop = true}` |
| **Isolated Development** | ✅ Yes | ⚠️  Limited (needs complete pyproject.toml) |
| **Idiomatic** | ✅ 100% | ⚠️  ~70% (documented pattern but not "native") |

---

## Caveats & Limitations

### 1. Poetry's Lack of True Workspace Support
- Poetry was designed for single-package projects, not monorepos
- This pattern works but requires manual synchronization of dependencies
- If you add a dependency to a sublibrary's `[project.dependencies]`, you MUST also add it to the top-level `[tool.poetry.dependencies]`

### 2. Two Sources of Truth for Dependencies
- Sublibrary `[project.dependencies]`: Documents what each library needs
- Top-level `[tool.poetry.dependencies]`: Actually controls what's installed
- Must keep these in sync manually

### 3. No Automatic Member Discovery
- Unlike Cargo's `members = [...]` which auto-discovers
- Poetry requires explicitly listing each sublibrary as a path dependency

### 4. Isolated Development Has Friction
- While you CAN build sublibraries in isolation with maturin
- You lose Poetry's dependency management for that individual library
- Best practice: Always work in the top-level environment

### 5. Alternative Tools Exist
- **PDM**: Has better workspace support (`pdm.lock` with groups)
- **Hatch**: Has workspace support via `[tool.hatch.envs.default]`
- **uv**: Emerging fast installer with workspace support
- Switching tools is disruptive; staying with Poetry is pragmatic for now

---

## Is This Idiomatic?

**Rust/Cargo side:** ✅ 100% idiomatic. This is exactly how Rust workspaces should be structured.

**Python/Poetry side:** ⚠️  ~70% idiomatic.
- ✅ Using `package-mode = false` for non-package roots is documented
- ✅ Path dependencies with `develop = true` is the recommended pattern
- ⚠️  Manual dependency synchronization is a known pain point
- ⚠️  This pattern is common in monorepos but not Poetry's primary use case

**Overall verdict:** This is the **most practical solution** given your requirements and Poetry's limitations. It achieves all your stated goals:
- ✅ Top-level unified environment
- ✅ Sublibraries declare their own dependencies
- ✅ Can build in isolation
- ✅ Libraries build together coherently

The only superior alternative would be switching to a tool with native workspace support (PDM, Hatch, or uv), but that's a significant disruption for marginal benefit.

---

## Conclusion

This architecture creates a **unified development environment** that mirrors Cargo's workspace pattern as closely as Poetry allows. While not perfect due to Poetry's limitations, it provides:

1. **Single source of truth**: One `poetry.lock` for all Python dependencies
2. **Consistent versions**: No conflicts between sublibraries
3. **Efficient development**: One virtualenv, editable installs, immediate reflection of changes
4. **Flexibility**: Can still build sublibraries individually if needed
5. **Cargo alignment**: Python structure parallels the existing Cargo workspace

The key tradeoff is manual dependency synchronization between sublibrary declarations and top-level resolution, but this is a small price for the benefits of a unified workspace.

---

**Next Actions:**
1. Review and approve this architecture
2. Implement the migration steps
3. Update any CI/CD pipelines to use top-level Poetry commands
4. Document the workflow for team members

---

*Last Updated: 2025-10-13*
*Author: Claude Code*

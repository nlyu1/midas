# Agora Test Suite

This directory contains the test suite for the Agora publisher-subscriber system.

## Test Files

### `metaserver.rs`
Tests for the MetaServer state management and publisher registration system.

**Key test areas:**
- Publisher registration success cases
- Publisher registration failure cases (empty paths, duplicates, invalid hierarchy)
- Path validation (empty paths, parent-child relationships)
- Directory vs publisher conflicts

### `pathtree.rs`
Tests for the tree data structure used for organizing publishers.

**Key test areas:**
- Tree node creation and hierarchy
- Path traversal and lookup
- Node removal (single and cascading)
- Tree serialization/deserialization (repr format)
- Parent-child relationships

## Test Utilities (`common/mod.rs`)

Shared helper functions for creating test fixtures:

- `test_connection(port)` - Creates a ConnectionHandle with localhost and specified port
- `default_test_connection()` - Creates a ConnectionHandle with default test port (8081)
- `create_test_server_state()` - Creates a pre-configured ServerState with typical API structure

The pre-configured tree structure looks like:
```
agora/
├── api/
│   ├── v1/
│   │   ├── users/
│   │   ├── posts/
│   │   └── auth/
│   └── v2/
├── static/
└── admin/
```

## Running Tests

```bash
# Run all tests
cargo test

# Run specific test file
cargo test --test metaserver
cargo test --test pathtree

# Run with output
cargo test -- --nocapture
```

## Major Changes from Previous Version

### Removed
- `tests/address_manager.rs` - IPv6 address allocation feature was removed from the codebase

### Updated API
- `ServerState::new()` no longer takes a port/UID parameter
- `register_publisher()` now requires a `ConnectionHandle` parameter (IP + port)
- All connection information is now managed through the `ConnectionHandle` abstraction

### Refactoring
- Created `tests/common/mod.rs` for shared test utilities
- Extracted `create_test_server_state()` helper to reduce code duplication
- All tests now use standardized `ConnectionHandle` creation via helper functions

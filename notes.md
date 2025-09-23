1. Adding a new crate: in parent, run `cargo new my-crate` 
    - Add directory to root `Cargo.toml`. 
2. `cargo run --bin {} -- {--args}` 
3. `cargo check`, `cargo test`, `cargo build --release`
4. 



Look at the current code setup carefully. I'm implementing a subscriber-publisher software. Next, help me complete @src/rpc/client.rs . (1) is receiver 
  the right construct to use here? (2) how do I operate on the stream so that it transforms serialized data back to type T correctly?
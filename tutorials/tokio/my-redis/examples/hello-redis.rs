use mini_redis::{Result, client};
// https://tokio.rs/tokio/tutorial/hello-tokio
// Copy to `src/main.rs` to run.

#[tokio::main]
async fn main() -> Result<()> {
    println!("Sample demonstration: even if we called print_world() first, it prints second.");
    let print_world = || async { println!("world") };
    let op = print_world();
    println!("Hello");
    op.await;

    println!("Make sure to start `mini-redis-server` first...");
    let mut client = client::connect("127.0.0.1:6379").await?;
    client.set("hello", "world".into()).await?;
    let result = client.get("hello").await?;
    println!("got value from the server; result={:?}", result);
    Ok(())
}

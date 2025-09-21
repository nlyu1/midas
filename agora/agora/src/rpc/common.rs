#[tarpc::service]
pub trait World {
    /// Returns a greeting for name.
    async fn hello(name: String) -> String;

    async fn hello2(name: String) -> String;
}
// Generates `WorldClient` with `hello` method
// Also generates `WorldServer` with `serve` method.

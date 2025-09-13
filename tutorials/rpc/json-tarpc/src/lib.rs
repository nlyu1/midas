#[tarpc::service]
pub trait World {
    /// Returns a greeting for name.
    async fn hello(name: String) -> String;
}
// Generates `WorldClient` with `hello` method
// Also generates `WorldServer` with `serve` method.

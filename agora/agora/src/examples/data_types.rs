use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;

pub trait RpcPair {
    type Query: Serialize + for<'de> Deserialize<'de> + 'static;
    type Response: Serialize + for<'de> Deserialize<'de> + 'static;
}

pub struct RpcHandle<'a, Op: RpcPair> {
    handler: &'a dyn Fn(&Op::Query) -> Op::Response,
    _phantom: PhantomData<Op>,
}

impl<'a, Op: RpcPair> RpcHandle<'a, Op> {
    pub fn call(&self, query: Op::Query) -> Op::Response {
        (self.handler)(&query)
    }
}

#[derive(Default)]
pub struct Metaserver {
    // What does the "Any" trait do here? Couldn't we have just called Send + Sync?
    registry: HashMap<String, (TypeId, Box<dyn Any + Send + Sync>)>,
}

impl Metaserver {
    pub fn new() -> Self {
        Self::default()
    }

    // What does 'static do?
    pub fn register<Op: RpcPair + 'static>(
        &mut self,
        path: &str,
        handler: impl Fn(&Op::Query) -> Op::Response + Send + Sync + 'static,
    ) {
        println!("✅ Registering operation at '{}'", path);
        let boxed_handler: Box<dyn Fn(&Op::Query) -> Op::Response + Send + Sync> =
            Box::new(handler);
        self.registry.insert(
            path.to_string(),
            // What happened here? Did we erase the type of the handler?
            (
                TypeId::of::<Op>(),
                Box::new(boxed_handler) as Box<dyn Any + Send + Sync>,
            ),
        );
    }

    // Getting a handle requires the user to specify which operation they expect.
    pub fn get_handle<'a, Op: RpcPair + 'static>(
        &'a self,
        path: &str,
    ) -> Option<RpcHandle<'a, Op>> {
        let (registered_type_id, handler_any) = self.registry.get(path)?;

        // --- THE RUNTIME GATEKEEPER ---
        // We check if the operation `Op` the user is asking for matches the one
        // we registered at this path. This is our one essential runtime check.
        if *registered_type_id != TypeId::of::<Op>() {
            return None; // Type mismatch!
        }

        // If the types match, we can safely downcast the handler.
        handler_any
            .downcast_ref::<Box<dyn Fn(&Op::Query) -> Op::Response + Send + Sync>>()
            .map(|handler| RpcHandle {
                handler: handler.as_ref(),
                _phantom: PhantomData,
            })
    }
}

// --- Define our RPC Operations ---

// An operation to get user info.
struct GetUserOp; // This is a Zero-Sized Type (ZST). It's just a name.
impl RpcPair for GetUserOp {
    type Query = u32; // User ID
    type Response = String; // User Name
}

// A different operation to sum two numbers.
struct CalculateSumOp;
impl RpcPair for CalculateSumOp {
    type Query = (i32, i32);
    type Response = i32;
}

// --- Main execution ---

fn main() {
    let mut server = Metaserver::new();

    // Register our two handlers.
    server.register::<GetUserOp>("/users/get", |id: &u32| -> String {
        format!("User-{}", id)
    });
    server.register::<CalculateSumOp>("/math/sum", |(a, b): &(i32, i32)| -> i32 { a + b });

    println!("\n--- Client-side execution ---");

    // 1. Get a handle for the GetUserOp and use it correctly.
    let user_handle = server.get_handle::<GetUserOp>("/users/get").unwrap();
    let user = user_handle.call(101);
    println!(
        "Successfully called GetUserOp with `101`. Result: '{}'",
        user
    );
    assert_eq!(user, "User-101");

    // 2. Get a handle for the CalculateSumOp and use it correctly.
    let math_handle = server.get_handle::<CalculateSumOp>("/math/sum").unwrap();
    let sum = math_handle.call((10, 32));
    println!(
        "Successfully called CalculateSumOp with `(10, 32)`. Result: '{}'",
        sum
    );
    assert_eq!(sum, 42);

    // 3. THIS WILL NOT COMPILE! ❌
    // The compiler knows `user_handle` is for `GetUserOp`, and `GetUserOp::Query` is `u32`, not `(i32, i32)`.
    // let result = user_handle.call((5, 5));
    // ^-- error[E0308]: mismatched types
    //      expected `u32`, found `(i32, i32)`

    // 4. This will fail at RUNTIME, as it should.
    // We are asking for the right handle type (`GetUserOp`) at the wrong path.
    let bad_handle = server.get_handle::<GetUserOp>("/math/sum");
    println!(
        "Attempting to get GetUserOp handle at '/math/sum'. Result: Is None? {}",
        bad_handle.is_none()
    );
    assert!(bad_handle.is_none());
}

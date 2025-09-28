mod core;
pub mod metaserver;
mod ping;
mod pywrappers; 
pub mod constants;
pub mod rawstream;
pub mod utils;

// Re-export core types at the top level for easy access
pub use core::publisher::Publisher;
pub use core::subscriber::{Subscriber, OmniSubscriber};
pub use core::common::Agorable; 

use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn agora(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<pywrappers::subscribers::PyOmniSubscriber>()?;
    Ok(())
}

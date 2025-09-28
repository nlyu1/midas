pub mod constants;
mod core;
pub mod metaserver;
mod ping;
mod pywrappers;
pub mod rawstream;
pub mod utils;

// Re-export core types at the top level for easy access
pub use core::common::Agorable;
pub use core::publisher::Publisher;
pub use core::subscriber::{OmniSubscriber, Subscriber};

use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn agora(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Original classes for backward compatibility
    m.add_class::<pywrappers::subscribers::PyOmniSubscriber>()?;

    // Typed subscribers
    m.add_class::<pywrappers::subscribers::PyStringSubscriber>()?;
    m.add_class::<pywrappers::subscribers::PyI64Subscriber>()?;
    m.add_class::<pywrappers::subscribers::PyBoolSubscriber>()?;
    m.add_class::<pywrappers::subscribers::PyF64Subscriber>()?;
    m.add_class::<pywrappers::subscribers::PyF32Subscriber>()?;

    // Typed publishers
    m.add_class::<pywrappers::publishers::PyStringPublisher>()?;
    m.add_class::<pywrappers::publishers::PyI64Publisher>()?;
    m.add_class::<pywrappers::publishers::PyBoolPublisher>()?;
    m.add_class::<pywrappers::publishers::PyF64Publisher>()?;
    m.add_class::<pywrappers::publishers::PyF32Publisher>()?;

    // Typed iterators
    m.add_class::<pywrappers::subscribers::PyStringIterator>()?;
    m.add_class::<pywrappers::subscribers::PyI64Iterator>()?;
    m.add_class::<pywrappers::subscribers::PyBoolIterator>()?;
    m.add_class::<pywrappers::subscribers::PyF64Iterator>()?;
    m.add_class::<pywrappers::subscribers::PyF32Iterator>()?;

    Ok(())
}

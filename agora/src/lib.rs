pub mod constants;
mod core;
pub mod metaserver;
pub mod ping;
mod pywrappers;
pub mod rawstream;
mod relay;
pub use relay::Relay;
pub mod utils;
pub use utils::ConnectionHandle;

pub mod gateway;

// Re-export core types at the top level for easy access
pub use core::publisher::Publisher;
pub use core::subscriber::{OmniSubscriber, Subscriber};
pub use core::{Agorable, AgorableOption};

use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn agora(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Connection handle
    m.add_class::<pywrappers::connection_handle::PyConnectionHandle>()?;

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

    // Typed relays
    m.add_class::<pywrappers::relays::PyStringRelay>()?;
    m.add_class::<pywrappers::relays::PyI64Relay>()?;
    m.add_class::<pywrappers::relays::PyBoolRelay>()?;
    m.add_class::<pywrappers::relays::PyF64Relay>()?;
    m.add_class::<pywrappers::relays::PyF32Relay>()?;

    // Typed iterators
    m.add_class::<pywrappers::subscribers::PyStringIterator>()?;
    m.add_class::<pywrappers::subscribers::PyI64Iterator>()?;
    m.add_class::<pywrappers::subscribers::PyBoolIterator>()?;
    m.add_class::<pywrappers::subscribers::PyF64Iterator>()?;
    m.add_class::<pywrappers::subscribers::PyF32Iterator>()?;

    Ok(())
}

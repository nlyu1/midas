pub mod crypto;
pub mod datasets;

pub use crypto::binance;

use pyo3::prelude::*;

#[pymodule]
fn mnemosyne(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<datasets::DatasetType>()?;
    Ok(())
}

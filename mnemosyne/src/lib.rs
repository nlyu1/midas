pub mod crypto;
pub mod datasets;

pub use crypto::binance;

use pyo3::prelude::*;

#[pymodule]
fn mnemosyne(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<datasets::DatasetType>()?;
    m.add_function(wrap_pyfunction!(
        crypto::hyperliquid::l2book::py_read_hyperliquid_l2book_bydate_to,
        m
    )?)?;
    Ok(())
}

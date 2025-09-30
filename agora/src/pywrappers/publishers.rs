use crate::Publisher;
use crate::utils::parse_ipv6_str;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

macro_rules! create_typed_publisher {
    ($name:ident, $type:ty) => {
        #[pyclass]
        pub struct $name {
            inner: Publisher<$type>,
            rt: Runtime,
        }

        #[pymethods]
        impl $name {
            #[new]
            fn new(
                name: String,
                path: String,
                initial_value: $type,
                metaserver_addr: String,
                metaserver_port: u16,
            ) -> PyResult<Self> {
                let metaserver_addr =
                    parse_ipv6_str(metaserver_addr).map_err(|e| PyValueError::new_err(e))?;

                // Create runtime that will be kept for the lifetime of this object
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e))
                })?;

                let inner = rt
                    .block_on(Publisher::new(
                        name,
                        path,
                        initial_value,
                        metaserver_addr,
                        metaserver_port,
                    ))
                    .map_err(|e| PyRuntimeError::new_err(e))?;

                Ok(Self { inner, rt })
            }

            fn publish(&mut self, value: $type) -> PyResult<()> {
                Ok(self
                    .rt
                    .block_on(self.inner.publish(value))
                    .map_err(|e| PyRuntimeError::new_err(e))?)
            }
        }
    };
}

// Generate typed publishers for all Agorable types
create_typed_publisher!(PyStringPublisher, String);
create_typed_publisher!(PyI64Publisher, i64);
create_typed_publisher!(PyBoolPublisher, bool);
create_typed_publisher!(PyF64Publisher, f64);
create_typed_publisher!(PyF32Publisher, f32);

use crate::Relay;
use crate::utils::parse_ipv6_str;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

macro_rules! create_typed_relay {
    ($name:ident, $type:ty) => {
        #[pyclass]
        pub struct $name {
            inner: Relay<$type>,
            rt: Runtime,
        }

        #[pymethods]
        impl $name {
            #[new]
            fn new(
                name: String,
                dest_path: String,
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
                    .block_on(Relay::new(
                        name,
                        dest_path,
                        initial_value,
                        metaserver_addr,
                        metaserver_port,
                    ))
                    .map_err(|e| PyRuntimeError::new_err(e))?;

                Ok(Self { inner, rt })
            }

            fn swapon(
                &mut self,
                src_path: String,
                metaserver_addr: String,
                metaserver_port: u16,
            ) -> PyResult<()> {
                let metaserver_addr =
                    parse_ipv6_str(metaserver_addr).map_err(|e| PyValueError::new_err(e))?;

                Ok(self
                    .rt
                    .block_on(
                        self.inner
                            .swapon(src_path, metaserver_addr, metaserver_port),
                    )
                    .map_err(|e| PyRuntimeError::new_err(e))?)
            }
        }
    };
}

// Generate typed relays for all Agorable types
create_typed_relay!(PyStringRelay, String);
create_typed_relay!(PyI64Relay, i64);
create_typed_relay!(PyBoolRelay, bool);
create_typed_relay!(PyF64Relay, f64);
create_typed_relay!(PyF32Relay, f32);

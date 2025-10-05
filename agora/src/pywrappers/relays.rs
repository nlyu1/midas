use crate::Relay;
use crate::pywrappers::connection_handle::PyConnectionHandle;
use pyo3::exceptions::PyRuntimeError;
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
                dest_metaserver_connection: PyConnectionHandle,
                local_gateway_port: u16,
            ) -> PyResult<Self> {
                // Create runtime that will be kept for the lifetime of this object
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e))
                })?;

                let inner = rt
                    .block_on(Relay::new(
                        name,
                        dest_path,
                        initial_value,
                        dest_metaserver_connection.to_connection_handle(),
                        local_gateway_port,
                    ))
                    .map_err(|e| PyRuntimeError::new_err(e))?;

                Ok(Self { inner, rt })
            }

            fn swapon(
                &mut self,
                src_path: String,
                src_metaserver_connection: PyConnectionHandle,
            ) -> PyResult<()> {
                self
                    .rt
                    .block_on(
                        self.inner
                            .swapon(src_path, src_metaserver_connection.to_connection_handle()),
                    )
                    .map_err(|e| PyRuntimeError::new_err(e))
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

use super::async_helpers::PyResultIterator;
use crate::create_py_result_iterator;
use crate::utils::{BlockingStreamIterator, OrError, parse_ipv6_str, stream_to_iter};
use crate::{OmniSubscriber, Subscriber};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

macro_rules! create_typed_subscriber {
    ($name:ident, $iterator_name:ident, $type:ty) => {
        // Create the corresponding PyResultIterator for this type
        create_py_result_iterator!($iterator_name, $type);

        #[pyclass]
        pub struct $name {
            inner: Subscriber<$type>,
            rt: Runtime,
        }

        #[pymethods]
        impl $name {
            #[new]
            fn new(path: String, metaserver_addr: String, metaserver_port: u16) -> PyResult<Self> {
                // convert metaserver_addr from string and parse to Ipv6Addr
                let parsed_addr =
                    parse_ipv6_str(metaserver_addr).map_err(|e| PyValueError::new_err(e))?;

                // Create runtime that will be kept for the lifetime of this object
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e))
                })?;

                let inner = rt
                    .block_on(Subscriber::new(path, parsed_addr, metaserver_port))
                    .map_err(|e| PyRuntimeError::new_err(e))?;

                Ok(Self { inner, rt })
            }

            fn get(&mut self) -> PyResult<$type> {
                let result = self
                    .rt
                    .block_on(self.inner.get())
                    .map_err(|e| PyRuntimeError::new_err(e))?;

                Ok(result)
            }

            fn get_stream(&mut self) -> PyResult<$iterator_name> {
                let (_current_value, stream) = self
                    .rt
                    .block_on(self.inner.get_stream())
                    .map_err(|e| PyRuntimeError::new_err(e))?;

                let rt_handle = self.rt.handle().clone();
                let iterator = stream_to_iter(stream, rt_handle);

                Ok($iterator_name::new(iterator))
            }
        }
    };
}

// Generate typed subscribers for all Agorable types
create_typed_subscriber!(PyStringSubscriber, PyStringIterator, String);
create_typed_subscriber!(PyI64Subscriber, PyI64Iterator, i64);
create_typed_subscriber!(PyBoolSubscriber, PyBoolIterator, bool);
create_typed_subscriber!(PyF64Subscriber, PyF64Iterator, f64);
create_typed_subscriber!(PyF32Subscriber, PyF32Iterator, f32);


#[pyclass]
pub struct PyOmniSubscriber {
    inner: OmniSubscriber,
    rt: Runtime,
}

#[pymethods]
impl PyOmniSubscriber {
    #[new]
    fn new(path: String, metaserver_addr: String, metaserver_port: u16) -> PyResult<Self> {
        // convert metaserver_addr from string and parse to Ipv6Addr
        let parsed_addr = parse_ipv6_str(metaserver_addr).map_err(|e| PyValueError::new_err(e))?;

        // Create runtime that will be kept for the lifetime of this object
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e))
        })?;

        let inner = rt
            .block_on(OmniSubscriber::new(path, parsed_addr, metaserver_port))
            .map_err(|e| PyRuntimeError::new_err(e))?;

        Ok(Self { inner, rt })
    }

    fn get(&mut self) -> PyResult<String> {
        let result = self
            .rt
            .block_on(self.inner.get())
            .map_err(|e| PyRuntimeError::new_err(e))?;

        Ok(result)
    }

    fn get_stream(&mut self) -> PyResult<PyResultIterator> {
        let (_current_value, stream) = self
            .rt
            .block_on(self.inner.get_stream())
            .map_err(|e| PyRuntimeError::new_err(e))?;

        let rt_handle = self.rt.handle().clone();
        let iterator = stream_to_iter(stream, rt_handle);

        Ok(PyResultIterator::new(iterator))
    }
}
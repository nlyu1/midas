use crate::utils::OrError;
use crate::utils::{BlockingStreamIterator, stream_to_iter};
use crate::{OmniSubscriber, Subscriber};
use pyo3::prelude::*;
use std::net::Ipv6Addr;
use tokio::runtime::Runtime;

#[pyclass(unsendable)]
pub struct PyResultIterator {
    inner: BlockingStreamIterator<OrError<String>>,
}

#[pymethods]
impl PyResultIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<String>> {
        match self.inner.next() {
            Some(result) => match result {
                Ok(value) => Ok(Some(value)),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
            },
            None => Ok(None),
        }
    }
}

#[pyclass]
pub struct PyOmniSubscriber {
    inner: OmniSubscriber,
    rt: Runtime,
}

fn parse_ipv6_str(str_addr: String) -> OrError<Ipv6Addr> {
    str_addr
        .parse::<Ipv6Addr>()
        .map_err(|e| format!("Failed to parse IPv6 address '{}': {}", str_addr, e))
}

#[pymethods]
impl PyOmniSubscriber {
    #[new]
    fn new(path: String, metaserver_addr: String, metaserver_port: u16) -> PyResult<Self> {
        // convert metaserver_addr from string and parse to Ipv6Addr
        let parsed_addr = parse_ipv6_str(metaserver_addr)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;

        // Create runtime that will be kept for the lifetime of this object
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create tokio runtime: {}",
                e
            ))
        })?;

        let inner = rt
            .block_on(OmniSubscriber::new(path, parsed_addr, metaserver_port))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

        Ok(Self { inner, rt })
    }

    fn get(&mut self) -> PyResult<String> {
        let result = self
            .rt
            .block_on(self.inner.get())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

        Ok(result)
    }

    fn get_stream(&mut self) -> PyResult<PyResultIterator> {
        let (_current_value, stream) = self
            .rt
            .block_on(self.inner.get_stream())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

        let rt_handle = self.rt.handle().clone();
        let iterator = stream_to_iter(stream, rt_handle);

        Ok(PyResultIterator { inner: iterator })
    }
}

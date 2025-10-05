use crate::utils::ConnectionHandle;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Python-compatible wrapper for ConnectionHandle
#[pyclass]
#[derive(Clone)]
pub struct PyConnectionHandle {
    inner: ConnectionHandle,
}

#[pymethods]
impl PyConnectionHandle {
    /// Create a ConnectionHandle from an IPv4 address string and port
    #[staticmethod]
    fn from_ipv4(addr: String, port: u16) -> PyResult<Self> {
        let ipv4: Ipv4Addr = addr.parse().map_err(|e| {
            PyValueError::new_err(format!("Invalid IPv4 address '{}': {}", addr, e))
        })?;
        let ip_addr = IpAddr::V4(ipv4);
        Ok(Self {
            inner: ConnectionHandle::new(ip_addr, port),
        })
    }

    /// Create a ConnectionHandle from an IPv6 address string and port
    #[staticmethod]
    fn from_ipv6(addr: String, port: u16) -> PyResult<Self> {
        let ipv6: Ipv6Addr = addr.parse().map_err(|e| {
            PyValueError::new_err(format!("Invalid IPv6 address '{}': {}", addr, e))
        })?;
        let ip_addr = IpAddr::V6(ipv6);
        Ok(Self {
            inner: ConnectionHandle::new(ip_addr, port),
        })
    }

    /// Get the address as a string (works for both IPv4 and IPv6)
    fn addr(&self) -> String {
        self.inner.addr().to_string()
    }

    /// Get the port number
    fn port(&self) -> u16 {
        self.inner.port()
    }

    /// String representation of the connection handle
    fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
}

impl PyConnectionHandle {
    /// Convert to the underlying Rust ConnectionHandle
    pub fn to_connection_handle(&self) -> ConnectionHandle {
        self.inner.clone()
    }
}

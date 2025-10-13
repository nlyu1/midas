//! Shared utilities for Agora: `ConnectionHandle` for network addressing, error helpers (`agora_error!`, `agora_error_cause!`),
//! path validation (`strip_and_verify`), and socket setup (`prepare_socket_path`). Used across all modules.

use anyhow::{bail, Context};
use local_ip_address::local_ip;
use std::fmt::{Display, Formatter};
use std::net::IpAddr;

/// Network address handle (IP + port) for gateway and metaserver connections.
/// Serializable for RPC transmission in `PublisherInfo`. Used by `Publisher`, `Subscriber`, `Gateway` to establish connections.
/// Display format: IPv4 as `ip:port`, IPv6 as `[ip]:port`.
#[derive(serde::Deserialize, serde::Serialize, Debug, Copy, Clone, PartialEq, Eq)]
pub struct ConnectionHandle {
    addr: IpAddr,
    port: u16,
}

pub type OrError<T> = anyhow::Result<T>;

/// RPC-compatible error type (serializable for TARPC)
pub type RpcError<T> = Result<T, String>;

/// Creates standardized Agora error message format
#[inline]
pub fn agora_error_msg(component: &str, method: &str, msg: &str) -> String {
    format!("Agora {}::{} Error: {}", component, method, msg)
}

/// Creates standardized Agora error message with formatted arguments
#[macro_export]
macro_rules! agora_error {
    ($component:expr, $method:expr, $msg:expr) => {
        $crate::utils::agora_error_msg($component, $method, $msg)
    };
}

/// Creates error with cause chain (for use with .context())
#[macro_export]
macro_rules! agora_error_cause {
    ($component:expr, $method:expr, $msg:expr, $cause:expr) => {
        format!(
            "Agora {}::{} Error: {}. Caused by -> {}",
            $component, $method, $msg, $cause
        )
    };
}

impl ConnectionHandle {
    pub fn new(addr: IpAddr, port: u16) -> Self {
        Self { addr, port }
    }

    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn new_local(port: u16) -> OrError<Self> {
        let addr = local_ip().context(agora_error!(
            "utils::ConnectionHandle",
            "new_local",
            "failed to get local IP"
        ))?;
        Ok(Self { addr, port })
    }

    pub fn addr_port(&self) -> (IpAddr, u16) {
        (self.addr, self.port)
    }
}

impl Display for ConnectionHandle {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self.addr {
            IpAddr::V6(_) => write!(f, "[{}]:{}", self.addr, self.port),
            IpAddr::V4(_) => write!(f, "{}:{}", self.addr, self.port),
        }
    }
}

/// Strips leading slash and validates path string.
/// Returns normalized path string with no leading/trailing slashes.
///
/// Validates:
/// - No empty path
/// - No double slashes (//)
/// - No special characters (only alphanumeric, -, _, and / allowed)
/// - No directory traversal (..)
pub fn strip_and_verify(path_string: &str) -> OrError<String> {
    if path_string.is_empty() {
        bail!(agora_error!(
            "utils",
            "strip_and_verify",
            "path cannot be empty"
        ));
    }
    let stripped = path_string.trim_matches('/');
    if stripped.is_empty() {
        bail!(agora_error!(
            "utils",
            "strip_and_verify",
            "path cannot be empty after stripping slashes"
        ));
    }
    if stripped.contains("//") {
        bail!(agora_error!(
            "utils",
            "strip_and_verify",
            "path cannot contain double slashes"
        ));
    }
    if stripped.contains("..") {
        bail!(agora_error!(
            "utils",
            "strip_and_verify",
            "path cannot contain '..' (directory traversal)"
        ));
    }
    for c in stripped.chars() {
        if !c.is_alphanumeric() && c != '-' && c != '_' && c != '/' {
            bail!(agora_error!(
                "utils",
                "strip_and_verify",
                &format!("path contains invalid character: '{}'", c)
            ));
        }
    }

    Ok(stripped.to_string())
}

pub fn prepare_socket_path(socket_path: &str) -> OrError<()> {
    let path = std::path::Path::new(socket_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context(agora_error!(
            "utils",
            "prepare_socket_path",
            "failed to create parent directory"
        ))?;
    }
    if path.exists() {
        std::fs::remove_file(path).context(agora_error!(
            "utils",
            "prepare_socket_path",
            "failed to remove existing socket file"
        ))?;
    }
    Ok(())
}

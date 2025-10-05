use local_ip_address::local_ip;
use std::fmt::{Display, Formatter};
use std::net::IpAddr;

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ConnectionHandle {
    addr: IpAddr,
    port: u16,
}

pub type OrError<T> = Result<T, String>;

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
        let addr =
            local_ip().map_err(|e| format!("ConnectionHandle initialization error: {}", e))?;
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
        return Err("Path cannot be empty".to_string());
    }
    // Strip leading and trailing slashes
    let stripped = path_string.trim_matches('/');
    if stripped.is_empty() {
        return Err("Path cannot be empty after stripping slashes".to_string());
    }
    // Check for double slashes
    if stripped.contains("//") {
        return Err("Path cannot contain double slashes".to_string());
    }
    // Check for directory traversal
    if stripped.contains("..") {
        return Err("Path cannot contain '..' (directory traversal)".to_string());
    }
    // Validate characters: only alphanumeric, dash, underscore, and slash
    for c in stripped.chars() {
        if !c.is_alphanumeric() && c != '-' && c != '_' && c != '/' {
            return Err(format!("Path contains invalid character: '{}'", c));
        }
    }

    Ok(stripped.to_string())
}

pub fn prepare_socket_path(socket_path: &str) -> OrError<()> {
    let path = std::path::Path::new(socket_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "Agora socket preparation error. Failed to create parent: {}",
                e
            )
        })?;
    }
    if path.exists() {
        std::fs::remove_file(path).map_err(|e| {
            format!(
                "Agora socket preparation error. Failed to delete existing socket file: {}",
                e
            )
        })?;
    }
    Ok(())
}

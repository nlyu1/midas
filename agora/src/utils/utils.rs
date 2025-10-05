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
        self.addr.clone()
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

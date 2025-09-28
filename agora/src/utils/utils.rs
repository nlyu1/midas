use std::net::Ipv6Addr;
pub type OrError<T> = Result<T, String>;

pub fn parse_ipv6_str(str_addr: String) -> OrError<Ipv6Addr> {
    str_addr
        .parse::<Ipv6Addr>()
        .map_err(|e| format!("Failed to parse IPv6 address '{}': {}", str_addr, e))
}

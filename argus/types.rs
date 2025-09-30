use agora::utils::OrError; 

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct TradingSymbol(String); 

impl TradingSymbol {
    pub fn from_str(s: &str) -> OrError<Self> {
        if s.is_empty() {
            return Err("TradingSymbol cannot be empty".to_string());
        }
        Ok(Self(s.to_string()))
    }
    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct UtcNanoTimestamp(u64); 

impl UtcNanoTimestamp {
    pub fn from_nanos_since_epoch(nanos: u64) -> OrError<Self> {
        Ok(Self(nanos))
    }
    pub fn to_nanos_since_epoch(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Price(f64); 

impl Price {
    pub fn of_f64_exn(f: f64) -> OrError<Self> {
        if !f.is_finite() || f < 0.0 {
            return Err("Price needs to be finite and nonnegative".to_string());
        }
        Ok(Self(f))
    }
    pub fn to_f64(&self) -> f64 {
        self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct TradeSize(f64); 

impl TradeSize {
    pub fn of_f64_exn(f: f64) -> OrError<Self> {
        if !f.is_finite() || f <= 0.0 {
            return Err("TradeSize needs to be finite and positive".to_string());
        }
        Ok(Self(f))
    }
    pub fn to_f64(&self) -> f64 {
        self.0
    }
}

pub type PriceLevel = (Price, TradeSize);
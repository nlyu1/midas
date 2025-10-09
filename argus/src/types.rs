use agora::utils::OrError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Price(f64);

impl Price {
    pub fn from_f64(f: f64) -> OrError<Self> {
        if !f.is_finite() || f < 0.0 {
            return Err("Price needs to be finite and nonnegative".to_string());
        }
        Ok(Self(f))
    }
    pub fn to_f64(&self) -> f64 {
        self.0
    }
    pub fn from_string(s: String) -> OrError<Self> {
        let float: f64 = s.parse().map_err(|e| {
            format!(
                "Argus price conversion error: cannot interpret string {} as price: {}",
                s, e
            )
        })?;
        Self::from_f64(float)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeSize(f64);

impl TradeSize {
    pub fn from_f64(f: f64) -> OrError<Self> {
        if !f.is_finite() || f <= 0.0 {
            return Err("TradeSize needs to be finite and positive".to_string());
        }
        Ok(Self(f))
    }
    pub fn to_f64(&self) -> f64 {
        self.0
    }
    pub fn from_string(s: String) -> OrError<Self> {
        let float: f64 = s.parse().map_err(|e| {
            format!(
                "Argus price conversion error: cannot interpret string {} as trade size: {}",
                s, e
            )
        })?;
        Self::from_f64(float)
    }
}

pub type PriceLevel = (Price, TradeSize);

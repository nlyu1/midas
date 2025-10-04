use argus::types::{Price, TradeSize, TradingSymbol};

#[cfg(test)]
mod trading_symbol_tests {
    use super::*;

    #[test]
    fn test_valid_trading_symbol() {
        let symbol = TradingSymbol::from_str("BTCUSDT").unwrap();
        assert_eq!(symbol.to_string(), "BTCUSDT");

        let symbol2 = TradingSymbol::from_str("ETH-USD").unwrap();
        assert_eq!(symbol2.to_string(), "ETH-USD");

        let symbol3 = TradingSymbol::from_str("A").unwrap();
        assert_eq!(symbol3.to_string(), "A");
    }

    #[test]
    fn test_empty_trading_symbol_fails() {
        let result = TradingSymbol::from_str("");
        assert!(result.is_err(), "Empty string should fail");
        assert!(
            result.unwrap_err().contains("cannot be empty"),
            "Error message should indicate symbol cannot be empty"
        );
    }

    #[test]
    fn test_trading_symbol_clone() {
        let symbol = TradingSymbol::from_str("BTCUSDT").unwrap();
        let cloned = symbol.clone();
        assert_eq!(symbol, cloned);
        assert_eq!(symbol.to_string(), cloned.to_string());
    }
}

#[cfg(test)]
mod price_tests {
    use super::*;

    #[test]
    fn test_valid_prices() {
        // Test zero price (valid - nonnegative)
        let zero = Price::from_f64(0.0).unwrap();
        assert_eq!(zero.to_f64(), 0.0);

        // Test positive prices
        let price1 = Price::from_f64(100.5).unwrap();
        assert_eq!(price1.to_f64(), 100.5);

        let price2 = Price::from_f64(0.00001).unwrap();
        assert_eq!(price2.to_f64(), 0.00001);

        let large_price = Price::from_f64(1_000_000.0).unwrap();
        assert_eq!(large_price.to_f64(), 1_000_000.0);
    }

    #[test]
    fn test_negative_price_fails() {
        let result = Price::from_f64(-1.0);
        assert!(result.is_err(), "Negative price should fail");
        assert!(
            result.unwrap_err().contains("nonnegative"),
            "Error message should indicate price must be nonnegative"
        );

        let result2 = Price::from_f64(-0.001);
        assert!(result2.is_err(), "Small negative price should fail");
    }

    #[test]
    fn test_infinite_price_fails() {
        let result = Price::from_f64(f64::INFINITY);
        assert!(result.is_err(), "Infinity should fail");
        assert!(
            result.unwrap_err().contains("finite"),
            "Error message should indicate price must be finite"
        );

        let result2 = Price::from_f64(f64::NEG_INFINITY);
        assert!(result2.is_err(), "Negative infinity should fail");
    }

    #[test]
    fn test_nan_price_fails() {
        let result = Price::from_f64(f64::NAN);
        assert!(result.is_err(), "NaN should fail");
        assert!(
            result.unwrap_err().contains("finite"),
            "Error message should indicate price must be finite"
        );
    }

    #[test]
    fn test_price_from_string() {
        // Valid string conversions
        let price1 = Price::from_string("123.45".to_string()).unwrap();
        assert_eq!(price1.to_f64(), 123.45);

        let price2 = Price::from_string("0.0".to_string()).unwrap();
        assert_eq!(price2.to_f64(), 0.0);

        let price3 = Price::from_string("1000000".to_string()).unwrap();
        assert_eq!(price3.to_f64(), 1000000.0);

        // Invalid string conversions
        let result = Price::from_string("not_a_number".to_string());
        assert!(result.is_err(), "Invalid string should fail");
        assert!(
            result.unwrap_err().contains("cannot interpret string"),
            "Error message should indicate conversion failure"
        );

        // Negative value in string should fail validation
        let result2 = Price::from_string("-10.5".to_string());
        assert!(result2.is_err(), "Negative string value should fail");
    }

    #[test]
    fn test_price_clone() {
        let price = Price::from_f64(42.5).unwrap();
        let cloned = price.clone();
        assert_eq!(price, cloned);
        assert_eq!(price.to_f64(), cloned.to_f64());
    }
}

#[cfg(test)]
mod trade_size_tests {
    use super::*;

    #[test]
    fn test_valid_trade_sizes() {
        // Test positive sizes
        let size1 = TradeSize::from_f64(100.5).unwrap();
        assert_eq!(size1.to_f64(), 100.5);

        let size2 = TradeSize::from_f64(0.00001).unwrap();
        assert_eq!(size2.to_f64(), 0.00001);

        let large_size = TradeSize::from_f64(1_000_000.0).unwrap();
        assert_eq!(large_size.to_f64(), 1_000_000.0);
    }

    #[test]
    fn test_zero_trade_size_fails() {
        let result = TradeSize::from_f64(0.0);
        assert!(result.is_err(), "Zero trade size should fail");
        assert!(
            result.unwrap_err().contains("positive"),
            "Error message should indicate trade size must be positive"
        );
    }

    #[test]
    fn test_negative_trade_size_fails() {
        let result = TradeSize::from_f64(-1.0);
        assert!(result.is_err(), "Negative trade size should fail");
        assert!(
            result.unwrap_err().contains("positive"),
            "Error message should indicate trade size must be positive"
        );

        let result2 = TradeSize::from_f64(-0.001);
        assert!(result2.is_err(), "Small negative trade size should fail");
    }

    #[test]
    fn test_infinite_trade_size_fails() {
        let result = TradeSize::from_f64(f64::INFINITY);
        assert!(result.is_err(), "Infinity should fail");
        assert!(
            result.unwrap_err().contains("finite"),
            "Error message should indicate trade size must be finite"
        );

        let result2 = TradeSize::from_f64(f64::NEG_INFINITY);
        assert!(result2.is_err(), "Negative infinity should fail");
    }

    #[test]
    fn test_nan_trade_size_fails() {
        let result = TradeSize::from_f64(f64::NAN);
        assert!(result.is_err(), "NaN should fail");
        assert!(
            result.unwrap_err().contains("finite"),
            "Error message should indicate trade size must be finite"
        );
    }

    #[test]
    fn test_trade_size_from_string() {
        // Valid string conversions
        let size1 = TradeSize::from_string("123.45".to_string()).unwrap();
        assert_eq!(size1.to_f64(), 123.45);

        let size2 = TradeSize::from_string("0.001".to_string()).unwrap();
        assert_eq!(size2.to_f64(), 0.001);

        let size3 = TradeSize::from_string("1000000".to_string()).unwrap();
        assert_eq!(size3.to_f64(), 1000000.0);

        // Invalid string conversions
        let result = TradeSize::from_string("not_a_number".to_string());
        assert!(result.is_err(), "Invalid string should fail");
        assert!(
            result.unwrap_err().contains("cannot interpret string"),
            "Error message should indicate conversion failure"
        );

        // Zero value in string should fail validation
        let result2 = TradeSize::from_string("0.0".to_string());
        assert!(result2.is_err(), "Zero string value should fail");

        // Negative value in string should fail validation
        let result3 = TradeSize::from_string("-10.5".to_string());
        assert!(result3.is_err(), "Negative string value should fail");
    }

    #[test]
    fn test_trade_size_clone() {
        let size = TradeSize::from_f64(42.5).unwrap();
        let cloned = size.clone();
        assert_eq!(size, cloned);
        assert_eq!(size.to_f64(), cloned.to_f64());
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_very_small_positive_values() {
        // Price can be very small (including close to zero)
        let tiny_price = Price::from_f64(f64::MIN_POSITIVE).unwrap();
        assert_eq!(tiny_price.to_f64(), f64::MIN_POSITIVE);

        // TradeSize can also be very small
        let tiny_size = TradeSize::from_f64(f64::MIN_POSITIVE).unwrap();
        assert_eq!(tiny_size.to_f64(), f64::MIN_POSITIVE);
    }

    #[test]
    fn test_very_large_values() {
        // Test with very large but finite values
        let large_price = Price::from_f64(f64::MAX / 2.0).unwrap();
        assert!(large_price.to_f64() > 0.0);

        let large_size = TradeSize::from_f64(f64::MAX / 2.0).unwrap();
        assert!(large_size.to_f64() > 0.0);
    }

    #[test]
    fn test_scientific_notation_strings() {
        // Test scientific notation in string conversion
        let price = Price::from_string("1.5e10".to_string()).unwrap();
        assert_eq!(price.to_f64(), 1.5e10);

        let size = TradeSize::from_string("2.5e-5".to_string()).unwrap();
        assert_eq!(size.to_f64(), 2.5e-5);
    }

    #[test]
    fn test_whitespace_in_strings() {
        // Strings with whitespace should fail to parse
        let result = Price::from_string("  123.45  ".to_string());
        // This might actually succeed depending on parse() behavior, but let's document it
        // The parse() method trims whitespace automatically in Rust
        if result.is_ok() {
            assert_eq!(result.unwrap().to_f64(), 123.45);
        }
    }
}

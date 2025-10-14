use anyhow::Result;
use quick_xml::Reader;
use quick_xml::events::Event;

/// List all S3 object keys (files) under a prefix with automatic pagination.
/// Handles S3's 1000-key response limit by following NextMarker tokens.
/// Returns full file paths, e.g.: "data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2025-10-05.zip"
pub async fn get_all_keys(base_url: &str, prefix: &str) -> Result<Vec<String>> {
    let mut all_keys = Vec::new();
    let mut marker: Option<String> = None; // Pagination token for S3 API

    // Normalize prefix: S3 API expects "data/" prefix and trailing "/"
    let prefix = if !prefix.starts_with("data/") {
        format!("data/{}", prefix)
    } else {
        prefix.to_string()
    };
    let prefix = if !prefix.ends_with("/") {
        format!("{}/", prefix)
    } else {
        prefix
    };

    let client = reqwest::Client::new();

    // Pagination loop: S3 API returns max 1000 keys per request
    loop {
        // Build S3 query URL: prefix filters, delimiter prevents recursion
        let mut url = format!("{}?prefix={}&delimiter=/", base_url, prefix);
        if let Some(ref m) = marker {
            url.push_str(&format!("&marker={}", m)); // Resume from last key
        }

        let response = client.get(&url).send().await?;
        let xml_content = response.text().await?;

        // XML parsing state machine
        let mut reader = Reader::from_str(&xml_content);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        let mut current_tag = String::new();
        let mut is_truncated = false;
        let mut next_marker = None;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                    current_tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                }
                Ok(Event::Text(e)) => {
                    let text = String::from_utf8_lossy(&e.into_inner()).to_string();
                    match current_tag.as_str() {
                        "Key" => all_keys.push(text), // File path
                        "IsTruncated" => is_truncated = text.to_lowercase() == "true", // More pages?
                        "NextMarker" => next_marker = Some(text), // Next page token
                        _ => {}
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {:?}", e)),
                _ => {}
            }
            buf.clear();
        }

        // Continue pagination if more results exist
        if is_truncated && next_marker.is_some() {
            marker = next_marker;
        } else {
            break; // All pages fetched
        }
    }

    Ok(all_keys)
}

/// Get all trading pair subdirectories from S3 using CommonPrefixes (directory listing).
/// Uses delimiter="/" to list subdirectories without recursing into them.
/// Returns trading pairs, e.g.: ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
pub async fn get_all_trade_pairs(base_url: &str, prefix: &str) -> Result<Vec<String>> {
    let mut trade_pairs = std::collections::HashSet::new(); // Dedup
    let mut marker: Option<String> = None; // Pagination token

    // Normalize prefix format for S3 API
    let fixed_prefix = if !prefix.starts_with("data/") {
        format!("data/{}", prefix)
    } else {
        prefix.to_string()
    };
    let fixed_prefix = if !fixed_prefix.ends_with("/") {
        format!("{}/", fixed_prefix)
    } else {
        fixed_prefix
    };

    let client = reqwest::Client::new();

    // Pagination loop: fetch all subdirectories
    loop {
        let mut url = format!("{}?prefix={}&delimiter=/", base_url, fixed_prefix);
        if let Some(ref m) = marker {
            url.push_str(&format!("&marker={}", m));
        }

        let response = client.get(&url).send().await?;
        let xml_content = response.text().await?;

        // XML parsing with context tracking (inside CommonPrefixes vs top-level)
        let mut reader = Reader::from_str(&xml_content);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        let mut current_tag = String::new();
        let mut is_truncated = false;
        let mut next_marker = None;
        let mut in_common_prefixes = false; // Track XML nesting context

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "CommonPrefixes" {
                        in_common_prefixes = true; // Entering subdirectory listing section
                    }
                    current_tag = tag;
                }
                Ok(Event::End(e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "CommonPrefixes" {
                        in_common_prefixes = false; // Exiting subdirectory section
                    }
                }
                Ok(Event::Text(e)) => {
                    let text = String::from_utf8_lossy(&e.into_inner()).to_string();
                    match current_tag.as_str() {
                        "Prefix" if in_common_prefixes => {
                            // Parse subdirectory name from full S3 prefix
                            // Example: "data/spot/daily/trades/EOSUSDT/" -> "EOSUSDT"
                            if let Some(pair) = text.strip_prefix(&fixed_prefix) {
                                if let Some(pair) = pair.strip_suffix('/') {
                                    if !pair.is_empty() {
                                        trade_pairs.insert(pair.to_string());
                                    }
                                }
                            }
                        }
                        "IsTruncated" if !in_common_prefixes => {
                            is_truncated = text.to_lowercase() == "true"
                        }
                        "NextMarker" if !in_common_prefixes => next_marker = Some(text),
                        _ => {}
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {:?}", e)),
                _ => {}
            }
            buf.clear();
        }

        // Continue pagination if needed
        if is_truncated && next_marker.is_some() {
            marker = next_marker;
        } else {
            break;
        }
    }

    Ok(trade_pairs.into_iter().collect())
}

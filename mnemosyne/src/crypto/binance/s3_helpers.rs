use anyhow::Result;
use quick_xml::Reader;
use quick_xml::events::Event;

/// List all object keys from S3 bucket with pagination
pub async fn get_all_keys(base_url: &str, prefix: &str) -> Result<Vec<String>> {
    let mut all_keys = Vec::new();
    let mut marker: Option<String> = None;

    // Ensure prefix has proper format: starts with "data/" and ends with "/"
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

    loop {
        let mut url = format!("{}?prefix={}&delimiter=/", base_url, prefix);
        if let Some(ref m) = marker {
            url.push_str(&format!("&marker={}", m));
        }

        let response = client.get(&url).send().await?;
        let xml_content = response.text().await?;

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
                        "Key" => all_keys.push(text),
                        "IsTruncated" => is_truncated = text.to_lowercase() == "true",
                        "NextMarker" => next_marker = Some(text),
                        _ => {}
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {:?}", e)),
                _ => {}
            }
            buf.clear();
        }

        if is_truncated && next_marker.is_some() {
            marker = next_marker;
        } else {
            break;
        }
    }

    Ok(all_keys)
}

/// Get all trade pairs (directories) from S3 listing
pub async fn get_all_trade_pairs(base_url: &str, prefix: &str) -> Result<Vec<String>> {
    let mut trade_pairs = std::collections::HashSet::new();
    let mut marker: Option<String> = None;

    // Ensure prefix has proper format
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

    loop {
        let mut url = format!("{}?prefix={}&delimiter=/", base_url, fixed_prefix);
        if let Some(ref m) = marker {
            url.push_str(&format!("&marker={}", m));
        }

        let response = client.get(&url).send().await?;
        let xml_content = response.text().await?;

        let mut reader = Reader::from_str(&xml_content);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        let mut current_tag = String::new();
        let mut is_truncated = false;
        let mut next_marker = None;
        let mut in_common_prefixes = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "CommonPrefixes" {
                        in_common_prefixes = true;
                    }
                    current_tag = tag;
                }
                Ok(Event::End(e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "CommonPrefixes" {
                        in_common_prefixes = false;
                    }
                }
                Ok(Event::Text(e)) => {
                    let text = String::from_utf8_lossy(&e.into_inner()).to_string();
                    match current_tag.as_str() {
                        "Prefix" if in_common_prefixes => {
                            // Extract trade pair from full prefix
                            // e.g., "data/spot/daily/trades/EOSUSDT/" -> "EOSUSDT"
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

        if is_truncated && next_marker.is_some() {
            marker = next_marker;
        } else {
            break;
        }
    }

    Ok(trade_pairs.into_iter().collect())
}

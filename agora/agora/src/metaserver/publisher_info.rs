#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublisherInfo {
    name: String,
}

impl PublisherInfo {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub fn for_demo() -> Self {
        Self {
            name: "demo".to_string(),
        }
    }
}

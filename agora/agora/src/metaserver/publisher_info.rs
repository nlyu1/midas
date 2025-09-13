#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PublisherInfo {
    name: String,
}

impl PublisherInfo {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

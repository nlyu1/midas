use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

pub trait Agorable: Display + Serialize + for<'de> Deserialize<'de> + Send + 'static {}

impl Agorable for String {}
impl Agorable for i64 {}
impl Agorable for bool {}
impl Agorable for f64 {}
impl Agorable for f32 {}

// Newtype wrapper to make Option<T> implement Display and Agorable
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgorableOption<T>(pub Option<T>);

impl<T: Display> Display for AgorableOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(value) => write!(f, "{}", value),
            None => write!(f, "None"),
        }
    }
}

impl<T: Agorable> Agorable for AgorableOption<T> {}

// Convenience conversions
impl<T> From<Option<T>> for AgorableOption<T> {
    fn from(opt: Option<T>) -> Self {
        AgorableOption(opt)
    }
}

impl<T> From<AgorableOption<T>> for Option<T> {
    fn from(agorable_opt: AgorableOption<T>) -> Self {
        agorable_opt.0
    }
}

pub mod publisher;
pub mod subscriber;

use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub trait Agorable: Display + Serialize + for<'de> Deserialize<'de> {}

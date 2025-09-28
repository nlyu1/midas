use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub trait Agorable: Display + Serialize + for<'de> Deserialize<'de> {}

impl Agorable for String {}
impl Agorable for i64 {}
impl Agorable for bool {}
impl Agorable for f64 {}
impl Agorable for f32 {}

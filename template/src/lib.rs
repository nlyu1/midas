use pyo3::prelude::*;

/// Add two numbers together.
///
/// Args:
///     a: First number
///     b: Second number
///
/// Returns:
///     The sum of a and b
#[pyfunction]
fn add(a: i64, b: i64) -> PyResult<i64> {
    Ok(a + b)
}

/// Multiply two numbers together.
///
/// Args:
///     a: First number
///     b: Second number
///
/// Returns:
///     The product of a and b
#[pyfunction]
fn multiply(a: i64, b: i64) -> PyResult<i64> {
    Ok(a * b)
}

/// Greet someone by name.
///
/// Args:
///     name: The name to greet
///
/// Returns:
///     A greeting string
#[pyfunction]
fn greet(name: &str) -> PyResult<String> {
    Ok(format!("Hello, {}! Welcome to the Midas workspace.", name))
}

/// A Python module implemented in Rust.
/// 
/// This template provides basic examples of PyO3 functions.
/// Copy this directory and modify to create a new library in the workspace.
#[pymodule]
fn template(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add, m)?)?;
    m.add_function(wrap_pyfunction!(multiply, m)?)?;
    m.add_function(wrap_pyfunction!(greet, m)?)?;
    Ok(())
}

use crate::utils::{BlockingStreamIterator, OrError};
use pyo3::prelude::*;

#[macro_export]
macro_rules! create_py_result_iterator {
    ($name:ident, $type:ty) => {
        #[pyclass(unsendable)]
        pub struct $name {
            inner: BlockingStreamIterator<OrError<$type>>,
        }

        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__(&mut self) -> PyResult<Option<$type>> {
                match self.inner.next() {
                    Some(result) => match result {
                        Ok(value) => Ok(Some(value)),
                        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                    },
                    None => Ok(None),
                }
            }
        }

        impl $name {
            pub fn new(inner: BlockingStreamIterator<OrError<$type>>) -> Self {
                Self { inner }
            }
        }
    };
}

create_py_result_iterator!(PyResultIterator, String);

use pyo3::prelude::*;

#[pyclass]
struct GlareDBCursor;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sql(query: String) -> PyResult<GlareDBCursor> {
    let statements = sqlexec::parser::parse_sql(&query).unwrap();
    println!("{:#?}", statements);
    todo!()
}

#[pyfunction]
fn connect(url: &str) -> PyResult<GlareDBCursor> {
    let url = url.parse::<url::Url>().unwrap();
    todo!()
}

/// A Python module implemented in Rust.
#[pymodule]
fn glaredb(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sql, m)?)?;
    Ok(())
}

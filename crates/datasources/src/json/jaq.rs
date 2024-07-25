use jaq_interpret::{Filter, ParseCtx};
use memoize::memoize;

#[derive(Clone, Debug, thiserror::Error)]
pub enum JaqError {
    #[error("parse: {0}")]
    Parse(String),
}

#[memoize(Capacity: 256)]
pub fn compile_jaq_query(query: String) -> Result<Filter, JaqError> {
    let (f, errs) = jaq_parse::parse(&query, jaq_parse::main());
    if !errs.is_empty() {
        return Err(JaqError::Parse(
            errs.into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        ));
    }
    Ok(ParseCtx::new(Vec::new()).compile(f.unwrap()))
}

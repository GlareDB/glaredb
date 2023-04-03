#[derive(Debug, thiserror::Error)]
pub enum StripeError {
    #[error("Invalid Stripe API object: {0}")]
    InvalidApiObject(String),

    #[error(transparent)]
    Stripe(#[from] stripe::StripeError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
}

pub type Result<T, E = StripeError> = std::result::Result<T, E>;

mod errors;
mod session;

use napi_derive::napi;

#[napi]
pub fn connect() -> napi::Result<session::NodeSession> {
    session::connect()
}

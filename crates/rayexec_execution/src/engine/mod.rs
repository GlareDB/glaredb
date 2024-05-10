pub mod materialize;
pub mod modify;
pub mod result_stream;
pub mod session;
pub mod vars;

use rayexec_error::Result;
use session::Session;

use crate::scheduler::Scheduler;

#[derive(Debug)]
pub struct Engine {
    scheduler: Scheduler,
}

impl Engine {
    pub fn try_new() -> Result<Self> {
        Ok(Engine {
            scheduler: Scheduler::try_new()?,
        })
    }

    pub fn new_session(&self) -> Result<Session> {
        Ok(Session::new(self.scheduler.clone()))
    }
}

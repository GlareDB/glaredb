use std::fmt;
use std::time::Duration;

use crate::optimizer::OptimizerProfileData;

/// Profile data for logical and physical planning.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlanningProfileData {
    pub resolve_step: Option<Duration>,
    pub bind_step: Option<Duration>,
    pub plan_logical_step: Option<Duration>,
    pub plan_intermediate_step: Option<Duration>,
    pub plan_executable_step: Option<Duration>,
    pub optimizer_step: Option<OptimizerProfileData>,
}

impl fmt::Display for PlanningProfileData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[allow(clippy::write_literal)]
        writeln!(f, "{:<30} {:>14}", "Step", "Elapsed (micro)")?;

        let write_step = &mut |f: &mut fmt::Formatter, step, dur: Option<Duration>| match dur {
            Some(dur) => writeln!(f, "{:<30} {:>14}", step, dur.as_micros()),
            None => writeln!(f, "{:<30} {:>14}", step, "NA"),
        };

        write_step(f, "resolve_step", self.resolve_step)?;
        write_step(f, "bind_step", self.bind_step)?;
        write_step(f, "plan_logical_step", self.plan_logical_step)?;

        match &self.optimizer_step {
            Some(opt) => {
                write_step(f, "optimizer_step", Some(opt.total))?;
                for rule in &opt.timings {
                    writeln!(f, "    {:<26} {:>14}", rule.0, rule.1.as_micros())?;
                }
            }
            None => write_step(f, "optimizer_step", None)?,
        }

        write_step(f, "plan_intermediate_step", self.plan_intermediate_step)?;
        write_step(f, "plan_executable_step", self.plan_executable_step)?;

        Ok(())
    }
}

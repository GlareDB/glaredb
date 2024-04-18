use arrow::compute::kernels::aggregate;
use arrow_array::{cast::AsArray, types::Float64Type, Array, ArrayRef, Float64Array};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

#[derive(Debug)]
pub struct AvgAccumulator {
    sum: f64,
    count: u64,
}

// impl Accumulator for AvgAccumulator {
//     fn update(&mut self, vals: &[ArrayRef]) -> Result<()> {
//         let vals = vals[0]
//             .as_primitive_opt::<Float64Type>()
//             .ok_or_else(|| RayexecError::new("failed to downcast to f64"))?;
//         if let Some(sum) = aggregate::sum(vals) {
//             self.sum += sum;
//             self.count += (vals.len() - vals.null_count()) as u64;
//         }
//         Ok(())
//     }

//     fn combine(&mut self, other: &mut dyn std::any::Any) -> Result<()> {
//         let other = other
//             .downcast_mut::<Self>()
//             .ok_or_else(|| RayexecError::new("failed to downcast to avg state"))?;
//         self.sum += other.sum;
//         self.count += other.count;
//         Ok(())
//     }

//     fn finalize(&mut self) -> Result<ArrayRef> {
//         Ok(if self.count == 0 {
//             Arc::new(Float64Array::new_null(1))
//         } else {
//             Arc::new(Float64Array::from(vec![self.sum / (self.count as f64)]))
//         })
//     }
// }

use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_error::Result;

#[derive(Debug, Default)]
pub struct CovarSampFloat64 {
    count: usize,
    meanx: f64,
    meany: f64,
    co_moment: f64,
}

impl AggregateState<(f64, f64), f64> for CovarSampFloat64 {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        let count = self.count + other.count;
        let meanx =
            (other.count as f64 * other.meanx + self.count as f64 * self.meanx) / count as f64;
        let meany =
            (other.count as f64 * other.meany + self.count as f64 * self.meany) / count as f64;

        let deltax = self.meanx - other.meanx;
        let deltay = self.meany - other.meany;

        self.co_moment = other.co_moment
            + self.co_moment
            + deltax * deltay * other.count as f64 * self.count as f64 / count as f64;
        self.meanx = meanx;
        self.meany = meany;
        self.count = count;

        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        let x = input.1;
        let y = input.0;

        let n = self.count as f64;
        self.count += 1;

        let dx = x - self.meanx;
        let meanx = self.meanx + dx / n;

        let dy = y - self.meany;
        let meany = self.meany + dy / n;

        let co_moment = self.co_moment + dx * (y - meany);

        self.meanx = meanx;
        self.meany = meany;
        self.co_moment = co_moment;

        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        Ok((self.co_moment / (self.count - 1) as f64, true))
    }
}

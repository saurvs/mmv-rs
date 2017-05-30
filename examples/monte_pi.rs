extern crate mmv;
extern crate rand;

use mmv::*;
use rand::distributions::{IndependentSample, Range};
use std::thread;
use std::time::Duration;
use std::env;

/*
 * usage: ./monte_pi <path-to-mmv-file>
 */

fn main() {
    let mut trials = Metric::new(
        "trials", 1, MetricSem::Counter, 0, 0, MetricType::I64(0),
        "Trials",
        "Number of Monte Carlo trials");
    let mut pi = Metric::new(
        "pi", 1, MetricSem::Instant, 0, 0, MetricType::F64(0.0),
        "Estimated Pi",
        "Estimated value of Pi through Monte Carlo trials");

    let mut args = env::args();
    let path = args.nth(1).unwrap();
    let mmv = MMV::new(&path, MMVFlags::empty(), 0);
    mmv.map(&mut [&mut trials, &mut pi]);

    let mut in_circle = 0;
    let between = Range::new(-1.0, 1.0);
    let mut rng = rand::thread_rng();

    for i in 1..1000001 {
        trials.set_val(MetricType::I64(i));

        let x = between.ind_sample(&mut rng);
        let y = between.ind_sample(&mut rng);
        if x*x + y*y <= 1.0 { in_circle += 1; }
        pi.set_val(MetricType::F64(
            (in_circle as f64)/(i as f64) * 4.0
        ));

        thread::sleep(Duration::from_millis(100));
    }
}
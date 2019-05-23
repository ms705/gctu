extern crate gctu;

use gctu::task_usage::{self, TaskUsageRecord};
use hdrhistogram::Histogram;

fn print(label: &str, hist: &hdrhistogram::Histogram<u64>) {
    println!("\n# {} samples: {}", label, hist.len());
    println!("median: {}", hist.value_at_quantile(0.5) as f64 / 10000.0);
    println!("90th percentile: {}", hist.value_at_quantile(0.9) as f64 / 10000.0);
    println!("99th percentile: {}", hist.value_at_quantile(0.99) as f64 / 10000.0);
}


fn main() -> std::io::Result<()> {
    let mut canon = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut assigned = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut unmapped_pc = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut total_pc = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();

    let f = "/data/google-trace/clusterdata-2011-2/task_usage/part-00000-of-00500.csv";

    task_usage::for_each_in_file(f, |task_usage: TaskUsageRecord| -> std::io::Result<()> {
        if let Some(cmu) = task_usage.canonical_mem_usage {
            canon.record((cmu * 10000.0) as u64).expect("recording to histogram failed");
        }
        if let Some(amu) = task_usage.assigned_mem_usage {
            assigned.record((amu * 10000.0) as u64).expect("recording to histogram failed");
        }
        if let Some(upc) = task_usage.unmapped_page_cache {
            unmapped_pc.record((upc * 10000.0) as u64).expect("recording to histogram failed");
        }
        if let Some(tpc) = task_usage.total_page_cache {
            total_pc.record((tpc * 10000.0) as u64).expect("recording to histogram failed");
        }
    
        Ok(())
    }).expect("failed to process task usage data");

    print("canon. mem usage", &canon);
    print("assigned mem usage", &assigned);
    print("unmapped page cache", &unmapped_pc);
    print("total page cache", &total_pc);

    Ok(())
}

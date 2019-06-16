extern crate gctu;

use gctu::machine_events::{self, MachineEvent, MachineEventType};
use gctu::task_usage::{self, TaskUsageRecord};
use hdrhistogram::Histogram;
use std::collections::HashMap;

static TRACE_START_TIME: u64 = 600_000_000u64;

fn print(label: &str, hist: &hdrhistogram::Histogram<u64>) {
    println!("\n# {} samples: {}", label, hist.len());
    println!("median: {}", hist.value_at_quantile(0.5) as f64 / 10000.0);
    println!(
        "90th percentile: {}",
        hist.value_at_quantile(0.9) as f64 / 10000.0
    );
    println!(
        "99th percentile: {}",
        hist.value_at_quantile(0.99) as f64 / 10000.0
    );
}

fn main() -> std::io::Result<()> {
    use clap::{App, Arg};

    let args = App::new("pagecache")
        .version("0.1")
        .about("Page cache usage analysis")
        .arg(
            Arg::with_name("trace-path")
                .short("t")
                .long("trace-path")
                .value_name("PATH")
                .default_value("/data/google-trace/clusterdata-2011-2")
                .help("Path to the Google cluster trace"),
        )
        .arg(
            Arg::with_name("trace-fraction")
                .short("f")
                .long("fraction")
                .value_name("N")
                .default_value("100")
                .conflicts_with("inital-state-only")
                .help("Fraction of trace to process"),
        )
        .arg(
            Arg::with_name("initial-state-only")
                .long("initial-state-only")
                .conflicts_with("trace-fraction")
                .help("Indicates to process only the initial cluster state"),
        )
        .get_matches();

    let trace_path = args.value_of("trace-path").unwrap();
    let initial_only = args.is_present("initial-state-only");

    let mut canon = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut assigned = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut unmapped_pc = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut total_pc = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();

    let mut active_machines = HashMap::new();

    let mf = format!("{}/machine_events/part-00000-of-00001.csv", trace_path);
    machine_events::for_each_in_file(&mf, |machine_event: MachineEvent| -> std::io::Result<()> {
        if initial_only && machine_event.time > TRACE_START_TIME {
            // XXX(malte): return and indication to stop iterating
            return Ok(());
        }

        if machine_event.event_type == MachineEventType::Add {
            if let Some(mf) = machine_event.memory {
                active_machines.insert(machine_event.machine_id, mf);
            }
        }

        Ok(())
    })
    .expect("failed to process machine events");

    let f = format!("{}/task_usage/part-00000-of-00500.csv", trace_path);

    task_usage::for_each_in_file(&f, |task_usage: TaskUsageRecord| -> std::io::Result<()> {
        if initial_only && task_usage.start_time > TRACE_START_TIME {
            // XXX(malte): return an indication to stop iterating
            return Ok(());
        }

        if let Some(mem_frac) = active_machines.get(&task_usage.machine_id) {
            if let Some(cmu) = task_usage.canonical_mem_usage {
                let cmu = cmu / mem_frac;
                canon
                    .record((cmu * 10000.0) as u64)
                    .expect("recording to histogram failed");
            }
            if let Some(amu) = task_usage.assigned_mem_usage {
                let amu = amu / mem_frac;
                assigned
                    .record((amu * 10000.0) as u64)
                    .expect("recording to histogram failed");
            }
            if let Some(upc) = task_usage.unmapped_page_cache {
                let upc = upc / mem_frac;
                unmapped_pc
                    .record((upc * 10000.0) as u64)
                    .expect("recording to histogram failed");
            }
            if let Some(tpc) = task_usage.total_page_cache {
                let tpc = tpc / mem_frac;
                total_pc
                    .record((tpc * 10000.0) as u64)
                    .expect("recording to histogram failed");
            }
        } else {
            println!("Machine {} does not exist", task_usage.machine_id);
        }

        Ok(())
    })
    .expect("failed to process task usage data");

    print("canon. mem usage", &canon);
    print("assigned mem usage", &assigned);
    print("unmapped page cache", &unmapped_pc);
    print("total page cache", &total_pc);

    Ok(())
}

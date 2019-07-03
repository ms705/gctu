extern crate gctu;

use gctu::common::{self, TRACE_START_TIME};
use gctu::job_events::{self, JobEvent, JobEventType};
use gctu::machine_events::{self, MachineEvent, MachineEventType};
use gctu::task_usage::{TaskUsageIterator, TaskUsageRecord};
use hdrhistogram::Histogram;
use std::collections::HashMap;

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

    if !initial_only {
        unimplemented!("processing beyond the initial state is not currently supported");
    }

    let mut canon = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut assigned = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut unmapped_pc = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();
    let mut total_pc = Histogram::<u64>::new_with_bounds(1, 10000, 2).unwrap();

    let mut active_jobs = HashMap::new();
    let mut active_machines = HashMap::new();

    let mf = format!("{}/job_events/part-00000-of-00500.csv", trace_path);
    job_events::for_each_in_file(&mf, |job_event: JobEvent| -> std::io::Result<()> {
        if initial_only && job_event.time > TRACE_START_TIME {
            // XXX(malte): return and indication to stop iterating
            return Ok(());
        }

        if job_event.event_type == JobEventType::Submit {
            active_jobs.insert(job_event.job_id, job_event);
        }

        Ok(())
    })
    .expect("failed to process machine events");

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

    let mut pcache_by_machine = HashMap::new();
    let mut pcache_by_job = HashMap::new();

    let f = format!("{}/task_usage/part-00000-of-00500.csv", trace_path);
    let usage_iter = TaskUsageIterator::new(&f);

    let num_tasks = usage_iter
        .take_while(|rec| {
            let task_usage = rec.as_ref().expect("failed to parse task record!");
            // stop iterating once we're no longer interested
            initial_only && task_usage.start_time > TRACE_START_TIME
        })
        .map(|rec| {
            let task_usage: TaskUsageRecord = rec.expect("failed to parse task record!");

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
                    let tpc_on_machine = pcache_by_machine
                        .entry(task_usage.machine_id)
                        .or_insert(0.0);
                    *tpc_on_machine += tpc;
                    let (tasks, job_tpc) = pcache_by_job
                        .entry(task_usage.job_id)
                        .or_insert((0, Vec::new()));
                    *tasks += 1;
                    job_tpc.push(tpc);
                }
            } else {
                eprintln!(
                    "task {}:{}'s machine {} does not exist",
                    task_usage.job_id, task_usage.task_index, task_usage.machine_id
                );
            }
        })
        .count();

    println!("processed {} tasks\n", num_tasks);

    print("canon. mem usage", &canon);
    print("assigned mem usage", &assigned);
    print("unmapped page cache", &unmapped_pc);
    print("total page cache", &total_pc);

    println!("\n\npage cache use by job:");
    println!("job ID, scheduling class, number of tasks, avg. page cache use");
    for (j, (tasks, tpc)) in pcache_by_job {
        let sum: f64 = tpc.iter().sum();
        if let Some(j) = active_jobs.get(&j) {
            let class = j
                .scheduling_class
                .as_ref()
                .unwrap_or(&common::SchedulingClass::Unknown);
            println!("{},{:?},{},{}", j.job_id, class, tasks, sum / tasks as f64);
        } else {
            eprintln!("skipping unknown job {}", j);
        }
    }

    Ok(())
}

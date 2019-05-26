// 1,start time,INTEGER,YES
// 2,end time,INTEGER,YES
// 3,job ID,INTEGER,YES
// 4,task index,INTEGER,YES
// 5,machine ID,INTEGER,YES
// 6,CPU rate,FLOAT,NO
// 7,canonical memory usage,FLOAT,NO
// 8,assigned memory usage,FLOAT,NO
// 9,unmapped page cache,FLOAT,NO
// 10,total page cache,FLOAT,NO
// 11,maximum memory usage,FLOAT,NO
// 12,disk I/O time,FLOAT,NO
// 13,local disk space usage,FLOAT,NO
// 14,maximum CPU rate,FLOAT,NO
// 15,maximum disk IO time,FLOAT,NO
// 16,cycles per instruction,FLOAT,NO
// 17,memory accesses per instruction,FLOAT,NO
// 18,sample portion,FLOAT,NO
// 19,aggregation type,BOOLEAN,NO
// 20,sampled CPU usage,FLOAT,NO
#[derive(Debug, Deserialize)]
pub struct TaskUsageRecord {
    pub start_time: u64,
    pub end_time: u64,
    pub job_id: u64,
    pub task_index: u64,
    pub machine_id: u64,
    pub cpu_rate: Option<f64>,
    pub canonical_mem_usage: Option<f64>,
    pub assigned_mem_usage: Option<f64>,
    pub unmapped_page_cache: Option<f64>,
    pub total_page_cache: Option<f64>,
    pub max_mem_usage: Option<f64>,
    pub disk_io_time: Option<f64>,
    pub local_disk_space: Option<f64>,
    pub max_cpu_rate: Option<f64>,
    pub max_disk_io_tim: Option<f64>,
    pub cpi: Option<f64>,
    pub mapi: Option<f64>,
    pub sample_portion: Option<f64>,
    pub agg_type: Option<u8>, // bool
    pub sampled_cpu_usage: Option<f64>,
}

pub fn for_each_in_file<F>(file: &str, mut f: F) -> std::io::Result<()>
where
    F: FnMut(TaskUsageRecord) -> std::io::Result<()>,
{
    use std::fs::File;

    let file = File::open(file)?;
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.records() {
        let sr = result?;
        let task_usage: TaskUsageRecord = sr.deserialize(None)?;

        f(task_usage)?
    }
    Ok(())
}

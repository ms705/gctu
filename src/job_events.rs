// 1,time,INTEGER,YES
// 2,missing info,INTEGER,NO
// 3,job ID,INTEGER,YES
// 4,event type,INTEGER,YES
// 5,user,STRING_HASH,NO
// 6,scheduling class,INTEGER,NO
// 7,job name,STRING_HASH,NO
// 8,logical job name,STRING_HASH,NO
#[derive(Debug, Deserialize)]
pub struct JobEvent {
    pub time: u64,
    pub missing_info: Option<u8>,
    pub job_id: u64,
    pub event_type: u8,
    pub user: Option<String>,
    pub scheduling_class: Option<u8>,
    pub job_name: Option<String>,
    pub logical_job_name: Option<String>,
}

pub fn for_each_in_file<F>(file: &str, mut f: F) -> std::io::Result<()>
where
    F: FnMut(JobEvent) -> std::io::Result<()>,
{
    use std::fs::File;

    let file = File::open(file)?;
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.records() {
        let sr = result?;
        let job_event: JobEvent = sr.deserialize(None)?;

        f(job_event)?
    }
    Ok(())
}

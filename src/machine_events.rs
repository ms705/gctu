// 1,time,INTEGER,YES
// 2,machine ID,INTEGER,YES
// 3,event type,INTEGER,YES
// 4,platform ID,STRING_HASH,NO
// 5,CPUs,FLOAT,NO
// 6,Memory,FLOAT,NO
#[derive(Debug, Deserialize)]
pub struct MachineEvent {
    pub time: u64,
    pub machine_id: u64,
    pub event_type: u8,
    pub platform_id: Option<String>,
    pub cpus: Option<f64>,
    pub memory: Option<f64>,
}

pub fn for_each_in_file<F>(file: &str, mut f: F) -> std::io::Result<()>
where
    F: FnMut(MachineEvent) -> std::io::Result<()>,
{
    use std::fs::File;

    let file = File::open(file)?;
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.records() {
        let sr = result?;
        let machine_event: MachineEvent = sr.deserialize(None)?;

        f(machine_event)?
    }
    Ok(())
}

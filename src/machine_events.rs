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
    pub event_type: MachineEventType,
    pub platform_id: Option<String>,
    pub cpus: Option<f64>,
    pub memory: Option<f64>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum MachineEventType {
    /// ADD (0): a machine became available to the cluster
    Add,
    /// REMOVE (1): a machine was removed from the cluster
    Remove,
    /// UPDATE (2): a machine available to the cluster had its available resources changed
    Update,
}

// pub fn for_each<F>(trace_path: &str, mut f: F) -> std::io::Result<()>
// where
//     F: FnMut(MachineEvent) -> std::io::Result<()>,
// {
//     for i in 0..499 {
//         let tf = format!("{}/machine_events/part-{:05}-of-00500.csv", trace_path, i);
//         for_each_in_file(&tf, f)?
//     }
//     Ok(())
// }

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

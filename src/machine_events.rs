use crate::iter::TraceFileIterator;

pub(crate) static MACHINE_EVENT_FILE_COUNT: usize = 1;

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

#[derive(Debug, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum MachineEventType {
    /// ADD (0): a machine became available to the cluster
    Add = 0,
    /// REMOVE (1): a machine was removed from the cluster
    Remove = 1,
    /// UPDATE (2): a machine available to the cluster had its available resources changed
    Update = 2,
}

impl Into<MachineEventType> for &str {
    fn into(self) -> MachineEventType {
        if self == "0" {
            MachineEventType::Add
        } else if self == "1" {
            MachineEventType::Remove
        } else if self == "2" {
            MachineEventType::Update
        } else {
            unreachable!()
        }
    }
}

pub struct MachineEventIterator {
    file_iter: TraceFileIterator<MachineEvent>,
}

impl MachineEventIterator {
    pub fn new(trace_path: &str) -> Self {
        let fp = format!("{}/machine_events/", trace_path);
        MachineEventIterator {
            file_iter: TraceFileIterator::new(&fp, MACHINE_EVENT_FILE_COUNT),
        }
    }
}

impl Iterator for MachineEventIterator {
    type Item = Result<MachineEvent, csv::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.file_iter.next()
    }
}

use crate::common::{MissingInfo, SchedulingClass};

// 1,time,INTEGER,YES
// 2,missing info,INTEGER,NO
// 3,job ID,INTEGER,YES
// 4,task index,INTEGER,YES
// 5,machine ID,INTEGER,NO
// 6,event type,INTEGER,YES
// 7,user,STRING_HASH,NO
// 8,scheduling class,INTEGER,NO
// 9,priority,INTEGER,YES
// 10,CPU request,FLOAT,NO
// 11,memory request,FLOAT,NO
// 12,disk space request,FLOAT,NO
// 13,different machines restriction,BOOLEAN,NO
#[derive(Debug, Deserialize)]
pub struct TaskEvent {
    pub time: u64,
    pub missing_info: Option<MissingInfo>,
    pub job_id: u64,
    pub task_index: u64,
    pub machine_id: Option<u64>,
    pub event_type: TaskEventType,
    pub user: Option<String>,
    pub scheduling_class: Option<SchedulingClass>,
    pub priority: u8,
    pub cpu_request: Option<f64>,
    pub memory_request: Option<f64>,
    pub disk_space_request: Option<f64>,
    pub different_machines_restrict: Option<u8>, // bool
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum TaskEventType {
    /// SUBMIT (0): A task or job became eligible for scheduling.
    Submit,
    /// SCHEDULE (1): A job or task was scheduled on a machine. (It may not start running
    /// immediately due to code-shipping time, etc.) For jobs, this occurs the first time any task
    /// of the job is scheduled on a machine.
    Schedule,
    /// EVICT(2): A task or job was descheduled because of a higher priority task or job, because
    /// the scheduler overcommitted and the actual demand exceeded the machine capacity, because
    /// the machine on which it was running became unusable (e.g. taken offline for repairs), or
    /// because a disk holding the task’s data was lost.
    Evict,
    /// FAIL(3): A task or job was descheduled (or, in rare cases, ceased to be eligible for
    /// scheduling while it was pending) due to a task failure.
    Fail,
    /// FINISH(4): A task or job completed normally.
    Finish,
    /// KILL(5): A task or job was cancelled by the user or a driver program or because another job
    /// or task on which this job was dependent died.
    Kill,
    /// LOST(6): A task or job was presumably terminated, but a record indicating its termination
    /// was missing from our source data.
    Lost,
    /// UPDATE_PENDING(7): A task or job’s scheduling class, resource requirements, or constraints
    /// were updated while it was waiting to be scheduled.
    UpdatePending,
    /// UPDATE_RUNNING(8): A task or job’s scheduling class, resource requirements, or constraints
    /// were updated while it was scheduled.
    UpdateRunning,
}

pub fn for_each_in_file<F>(file: &str, mut f: F) -> std::io::Result<()>
where
    F: FnMut(TaskEvent) -> std::io::Result<()>,
{
    use std::fs::File;

    let file = File::open(file)?;
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.records() {
        let sr = result?;
        let task_event: TaskEvent = sr.deserialize(None)?;

        f(task_event)?
    }
    Ok(())
}

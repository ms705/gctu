use crate::common::{MissingInfo, SchedulingClass};

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
    pub missing_info: Option<MissingInfo>,
    pub job_id: u64,
    pub event_type: JobEventType,
    pub user: Option<String>,
    pub scheduling_class: Option<SchedulingClass>,
    pub job_name: Option<String>,
    pub logical_job_name: Option<String>,
}

#[derive(Debug, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum JobEventType {
    /// SUBMIT (0): A task or job became eligible for scheduling.
    Submit = 0,
    /// SCHEDULE (1): A job or task was scheduled on a machine. (It may not start running
    /// immediately due to code-shipping time, etc.) For jobs, this occurs the first time any task
    /// of the job is scheduled on a machine.
    Schedule = 1,
    /// EVICT(2): A task or job was descheduled because of a higher priority task or job, because
    /// the scheduler overcommitted and the actual demand exceeded the machine capacity, because
    /// the machine on which it was running became unusable (e.g. taken offline for repairs), or
    /// because a disk holding the task’s data was lost.
    Evict = 2,
    /// FAIL(3): A task or job was descheduled (or, in rare cases, ceased to be eligible for
    /// scheduling while it was pending) due to a task failure.
    Fail = 3,
    /// FINISH(4): A task or job completed normally.
    Finish = 4,
    /// KILL(5): A task or job was cancelled by the user or a driver program or because another job
    /// or task on which this job was dependent died.
    Kill = 5,
    /// LOST(6): A task or job was presumably terminated, but a record indicating its termination
    /// was missing from our source data.
    Lost = 6,
    /// UPDATE_PENDING(7): A task or job’s scheduling class, resource requirements, or constraints
    /// were updated while it was waiting to be scheduled.
    UpdatePending = 7,
    /// UPDATE_RUNNING(8): A task or job’s scheduling class, resource requirements, or constraints
    /// were updated while it was scheduled.
    UpdateRunning = 8,
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

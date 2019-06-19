#[derive(Debug, Deserialize)]
pub enum MissingInfo {
    /// (0): "we did not find a record representing the given event, but a later snapshot of the job
    /// or task state indicated that the transition must have occurred. The timestamp of the
    /// synthesized event is the timestamp of the snapshot."
    SnapshotButNoTransition,
    /// (1): "we did not find a record representing the given termination event, but the job or task
    /// disappeared from later snapshots of cluster states, so it must have been terminated. The
    /// timestamp of the synthesized event is a pessimistic upper bound on its actual termination
    /// time assuming it could have legitimately been missing from one snapshot."
    NoSnapshotOrTransition,
    /// (2): "we did not find a record representing the creation of the given task or job. In this
    /// case, we may be missing metadata (job name, resource requests, etc.) about the job or task
    /// and we may have placed SCHEDULE or SUBMIT events latter than they actually are."
    ExistsButNoCreation,
}

impl Into<MissingInfo> for &str {
    fn into(self) -> MissingInfo {
        if self == "0" {
            MissingInfo::SnapshotButNoTransition
        } else if self == "1" {
            MissingInfo::NoSnapshotOrTransition
        } else if self == "2" {
            MissingInfo::ExistsButNoCreation
        } else {
            unreachable!()
        }
    }
}

#[derive(Debug, Deserialize)]
pub enum SchedulingClass {
    /// "a non-production task (e.g., development, non-business-critical analyses, etc.)"
    Class0,
    Class1,
    Class2,
    /// "a more latency-sensitive task (e.g., serving revenue-generating user requests)"
    Class3,
}

impl Into<SchedulingClass> for &str {
    fn into(self) -> SchedulingClass {
        if self == "0" {
            SchedulingClass::Class0
        } else if self == "1" {
            SchedulingClass::Class1
        } else if self == "2" {
            SchedulingClass::Class2
        } else if self == "3" {
            SchedulingClass::Class3
        } else {
            unreachable!()
        }
    }
}

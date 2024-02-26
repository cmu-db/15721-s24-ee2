pub enum OperatorResultType {
    NeedMoreInput,
    HaveMoreOutput,
    Finished,
    Blocked,
}

pub enum SourceResultType {
    HaveMoreOutput,
    Finished,
    Blocked,
}

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
    Blocked,
}

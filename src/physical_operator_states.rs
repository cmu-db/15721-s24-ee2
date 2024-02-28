pub struct OperatorState {
    //TODO
}

pub struct GlobalOperatorState {
    //TODO
}

pub struct GlobalSourceState {
    //TODO
}

pub struct LocalSourceState {
    //TODO
}

pub struct GlobalSinkState {
    //TODO
}

pub struct LocalSinkState {
    //TODO
}

pub struct OperatorSourceInput<'a, 'b> {
    pub global_state: &'a GlobalSourceState,
    pub local_state: &'b LocalSourceState,
    // interrupt_state: &'c InterruptState,
    //TODO
}

pub struct OperatorSinkInput<'a, 'b> {
    pub global_state: &'a GlobalSinkState,
    pub local_state: &'b LocalSinkState,
    // interrupt_state : &'c InterruptState,
}

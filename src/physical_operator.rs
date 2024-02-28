use crate::common::enums::operator_result_type::{
    OperatorResultType, SinkResultType, SourceResultType,
};
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::common::types::data_chunk::DataChunk;
use crate::common::types::LogicalType;
use crate::execution_context::ExecutionContext;
use crate::physical_operator_states::{
    GlobalOperatorState, GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState,
    OperatorSinkInput, OperatorSourceInput, OperatorState,
};

pub trait PhysicalOperator {
    //TODO getTypes etc
    fn get_types(&self) -> Vec<LogicalType>;
    fn is_sink(&self) -> bool;
    // fn is_source(&self) -> bool;
}

//Operators that implement Sink trait consume data
pub trait Sink: PhysicalOperator {
    // Sink method is called constantly with new input, as long as new input is available
    fn sink(
        &self,
        context: &ExecutionContext,
        chunk: &DataChunk,
        input: &OperatorSinkInput,
    ) -> SinkResultType;

    fn get_local_sink_state(&self) -> Box<LocalSinkState>;

    // virtual unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const;
    //TODO there are many more things here
}

//Operators that implement Source trait emit data
pub trait Source: PhysicalOperator {
    fn get_local_source_state(
        &self,
        // context: &ExecutionContext,
        global_operator_state: Option<&GlobalSourceState>,
    ) -> Box<LocalSourceState>;

    // fn get_global_source_state(context: &ClientContext) -> Box<GlobalSourceState>;
    fn get_data(
        &self,
        // context: &ExecutionContext,
        chunk: &DataChunk,
        input: &OperatorSourceInput,
    ) -> SourceResultType;

    //TODO there are more stuff here
}

//Physical operators that implement the Operator trait process data
pub trait IntermediateOperator: PhysicalOperator {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(
        &self,
        context: &ExecutionContext,
        input: &DataChunk,
        chunk: &DataChunk,
        gstate: &GlobalOperatorState,
        state: &OperatorState,
    ) -> OperatorResultType;
    fn get_operator_state(
        &self,
        // context: &ExecutionContext
    ) -> Box<OperatorState>;
    // fn get_global_operator_state(context: &ExecutionContext) -> Box<GlobalOperatorState>;

    //TODO there are more stuff here
}

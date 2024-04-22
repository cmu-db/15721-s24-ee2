pub enum PhysicalOperatorType {
    Scan,
    Filter,
    Projection,
    HashAggregate,
    HashJoinBuild,
    HashJoinProbe,
    Limit,
    Sort,
}

//returns a string representation given an operator type
//useful for debugging
pub fn physical_operator_to_string(typ: &PhysicalOperatorType) -> &'static str {
    match typ {
        PhysicalOperatorType::Scan => "Scan",
        PhysicalOperatorType::Filter => "Filter",
        PhysicalOperatorType::Projection => "Projection",
        PhysicalOperatorType::HashAggregate => "HashAggregate",
        PhysicalOperatorType::HashJoinBuild => "HashJoinBuild",
        PhysicalOperatorType::HashJoinProbe => "HashJoinProbe",
        PhysicalOperatorType::Limit => "Limit",
        PhysicalOperatorType::Sort => "Sort",
    }
}

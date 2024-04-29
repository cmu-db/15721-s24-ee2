pub enum PhysicalOperatorType {
    Scan,
    ScanIntermediates,
    Filter,
    Projection,
    HashAggregate,
    HashJoinBuild,
    HashJoinProbe,
    Limit,
    Sort,
    Placeholder,
}

//returns a string representation given an operator type
//useful for debugging
pub fn physical_operator_to_string(typ: &PhysicalOperatorType) -> &'static str {
    match typ {
        PhysicalOperatorType::Scan => "Scan",
        PhysicalOperatorType::ScanIntermediates=> "Scan Intermediates",
        PhysicalOperatorType::Placeholder=> "Placeholer Operator",
        PhysicalOperatorType::Filter => "Filter",
        PhysicalOperatorType::Projection => "Projection",
        PhysicalOperatorType::HashAggregate => "HashAggregate",
        PhysicalOperatorType::HashJoinBuild => "HashJoinBuild",
        PhysicalOperatorType::HashJoinProbe => "HashJoinProbe",
        PhysicalOperatorType::Limit => "Limit",
        PhysicalOperatorType::Sort => "Sort",
    }
}

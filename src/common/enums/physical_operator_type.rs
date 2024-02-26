pub enum PhysicalOperatorType {
    Filter,
    Projection,
    Scan,
    Undefined,
}

pub fn physical_operator_to_string(typ: &PhysicalOperatorType) -> &'static str {
    match typ {
        PhysicalOperatorType::Filter => "Filter",
        PhysicalOperatorType::Projection => "Projection",
        PhysicalOperatorType::Scan => "Scan",
        PhysicalOperatorType::Undefined => "Undefined",
    }
}

use ahash::HashMap;

pub struct GroupedRowList {
    pub row_list: Vec<(usize, u64)>
}

pub type GroupedRowHashMap = HashMap<u64, GroupedRowList>;

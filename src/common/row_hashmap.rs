use ahash::{HashMap, RandomState};
use datafusion::common::hash_utils::create_hashes;
use datafusion::{arrow::array::RecordBatch, physical_plan::PhysicalExpr};
use std::sync::Arc;

pub struct GroupedRowList {
    pub row_list: Vec<(usize, u64)>,
}

pub type GroupedRowHashMap = HashMap<u64, GroupedRowList>;

pub fn grouped_row_hashmap_add_batch(
    hashmap: &mut GroupedRowHashMap,
    batch: &Arc<RecordBatch>,
    batch_id: usize,
    exprs: &Vec<Arc<dyn PhysicalExpr>>,
    random_state: &RandomState,
) {
    let num_rows = batch.num_rows();
    let keys_values = exprs
        .iter()
        .map(|f| {
            f.evaluate(batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut hashes_buffer: Vec<u64> = Vec::new();
    hashes_buffer.resize(num_rows, 0);
    let hash_values = create_hashes(&keys_values, random_state, &mut hashes_buffer).unwrap();

    for row_idx in 0..num_rows {
        //grab the hash which is the key
        let hashmap_key = hash_values[row_idx];
        // the value in the hash table is the row_id
        let hashmap_value = row_idx as u64;

        //if the key does not exist create a new array
        if !hashmap.contains_key(&hashmap_key) {
            let mut row_list = Vec::new();
            row_list.push((batch_id, hashmap_value));
            hashmap.insert(hashmap_key, GroupedRowList { row_list });
        } else {
            //else if the key already exists in the map then just append the value to the
            let grouped_row_lists = hashmap.get_mut(&hashmap_key).unwrap();
            let row_list = &mut grouped_row_lists.row_list;
            row_list.push((batch_id, hashmap_value));
        }
    }
}

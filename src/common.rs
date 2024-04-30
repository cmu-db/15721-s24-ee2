use std::sync::Arc;
use datafusion::arrow::array::RecordBatch;

pub mod enums;
pub mod row_hashmap;

pub fn split_large_batch(batch: &RecordBatch, size: usize) -> Vec<Arc<RecordBatch>> {
    let num_output_vec = (batch.num_rows() + size - 1) / size;
    let mut output_vec = Vec::with_capacity(num_output_vec);
    let mut output_offset: usize = 0;
    loop {
        if output_offset >= batch.num_rows() {
            return output_vec;
        }
        let length = std::cmp::min(1024, batch.num_rows() - output_offset);
        output_vec.push(Arc::new(batch.slice(output_offset, length)));
        output_offset += length;
    }
}

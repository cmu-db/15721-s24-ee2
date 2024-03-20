use arrow::array::RecordBatch;
use arrow::error::Result;

use core::panic;
use std::collections::HashMap;
use std::sync::Arc;
pub enum Blob {
    RecordBatchBlob(Vec<RecordBatch>),
    HashMapBlob(HashMap<i32, i32>),
}

impl Blob {
    pub fn get_map(self) -> HashMap<i32, i32> {
        match self {
            Blob::HashMapBlob(m) => m,
            _ => panic!("error"),
        }
    }
    pub fn get_records(self) -> Vec<RecordBatch> {
        match self {
            Blob::RecordBatchBlob(records) => records,
            _ => panic!("error"),
        }
    }
    pub fn append_records(&mut self, batches: Vec<RecordBatch>) {
        match self {
            Blob::RecordBatchBlob(records) => {
                // TODO: check if schema is same
                records.extend(batches)
            }
            _ => panic!("error"),
        }
    }
}

// right now this is typedef of HashMap<i32, Blob>,
// but we may need something else in near future

pub struct Store {
    store: HashMap<i32, Blob>,
}
impl Store {
    pub fn new() -> Store {
        Store {
            store: HashMap::new(),
        }
    }
    pub fn insert(&mut self, key: i32, value: Blob) {
        self.store.insert(key, value);
    }
    pub fn remove(&mut self, key: i32) -> Option<Blob> {
        self.store.remove(&key)
    }
}

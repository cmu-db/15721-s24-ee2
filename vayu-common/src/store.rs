use arrow::array::RecordBatch;
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::thread::scope;
use datafusion::physical_plan::joins::hash_join::JoinLeftData;

use core::panic;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
pub enum Blob {
    // RecordBatchBlob(Vec<RecordBatch>),
    HashMapBlob(JoinLeftData),
}

impl Blob {
    pub fn get_map(self) -> JoinLeftData {
        match self {
            Blob::HashMapBlob(m) => m,
            _ => panic!("error"),
        }
    }
    // pub fn get_records(self) -> Vec<RecordBatch> {
    //     match self {
    //         Blob::RecordBatchBlob(records) => records,
    //         _ => panic!("error"),
    //     }
    // }
    // pub fn append_records(&mut self, batches: Vec<RecordBatch>) {
    //     match self {
    //         Blob::RecordBatchBlob(records) => {
    //             // TODO: check if schema is same
    //             records.extend(batches)
    //         }
    //         _ => panic!("error"),
    //     }
    // }
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
    // pub fn append(&mut self, key: i32, value: Vec<RecordBatch>) {
    //     let blob = self.remove(key);
    //     let mut blob = match blob {
    //         Some(r) => r,
    //         None => Blob::RecordBatchBlob(Vec::new()),
    //     };
    //     blob.append_records(value);
    //     self.store.insert(key, blob);
    // }
    pub fn remove(&mut self, key: i32) -> Option<Blob> {
        self.store.remove(&key)
        // let x = self.store.remove(&key).unwrap().value();
        // Some(x)
    }
}
pub struct Store1 {
    store: SkipMap<i32, i32>,
}
impl Store1 {
    pub fn new() -> Self {
        Store1 {
            store: SkipMap::new(),
        }
    }
    pub fn insert(&mut self, key: i32, value: i32) {
        self.store.insert(key, value);
    }
    // pub fn append(&mut self, key: i32, value: Vec<RecordBatch>) {
    //     let blob = self.remove(key);
    //     let mut blob = match blob {
    //         Some(r) => r,
    //         None => Blob::RecordBatchBlob(Vec::new()),
    //     };
    //     blob.append_records(value);
    //     self.store.insert(key, blob);
    // }
    // pub fn remove(&mut self, key: i32) -> &Option<Arc<Blob>> {
    //     let value = self.store.remove(&key).unwrap().value();
    //     Some(value.un)
    //     // let x = self.store.remove(&key).unwrap().value();
    //     // Some(x)
    // }
}

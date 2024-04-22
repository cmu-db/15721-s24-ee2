use arrow::record_batch::RecordBatch;
use core::panic;
use datafusion::physical_plan::joins::hash_join::JoinLeftData;
use std::collections::HashMap;
#[derive(Clone)]
pub enum Blob {
    RecordBatchBlob(RecordBatch),
    HashMapBlob(JoinLeftData),
}

// impl Blob {
//     // pub fn get_map(self) -> JoinLeftData {
//     //     match self {
//     //         Blob::HashMapBlob(m) => m,
//     //         _ => panic!("error"),
//     //     }
//     // }
//     pub fn get_records(self) -> Vec<RecordBatch> {
//         match self {
//             Blob::RecordBatchBlob(records) => records,
//             _ => panic!("error"),
//         }
//     }
//     pub fn append_records(&mut self, batches: Vec<RecordBatch>) {
//         match self {
//             Blob::RecordBatchBlob(records) => {
//                 // TODO: check if schema is same
//                 records.extend(batches)
//             }
//             _ => panic!("error"),
//         }
//     }
// }

// store store a vector of blobs
// each blob would be output of one of the threads
// finalize step would remove the vec of blob and combine then store the result again

#[derive(Clone)]
pub struct Store {
    pub store: HashMap<i32, Vec<Blob>>,
}
impl Store {
    pub fn new() -> Store {
        Store {
            store: HashMap::new(),
        }
    }
    pub fn insert(&mut self, key: i32, mut value: Blob) {
        let blob = self.store.get_mut(&key);
        let mut blob = match blob {
            Some(r) => r,
            None => {
                self.store.insert(key, vec![]);
                self.store.get_mut(&key).unwrap()
            }
        };
        blob.push(value);
    }
    pub fn append(&mut self, key: i32, mut value: Vec<Blob>) {
        let blob = self.remove(key);
        let mut blob = match blob {
            Some(r) => r,
            None => vec![],
        };
        blob.append(&mut value);
        self.store.insert(key, blob);
    }
    pub fn remove(&mut self, key: i32) -> Option<Vec<Blob>> {
        self.store.remove(&key)
    }
    pub fn get(&mut self, key: i32) -> Option<&Vec<Blob>> {
        self.store.get(&key)
    }
}

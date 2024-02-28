use crate::common::types::LogicalType;

pub const CHUNK_SIZE: u64 = 2048;
//boilerplate for the moment
pub struct DataChunk;

impl DataChunk {
    pub fn new() -> DataChunk {
        DataChunk {}
    }

    pub fn reset(&mut self) {
        // todo!();
    }

    pub fn size(&self) -> u64 {
        0
    }

    pub fn initialize(&mut self, types: &Vec<LogicalType>, capacity: u64) {
        todo!()
    }
}

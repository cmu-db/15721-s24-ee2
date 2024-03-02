use crate::common::types::LogicalType;

pub const CHUNK_SIZE: u64 = 2048;
//boilerplate for the moment
pub struct DataChunk{
    data : Vec<usize>,
    count : usize,
}

impl DataChunk {
    pub fn new() -> DataChunk {
        DataChunk { data : vec![0; CHUNK_SIZE as usize] , count:0 }
    }
    //should return result
    pub fn push(&mut self, val : usize) -> (){
        self.data[self.count] = val;
        self.count+=1;
    }

    pub fn print(&self)->(){
        for i in 0..self.count{
            print!("{} ", self.data[i]);
        }
    }


    pub fn reset(&mut self) {
        self.count = 0;
    }

    pub fn size(&self) -> usize {
        self.count
    }

    pub fn initialize(&mut self, types: &Vec<LogicalType>, capacity: u64) {
        todo!()
    }
}

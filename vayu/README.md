# Vayu - push-based execution engine

## Introduction 

Pipeline 

## making things go fast
1. https://docs.rs/core_affinity/latest/core_affinity/
2. taskset 


tokio has a scheduler which 'try' to keep the process on same cpu.

Lets say we have 16 usable CPUs.

## Keywords

# Executor
One instance of executation

### Tasks
Physical plan of entire SQL query would be represented as a Task.
A Task consists of multiple pipelines. Scheduler should try to assign pipelines of same tasks to same executor.

### DataChunk


### Pipeline


### Driver


How would join work? 

One pipeline would end with HashBuildOperator(sink).

Another pipeline would have HashProbe 

HashProbe::execute(block:RecordBatch){
    
}


## Operators 
1. Scan - CsvScan
2. Filter 
3. Projection 

Upcoming 
1. Aggregate - Sum 
2. Join - HashJoin
3. 




Query execution can be of three kinds

1. sequential query
Scan -> Filter -> Project

2. wait for all before making progress 
Scan -> Sum

3. hash join 
    2 children hashbuild and hashjoin


taskset --cpu-list 0-3 your_program,  taskset --cpu-list 0,1,16,17 your_program

Velox Join 
https://facebookincubator.github.io/velox/develop/joins.html#

Testing on aws
https://docs.aws.amazon.com/AWSEC2/latest/WindowsGuide/dedicated-hosts-overview.html


https://docs.google.com/document/d/1txX60thXn1tQO1ENNT8rwfU3cXLofa7ZccnvP4jD6AA/edit#heading=h.3iwlbn2gzs29



1. Sum 
2. HashJoin 
3. Breaking HashJoin in two parts discuss with aditya from scheduler

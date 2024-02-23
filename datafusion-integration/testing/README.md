## DATAFUSION INTEGRATION 

how to run? 
`cargo run`

what will it do? 
one SQL query is hardcoded in main file. 
1. It will use datafusion to create a logical plan
2. Then it will create a trait based physical plan.
3. Then it will convert it to node based datafusion physical plan
4. It can either run using default datafusion execution engine or cmu execution engine.


Idea is to develop the execution as a library and then this crate can be used to test the execution engine by passing the physical node to it.
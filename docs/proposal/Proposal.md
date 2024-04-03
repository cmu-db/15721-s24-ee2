# Execution Engine Team 2

* Christos Laspias (claspias)

* Hyoungjoo Kim (hyoungjk)

* Yash Kothari (yashkoth)

## Overview
>What is the goal of this project? What will this component achieve?

The goal of this project is to implement a fully functional query execution engine using (some) state-of-the-art techniques in rust. We are going to take inspiration from existing execution engines like Velox and Apache Arrow Datafusion. Moreover, DuckDB will be referenced as well. The main priority is to build an execution engine that works and has good code quality so that it can be easily extended in next semesters. Moreover, we want to be able to be decoupled as much as possible from other components and not make assumptions that will prove to be dead ends in the future. Optimizations are a second priority.

## API Specification

### Overview

From the scheduler, we will receive Datafusion representation of plan graphs.
Because we process plan fragments processed from the scheduler, some operators such as HashJoinBuild should be represented as a custom plan node.
With the I/O service, we will request data of specific columns at given table, offset, and size.

### Encoding
The plan graphs will be encoded in Datafusion, binary protobuf format.
The data from I/O service will be given as Apache Arrow format.

### Errors
We will handle 4 possible errors.
1. Invalid Input error occurs when the input substrait file is invalid. For example, there can be parsing errors, incorrect table name included, or consists of unsupported nodes.
2. Out of Resource error occurs when execution engine requires more memory or local disk than hardware provides.
3. IO error occurs when the I/O service component returns any error.
4. Unknown error handles all other possible errors.

## Architectural Design
>Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.

The input given to the execution engine will be a plan fragment in Datafusion format. We will deserialize the plan and convert it into our own representation of pipeline. The pipeline consists of a source, a sink, and a vector of intermediate operators. We are aiming to implement the basic operators including filter, projection, hash join, hash aggregation, sort, and limit.

The output of the query execution engine will be stored into in-memory UUID store. The scheduler team can instruct us to store the result with the specified UUID and later reference it with the same ID. The result can be stored to the remote storage, local disk, or in memory.

We are going to implement a push based execution engine. Apache Arrow Datafusion is implementing the pull based execution model (just like Volcano), which is simpler and cleaner to implement but slower. Moreover, we are going to implement vectorized execution passing vectors between operators. The query will be interpreted and not compiled. 

We are going to receive data from the I/O service and implement a buffer pool in order to place data in main memory. Another 100% goal is to spill to the local disk if there is no available memory for intermediate results. 

For late/early materialization there is no clear answer yet.

We will also send the required statistics to the catalog. 

## Design Rationale
>Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

The techniques mentioned above are the state of the art for the execution engine and we aim to implement those. As a backup plan, we can switch to the classic pull based execution model (Volcano style) if we are out of time.

## Testing Plan
>How should the component be tested?

### Correctness Test

We will verify the correctness of our execution engine by feeding arbitrary physical execution plans into both our engine and Apache Datafusion execution engine and compare their results. We can generate datafusion physical plan using the datafusion frontend.

### Performance Test

We will also measure the performance of our engine by executing some queries in TPC-H. The execution runtime will be compared with the one from executing the same query in Datafusion. In addition, similar to the correctness test, the result will be verified by comparing with the results from another query engine/database system.

## Trade-offs and Potential Problems
>Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).

We decided to implement operators in our own, so some metadata in datafusion representation might not be properly treated. As a result, we will incrementally update our engine by supporting more and more hand-crafted datafusion plans.

## Glossary (Optional)
>If you are introducing new concepts or giving unintuitive names to components, write them down here.


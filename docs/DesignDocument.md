# Execution Engine Team 2

* Christos Laspias (claspias)

* Hyoungjoo Kim (hyoungjk)

* Yash Kothari (yashkoth)

## Overview

This project aims to implement a fully functional OLAP query execution engine using state-of-the-art techniques such as a push-based vectorized execution model.
The execution engine is responsible for executing physical query plan fragments given by the other component.
The internal abstraction of the engine is inspired by existing engines such as Apache Datafusion, DuckDB, and Velox.

## API Specification

### Overview

The execution engine interacts with two external components: the scheduler and the I/O service.
From the scheduler, we receive plan fragments represented by the Datafusion ExecutionPlan format.
Because the Datafusion format represents the whole query instead of plan fragments, the incompatible nodes such as HashJoinBuild and Probe are represented as a custom node.
When the leaf node requires fetching files from the remote storage, the engine requests it from the I/O service.

### Encoding
The plan fragments are encoded in Datafusion ExecutionPlan data structures.
The current implementation does not support the complete interface for the physical plan.
However, in the future, it will receive the binary protobuf format and decode it to Datafusion ExecutionPlan structures.

The data from the I/O service should be given as Apache Arrow format.

### Errors
The engine will handle 4 possible errors.
1. **Invalid Input error** occurs when the input Datafusion plan is invalid.
2. **Out-of-Resource error** occurs when the engine requires more memory or local disk storage than the hardware provides.
3. **IO error** occurs when the I/O service component returns any error.
4. **Unknown error** handles all other possible errors.

## Architectural Design

The input given to the execution engine is a plan fragment in Datafusion ExecutionPlan format. 
We traverse the plan graph and convert it into our pipeline representation. 
The pipeline consists of a source, a sink, and a vector of intermediate operators. 
We currently support parquet scan, filter, projection, hash aggregation, hash join, sort, and limit operators.

The output of the engine is stored in the in-memory UUID store. 
When the scheduler sends the plan fragment, it also sends the UUID to which the result of the plan fragment should be stored.
When the scheduler sends a plan fragment that uses the intermediate result stored in the UUID store, it specifies the UUID value so that the engine can look up the UUID store and use the stored data to process the fragment.
The UUID store can store either the vector of RecordBatches or the HashMap.

The engine operates as a push-based vectorized model.
Each operator consumes and/or produces in-memory vectorized PAX-layout data represented by Apache Arrow RecordBatch.
Although the pipeline is a linear chain of operators, its execution forms a tree because each operator can consume and produce multiple RecordBatches.
Hence, the push-based pipeline executor uses a DFS algorithm to traverse the abstract execution graph, execute the operators, and store the result in the UUID store.
It is faster than the pull-based model of Apache Datafusion.
Queries are not compiled, but we use pre-compiled vectorized kernel functions provided by the Datafusion library for some operators.

The hash aggregation and hash join operators are implemented using the custom hashmap structure.
They both build the hash map that maps a set of column values to the list of row offsets.
If the input has multiple RecordBatches, instead of concatenating them to create a large RecordBatch, the engine stores them separately without copying and maintains the list of pairs of (batch id, row offset).
Later, the operator traverses the list and perform appropriate operations.

The current implementation does not support buffer pools or memory spills.
In the future, the data from the I/O service is cached in the in-memory buffer pool and the excess memory is spilled to the local disk.

## Design Rationale
>Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

Our design aims to achieve the performance of state-of-the-art execution engines without excessive engineering costs.

The push-based vectorized model is chosen because it is faster and more efficient than the pull-based model.
However, the query compilation is not adopted as it requires excessive effort on implementation and maintenance.

Each operator operates on the in-memory data represented in PAX format.
The row-oriented layout is not chosen because it is not suitable for vectorized execution of OLAP queries.

The list of row offsets in the custom hashmap is stored as the pair of (batch id, row offset).
This format stores the row index information without excessive memory overhead.
Additionally, this is better than concatenating the RecordBatches because the internal contents do not have to be copied into consecutive memory.

## Testing

### Correctness Test

The unit tests are implemented for each operator, and the correctness of the result is verified manually.

We also implmented the integrated executor that executes arbitrary Datafusion ExecutionPlan.
The result of our engine is compared with the result from the default execution engine of Datafusion.

### Performance Test

Using the integrated executor, we tested all TPC-H queries.
For the queries that produced the correct result, we measured the runtime of our engine executing each query and compared it with the runtime from the Datafusion engine.

## Trade-offs and Potential Problems
>Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).

We decided to implement operators ourselves, so some subtle metadata in the Datafusion ExecutionPlan might not be properly treated. 
As a result, we will incrementally update our engine by supporting more and more hand-crafted physical plans.

## Glossary (Optional)
>If you are introducing new concepts or giving unintuitive names to components, write them down here.

# 15721-s24-ee2
15-721 Spring 2024 - Execution Engine 



crates:
1. vayu
    this contains just a single pipeline execution. no dependency on datafusion directly.
    depends on: vayu_common 
2. vayu-common
    this contains common data structures used in vayu and utility functions for conversions from datafusion pipeline to vayu pipeline
    depends on: datafusion
3. vayuDB
    this contains actually database made up of scheduler, io and vayu execution engine
    depends on: arrow, vayu and vayu_common




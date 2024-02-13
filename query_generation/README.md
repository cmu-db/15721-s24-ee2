# Qyery Generation
## Purpose
This 'query_generation' directory exists to provide a simple way to generate substrait query plans.

## Setup.
First install duckdb via pip . The substrait extension is only available in python, R and through SQL api.
```
pip install duckdb==0.9.2
```

## Batch convert sql to proto
```shell
make
```
This command converts all sql files to proto files.

## Investigate proto
```shell
cargo run -- proto_file.proto
```

## Writing a new sql test case
See example sql files.
Write several SQL commands, newline, `--PARSE`, newline, and then write SQL command that you want to generate substrait plan graph of.

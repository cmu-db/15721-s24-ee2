# Qyery Generation
## Purpose
This 'query_generation' directory exists to provide a simple way to generate substrait query plans.

## Setup.
First install duckdb via pip . The substrait extension is only available in python, R and through SQL api.
```
pip install duckdb==0.9.2
```

## Example
An example of how to generate plans is shown in query_generation.py

## Output
There are two ways to generate the plan. 
1. In a json file.
2. In a blob of bytes. 

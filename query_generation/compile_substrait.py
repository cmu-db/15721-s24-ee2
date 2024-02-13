import duckdb
import sys

#connect to the in-memory database
con = duckdb.connect()

#install and load the extension
con.install_extension("substrait")
con.load_extension("substrait")

if len(sys.argv) != 3:
    print("Usage: python3 query_generation.py input.sql output.proto")
    exit(1)

input_sql_file = sys.argv[1]
output_proto_file = sys.argv[2]

query_to_parse = ""
with open(input_sql_file, "r") as qf:
    while (True):
        sql_line = qf.readline()
        if sql_line.strip() == "--PARSE":
            query_to_parse = qf.readline()
            break
        else:
            con.execute(query = sql_line)
assert query_to_parse != "", "Check if sql contains --PARSE line"

# blob generation
proto_bytes = con.get_substrait(query=query_to_parse).fetchone()[0]
with open(output_proto_file, "wb") as proto_output:
    proto_output.write(proto_bytes)

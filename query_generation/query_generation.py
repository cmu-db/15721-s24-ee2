import duckdb

#connect to the in-memory database
con = duckdb.connect()

#install and load the extension
con.install_extension("substrait")
con.load_extension("substrait")

con.execute(query = "CREATE TABLE cmu_students (cmu_id INT)")
con.execute(query = "INSERT INTO cmu_students VALUES (3),(5)")

query = "SELECT * FROM cmu_students"

# blob generation
proto_bytes = con.get_substrait(query=query).fetchone()[0]

# json generation
json = con.get_substrait_json(query).fetchone()[0]
print(json)

with open("query.json", "w") as query_output:
    query_output.write(json)

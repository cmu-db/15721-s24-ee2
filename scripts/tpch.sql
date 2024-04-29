INSTALL TPCH;
LOAD TPCH;
CALL dbgen(sf=1);

copy (select * from customer) to 'data/tpch/customer.parquet' (Format 'parquet');
copy (select * from lineitem) to 'data/tpch/lineitem.parquet' (Format 'parquet');
copy (select * from nation) to 'data/tpch/nation.parquet' (Format 'parquet');
copy (select * from orders) to 'data/tpch/orders.parquet' (Format 'parquet');
copy (select * from part) to 'data/tpch/part.parquet' (Format 'parquet');
copy (select * from partsupp) to 'data/tpch/partsupp.parquet' (Format 'parquet');
copy (select * from region) to 'data/tpch/region.parquet' (Format 'parquet');
copy (select * from supplier) to 'data/tpch/supplier.parquet' (Format 'parquet');

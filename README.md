# SQL OLAP bench

## Generate data

### Generate TPC-H data (Parquet, DuckDB and Hyper files)

```bash
$ python generate_tpch_data.py -h
usage: generate_tpch_data.py [-h] [-sf INT] [-d TXT] [-n INT] [-s]

Command line interface to generate_TPC-H_data

options:
  -h, --help            show this help message and exit
  -sf NUM, --scale_factor NUM
                        TPC-H scale factor
  -d TXT, --data_dir TXT
                        Data dir path
  -n INT, --n_steps INT
                        number of files
  -s, --suite           Benchmark suite with scale factors [1, 3, 10, 30, 100]
```

- Examples:

```bash
$ python generate_tpch_data.py -sf 1 -d /home/francois/Workspace/pydbbench/data
```
Creates the folder `/home/francois/Data/dbbenchdata/tpch_1` with the following files:

	customer.parquet  lineitem.parquet  part.parquet      supplier.parquet
	data.duckdb       nation.parquet    partsupp.parquet  tmp.hyper
	data.hyper        orders.parquet    region.parquet

```bash
$ python generate_tpch_data.py -sf 1 -d /home/francois/Workspace/pydbbench/data -n 2
```
Creates the folder `/home/francois/Data/dbbenchdata/tpch_1` with the following files:

	customer_000.parquet  nation_000.parquet  partsupp_000.parquet
	customer_001.parquet  nation_001.parquet  partsupp_001.parquet
	data.duckdb           orders_000.parquet  region_000.parquet
	data.hyper            orders_001.parquet  region_001.parquet
	lineitem_000.parquet  part_000.parquet    supplier_000.parquet
	lineitem_001.parquet  part_001.parquet    supplier_001.parquet

```bash
$ python generate_tpch_data.py -sf 10 -d /home/francois/Workspace/pydbbench/data
```
Creates the folder `/home/francois/Data/dbbenchdata/tpch_10` with the same files as above.

```bash
$ python generate_tpch_data.py -s -d /home/francois/Workspace/pydbbench/data 
```

Creates the benchmark suite with the following folders:  

	/home/francois/Workspace/pydbbench/data/tpch_1  
	/home/francois/Workspace/pydbbench/data/tpch_3  
	/home/francois/Workspace/pydbbench/data/tpch_10  
	/home/francois/Workspace/pydbbench/data/tpch_30  
	/home/francois/Workspace/pydbbench/data/tpch_100  


### Generate TPC-DS data (Parquet, DuckDB and Hyper files)

```bash
$ python generate_tpcds_data.py -h
usage: generate_tpcds_data.py [-h] [-sf INT] [-d TXT] [-s]

Command line interface to generate_TPC-H_data

options:
  -h, --help            show this help message and exit
  -sf NUM, --scale_factor NUM
                        TPC-DS scale factor
  -d TXT, --data_dir TXT
                        Data dir path
  -s, --suite           Benchmark suite with scale factors [1, 3, 10, 30, 100]
```

- Examples:

```bash
$ python generate_tpcds_data.py -sf 1 -d /home/francois/Workspace/pydbbench/data
```
Creates the folder `/home/francois/Data/dbbenchdata/tpcds_1` with the following files:

	call_center.parquet             item.parquet
	catalog_page.parquet            promotion.parquet
	catalog_returns.parquet         reason.parquet
	catalog_sales.parquet           ship_mode.parquet
	customer_address.parquet        store.parquet
	customer_demographics.parquet   store_returns.parquet
	customer.parquet                store_sales.parquet
	data.duckdb                     time_dim.parquet
	data.hyper                      warehouse.parquet
	date_dim.parquet                web_page.parquet
	household_demographics.parquet  web_returns.parquet
	income_band.parquet             web_sales.parquet
	inventory.parquet               web_site.parquet


```bash
$ python generate_tpcds_data.py -sf 10 -d /home/francois/Workspace/pydbbench/data
```
Creates the folder `/home/francois/Data/dbbenchdata/tpcds_10` with the same files as above.

```bash
$ python generate_tpcds_data.py -s -d /home/francois/Workspace/pydbbench/data 
```

Creates the benchmark suite with the following folders:  

	/home/francois/Workspace/pydbbench/data/tpcds_1  
	/home/francois/Workspace/pydbbench/data/tpcds_3  
	/home/francois/Workspace/pydbbench/data/tpcds_10  
	/home/francois/Workspace/pydbbench/data/tpcds_30  
	/home/francois/Workspace/pydbbench/data/tpcds_100  

## TPC-H benchmark

```bash
$ python tpch_bench.py -d /home/francois/Workspace/pydbbench/data
```

Loops over all the `tpch_*` subfolders of the data directory, run the queries and generates the CSV file: `timings_TPCH.csv`
import os
import sys
from time import perf_counter

import duckdb
from loguru import logger


folder_path = "/home/francois/Data/dbbenchdata/tpch_1"

query = """ SELECT * FROM lineitem ORDER BY l_shipdate """

fmt = (
    "[<g>{time:YYYY-MM-DD HH:mm:ss.SSSZ}</g> :: <c>{level}</c> ::"
    + " <e>{process.id}</e>] {message}"
)
logger.remove()
logger.add(
    sys.stdout,
    level="DEBUG",
    backtrace=True,
    diagnose=True,
    format=fmt,
    enqueue=True,
)

folder_name = os.path.basename(os.path.normpath(folder_path))
scale_factor = float(folder_name.split("_")[-1])
logger.info(f"folder name : {folder_name}")
logger.info(f"scale factor : {scale_factor}")


# DuckDB
# ======
logger.info("DuckDB - parquet")
file_path = os.path.join(folder_path, "lineitem.parquet")
logger.info(f"file path : {file_path}")
start_time_step = perf_counter()

conn = duckdb.connect()
conn.execute(
    f"""
COPY  (select * from '{file_path}' order by l_shipdate )
to 'lineitem_DuckDB' (FORMAT 'PARQUET', PER_THREAD_OUTPUT TRUE)
"""
)

elapsed_time_step = perf_counter() - start_time_step
logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

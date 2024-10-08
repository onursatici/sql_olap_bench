"""
Example:
$ python tpch_bench.py -d /home/francois/Data/dbbenchdata -o test.csv
"""

import os
import pathlib
import sys
import datetime
from argparse import ArgumentParser

import datafusion
import duckdb
import numpy as np
import pandas as pd
import tableauhyperapi
from loguru import logger

from bench_tools import (
    run_queries_duckdb_on_duckdb,
    run_queries_duckdb_on_parquet,
    run_queries_duckdb_on_lance,
    run_queries_hyper_on_hyper,
    run_queries_hyper_on_parquet,
    run_queries_datafusion_on_parquet,
    run_queries_ballista_on_parquet,
    run_queries_datafusion_on_lance,
    run_queries_postgresql,
    run_queries_polars_on_parquet,
    run_queries_datafusion_ray_on_parquet,
    run_queries_quokka_on_parquet,
)
from tpch_queries import sql
from ref_row_count import tpch_ref_n_rows_returned
from misc import find_subfolders_with_prefix, visualize_timings


if __name__ == "__main__":
    # logger
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

    logger.info(f"Python          : {sys.version}")
    logger.info(f"DuckDB          : {duckdb.__version__}")
    logger.info(f"TableauHyperAPI : {tableauhyperapi.__version__}")
    logger.info(f"Datafusion      : {datafusion.__version__}")

    # argument parser
    parser = ArgumentParser(description="Command line interface to the TPC-H benchmark")
    _ = parser.add_argument(
        "-d",
        "--data_dir",
        dest="data_dir_path",
        help="Data dir path",
        metavar="TXT",
        type=str,
        required=False,
        default=os.getcwd(),
    )
    _ = parser.add_argument(
        "-o",
        "--output",
        dest="output_dir",
        help="output directory path",
        metavar="TXT",
        type=str,
        required=False,
        default=os.path.join(
            os.getcwd(),
            "results",
            datetime.datetime.now().replace(microsecond=0).isoformat(),
        ),
    )
    args = parser.parse_args()
    data_dir_path = pathlib.Path(args.data_dir_path).resolve()
    logger.info(f"data dir path : {data_dir_path}")
    logger.info(f"output dir path : {args.output_dir}")
    # Create the output directory if it does not exist
    os.makedirs(args.output_dir, exist_ok=True)

    output_csv = pathlib.Path(os.path.join(args.output_dir, "timings.csv")).resolve()

    tpch_subfolders = find_subfolders_with_prefix(data_dir_path, "tpch_")

    df = pd.DataFrame()
    df_tmp = run_queries_polars_on_parquet(tpch_subfolders, sql, logger)
    df = pd.concat((df, df_tmp), axis=0)

    df_tmp = run_queries_duckdb_on_duckdb(tpch_subfolders, sql, logger)
    df = pd.concat((df, df_tmp), axis=0)

    df_tmp = run_queries_duckdb_on_parquet(tpch_subfolders, sql, logger)
    df = pd.concat((df, df_tmp), axis=0)

    # df_tmp = run_queries_duckdb_on_lance(tpch_subfolders, sql, logger)
    # df = pd.concat((df, df_tmp), axis=0)

    df_tmp = run_queries_hyper_on_hyper(tpch_subfolders, sql, logger)
    df = pd.concat((df, df_tmp), axis=0)

    df_tmp = run_queries_hyper_on_parquet(tpch_subfolders, sql, logger)
    df = pd.concat((df, df_tmp), axis=0)

    df_tmp = run_queries_datafusion_on_parquet(tpch_subfolders, sql, logger)
    df = pd.concat((df, df_tmp), axis=0)

    df_tmp = run_queries_ballista_on_parquet(tpch_subfolders, sql, logger)
    df = pd.concat((df, df_tmp), axis=0)

    # df_tmp = run_queries_quokka_on_parquet(tpch_subfolders, logger)
    # df = pd.concat((df, df_tmp), axis=0)

    # df_tmp = run_queries_datafusion_ray_on_parquet(tpch_subfolders, sql, logger)
    # df = pd.concat((df, df_tmp), axis=0)

    # df_tmp = run_queries_datafusion_on_lance(tpch_subfolders, sql, logger)
    # df = pd.concat((df, df_tmp), axis=0)

    # df_tmp = run_queries_postgresql(tpch_subfolders, sql, logger)
    # df = pd.concat((df, df_tmp), axis=0)

    d = tpch_ref_n_rows_returned()
    for row in df.itertuples():
        n_returned_rows_ref = d[(int(row.scale_factor), int(row.query))]
        if (not np.isnan(row.n_returned_rows)) and (
            n_returned_rows_ref != int(row.n_returned_rows)
        ):
            raise ValueError(
                f"Wrong number of returned rows! engine : {row.engine}, "
                + f"file type : {row.file_type}, scale factor : {row.scale_factor}, query : {row.query}, "
                + f"n returned rows : {row.n_returned_rows}, should be : {n_returned_rows_ref}"
            )

    df.to_csv(output_csv, index=False)
    visualize_timings(df, args.output_dir)

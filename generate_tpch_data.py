"""
Generate TPC-H data (Parquet, DuckDB and Hyper files).

The TPC-H benchmark emulates a decision support systems that examines large volumes of data,
executes queries with a high degree of complexity, and gives answers to critical business questions.

Example:
$ python generate_tpch_data.py -sf 1 -d /home/francois/Workspace/pydbbench/data
"""

import os
import pathlib
import sys
from argparse import ArgumentParser
from time import perf_counter

import duckdb
from loguru import logger

from hyper_tools import convert_parquets_to_hyper
from lance_tools import convert_parquets_to_lance


def generate_tpch_data_files(
    data_dir_path: pathlib.Path,
    logger: logger,
    scale_factor: float = 1.0,
    n_steps: int = 1,
    compression: str = "snappy",
    row_group_size: int = 122_880,
    hyper: bool = True,
    lance: bool = True,
    csv: bool = False,
):
    """
    Generate TPC-H benchmark data.

    Parameters
    ----------
    data_dir_path : pathlib.Path
        The path to the directory where the generated data will be saved.
    logger : loguru.logger
        A logger object that will be used to log information about the data generation process.
    scale_factor : float, optional
        The scale factor for the TPC-H benchmark data generation. The default value is 1.0.
    n_steps : int, optional
        The number of steps to generate the data in. Each step generates a subset of the data.
        The default value is 1.
    compression : str
        The Parquet compression format to use ('uncompressed', 'snappy', 'gzip' or 'zstd').
        The default is 'snappy'.
    row_group_size : int
        The Parquet target size of each row-group. Default is 122880.
    hyper : bool
        Generates an Hyper file from the Parquet files if True.

    Returns
    -------
    None

    Notes
    -----
    This function generates the TPC-H benchmark data in Parquet format using DuckDB.

    The generated data is saved to the specified directory as Parquet files. Each table in the
    TPC-H benchmark schema is saved as n_steps separate Parquet files.

    """
    start_time_s = perf_counter()
    logger.info("==== BEGIN generate TPC-H data ====")

    logger.info(f"Scale factor : {scale_factor}")
    logger.info(f"Data dir path : {data_dir_path}")
    assert n_steps < 1000
    logger.info(f"Number of steps : {n_steps}")
    compression = compression.upper()
    assert compression in ["UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD"]
    logger.info(f"Parquet compression : {compression}")
    assert isinstance(row_group_size, int)
    assert row_group_size > 0
    logger.info(f"Parquet row group size : {row_group_size}")

    data_dir_path.mkdir(parents=True, exist_ok=True)
    parquet_dir = data_dir_path.joinpath("tpch_" + str(scale_factor))
    parquet_dir.mkdir(exist_ok=False)
    duck_db_file_path = parquet_dir.joinpath("data.duckdb")

    logger.info("Connection to duckdb")
    with duckdb.connect(database=str(duck_db_file_path), read_only=False) as con:
        logger.info("Generate the Parquet files")
        _ = con.sql("INSTALL tpch")
        _ = con.sql("LOAD tpch")
        table_names = None
        for step in range(0, n_steps):
            logger.info(f"Step {step + 1} / {n_steps}")

            _ = con.sql(
                f"CALL dbgen(sf={scale_factor}, children={n_steps}, step={step})"
            )

            if table_names is None:
                df = con.sql("SELECT * FROM information_schema.tables").df()
                table_names = df.table_name.to_list()

            for tbl in table_names:
                if n_steps > 1:
                    parquet_file_path = parquet_dir.joinpath(
                        tbl + f"_{str(step).zfill(3)}.parquet"
                    )
                else:
                    parquet_file_path = parquet_dir.joinpath(tbl + ".parquet")
                logger.info(f"Writting file : {str(parquet_file_path)[:70]:<70s}")
                _ = con.sql(
                    f"COPY (SELECT * FROM {tbl}) TO '{parquet_file_path}' (FORMAT PARQUET, "
                    + f"COMPRESSION {compression}, "
                    + f"ROW_GROUP_SIZE {row_group_size})"
                )

                # table statistics
                n_rows = con.sql(
                    f"SELECT COUNT(*) FROM '{parquet_file_path}'"
                ).fetchone()[0]
                df = con.sql(f"SELECT * FROM '{parquet_file_path}' LIMIT 1").df()
                columns = df.columns
                n_cols = len(columns)
                logger.info(f"{n_rows:>12d} rows, {n_cols:>12d} columns")

    # Convert the parquet file to an Hyper file
    if hyper:
        convert_parquets_to_hyper(parquet_dir, logger)

    if lance:
        convert_parquets_to_lance(parquet_dir, logger)

    logger.info("====  END  generate TPC-H data ====")
    elapsed_time_s = perf_counter() - start_time_s
    logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")


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

    # argument parser
    parser = ArgumentParser(description="Command line interface to generate_TPC-H_data")
    _ = parser.add_argument(
        "-sf",
        "--scale_factor",
        dest="scale_factor",
        help="TPC-H scale factor",
        metavar="NUM",
        type=float,
        required=False,
        default=1.0,
    )
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
        "-n",
        "--n_steps",
        dest="n_steps",
        help="number of files",
        metavar="INT",
        type=int,
        required=False,
        default=1,
    )
    parser.add_argument(
        "-s",
        "--suite",
        dest="suite",
        help="Benchmark suite with scale factors [1, 3, 10, 30, 100]",
        action="store_true",
    )
    args = parser.parse_args()
    scale_factor = args.scale_factor
    if scale_factor.is_integer():
        scale_factor = int(scale_factor)
    data_dir_path = pathlib.Path(args.data_dir_path).resolve()
    n_steps = args.n_steps
    suite = args.suite

    if suite:
        for scale_factor in [1, 3, 10, 30, 100]:
            generate_tpch_data_files(
                data_dir_path,
                logger,
                scale_factor,
                n_steps,
            )
    else:
        generate_tpch_data_files(
            data_dir_path,
            logger,
            scale_factor,
            n_steps,
        )

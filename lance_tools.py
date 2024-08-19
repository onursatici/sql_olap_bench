import glob
import os
import pathlib
import sys
from argparse import ArgumentParser
from time import perf_counter

from loguru import logger
import pyarrow.parquet as pq
import lance

def convert_parquets_to_lance(directory_path: pathlib.Path, logger: logger) -> list:
    """
    Convert all parquet files in a directory into individual LanceDB tables.

    Parameters
    ----------
    directory_path : pathlib.Path
        Path to directory containing parquet files.

    Returns
    -------
    list: List of paths to the generated LanceDB tables.

    Raises
    ------
    FileNotFoundError: If the specified directory does not exist.
    ValueError: If no parquet files are found in the specified directory.
    """

    start_time_s = perf_counter()
    logger.info("==== BEGIN convert_parquets_to_lance ====")

    if not os.path.isdir(directory_path):
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    logger.info(f"Directory path : {directory_path}")

    parquet_files = glob.glob(os.path.join(directory_path, "*.parquet"))
    parquet_file_count = len(parquet_files)
    logger.info(f"Found {parquet_file_count} Parquet files")

    if parquet_file_count == 0:
        raise ValueError(f"No Parquet files found in {directory_path}")

    lance_tables = []

    for parquet_file_path in parquet_files:
        file_name = os.path.basename(parquet_file_path)
        table_name = os.path.splitext(file_name)[0]

        lance_table_path = directory_path.joinpath(f"{table_name}.lance")
        logger.info(f"Converting {file_name} to Lance table: {lance_table_path}")

        # Read Parquet file
        table = pq.read_table(parquet_file_path)

        # Get number of rows
        table_length = len(table)
        logger.info(f"{file_name[-50:]:<50s} : {table_length:>12d} rows")

        # Create Lance table
        lance.write_dataset(table, lance_table_path)

        logger.info(f"-- {table_length} rows have been copied from the file '{file_name}' into the Lance table.")

        lance_tables.append(str(lance_table_path))

    logger.info("====  END  convert_parquets_to_lance ====")
    elapsed_time_s = perf_counter() - start_time_s
    logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

    return lance_tables

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
    parser = ArgumentParser(
        description="Command line interface to convert_parquets_to_lance"
    )
    _ = parser.add_argument(
        "-d",
        "--parquet_dir",
        dest="parquet_dir",
        help="Path to the directory holding the Parquet files",
        metavar="TXT",
        type=str,
        required=True,
    )
    args = parser.parse_args()
    directory_path = pathlib.Path(args.parquet_dir).resolve()
    lance_tables = convert_parquets_to_lance(directory_path, logger)

    logger.info(f"Created {len(lance_tables)} Lance tables:")
    for table_path in lance_tables:
        logger.info(f"  {table_path}")

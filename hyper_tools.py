import glob
import os
import pathlib
import sys
from argparse import ArgumentParser
from time import perf_counter

from loguru import logger
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperException,
    HyperProcess,
    TableName,
    Telemetry,
    escape_string_literal,
)


def convert_parquets_to_hyper(
    directory_path: pathlib.Path, logger: logger, hyper_schema: str = "Export"
) -> str:
    """
    Combine all parquet files in a directory into a Tableau Hyper file.

    Parameters
    ----------
    directory_path : pathlib.Path
        Path to directory containing parquet files.
    hyper_schema : str
        Hyper schema name

    Returns
    -------
    str: Path to the generated Tableau Hyper file.

    Raises
    ------
    FileNotFoundError: If the specified directory does not exist.
    ValueError: If no parquet files are found in the specified directory.

    Example:
    >>> parquet_to_hyper('C:/Users/JohnDoe/parquet_data/')
    'C:/Users/JohnDoe/combined_data.hyper'
    """

    start_time_s = perf_counter()
    logger.info("==== BEGIN convert_parquets_to_hyper ====")

    if not os.path.isdir(directory_path):
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    logger.info(f"Directory path : {directory_path}")

    hyper_database_path = directory_path.joinpath("data.hyper")
    logger.info(f"Hyper file path : {hyper_database_path}")

    parquet_files = glob.glob(os.path.join(directory_path, "*.parquet"))
    parquet_file_count = len(parquet_files)
    logger.info(f"Found {parquet_file_count} Parquet files")

    # Start the Hyper process.
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Open a connection to the Hyper process. This will also create the new Hyper file.
        # The `CREATE_AND_REPLACE` mode causes the file to be replaced if it
        # already exists.
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_database_path,
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            connection.catalog.create_schema_if_not_exists(hyper_schema)

            for parquet_file_path in parquet_files:
                file_name = os.path.basename(parquet_file_path)
                table_name = os.path.splitext(file_name)[0]
                if table_name[-3:].isdigit():
                    table_name = table_name[:-4]

                # number of rows in the source file
                sql = f"SELECT COUNT(*) FROM external({escape_string_literal(parquet_file_path)})"
                table_length = connection.execute_scalar_query(sql)
                logger.info(f"{file_name[-50:]:<50s} : {table_length:>12d} rows")

                # check if the table already exists in the Hyper file. If not creates it:
                is_table = False
                table = TableName(hyper_schema, table_name)
                try:
                    connection.execute_command(f"SELECT COUNT(*) FROM {table}")
                    is_table = True
                except HyperException as e:
                    if "does not exist" in str(e):
                        logger.info(f"Table {table} not found in Hyper database")
                    else:
                        logger.error(e)

                # Insert the data
                if not is_table:
                    sql = f"""CREATE TABLE {table} AS 
                    (SELECT * FROM external({escape_string_literal(parquet_file_path)}))"""
                    connection.execute_command(sql)
                    sql = f"SELECT COUNT(*) FROM {table}"
                    count_inserted = connection.execute_scalar_query(sql)
                else:
                    sql = f"""INSERT INTO {table} 
                    (SELECT * FROM external({escape_string_literal(parquet_file_path)}))"""
                    count_inserted = connection.execute_command(sql)
                logger.info(
                    f"-- {count_inserted} rows have been copied from "
                    + f"the file '{file_name}' into the "
                    + f"table {table}."
                )

    logger.info("====  END  convert_parquets_to_hyper ====")
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
    parser = ArgumentParser(
        description="Command line interface to convert_parquets_to_hyper"
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
    convert_parquets_to_hyper(directory_path, logger)

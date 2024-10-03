import json
import glob
import os
from time import perf_counter

import datafusion
import pyballista
import pyarrow as pa
import duckdb
import numpy as np
import pandas as pd
import psycopg2
import lance
import polars as pl
from tableauhyperapi import Connection, CreateMode, HyperProcess, Telemetry

from misc import get_query_tag


def get_queries(txt, scale_factor):
    queries_txt = txt.replace("__COEF__", str(float(0.0001 / scale_factor)))
    queries = queries_txt.split(";")
    queries = [q.strip() for q in queries]
    queries = [q for q in queries if len(q) > 0]
    return queries


def run_queries_duckdb_on_duckdb(subfolders, queries_duckdb, logger, tmp_dir_path=None):
    """
    if the DuckDB connection is created inside the query loop, query 18 causes a crash...
    """
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"DuckDB / .duckdb - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_duckdb, scale_factor)
        query_count = len(queries)

        duckdb_file_path = os.path.join(folder_path, "data.duckdb")
        logger.info(f"DuckDB file path : {duckdb_file_path}")

        for i, query in enumerate(queries):
            query_tag = get_query_tag(query)
            logger.info(f"query {i+1} / {query_count} : tag {query_tag}")

            with duckdb.connect(database=str(duckdb_file_path), read_only=False) as con:
                # _ = con.execute("PRAGMA enable_object_cache")
                if tmp_dir_path is not None:
                    _ = con.execute(f"SET temp_directory='{tmp_dir_path}'")

                if (
                    (tpc_name == "tpch") and (i + 1 == 21) and (scale_factor == 100.0)
                ):  # OOM error with query 21 and SF100
                    d = dict(
                        [
                            ("engine", "DuckDB"),
                            ("file_type", "duckdb"),
                            ("scale_factor", scale_factor),
                            ("query", i + 1),
                            ("n_returned_rows", np.NaN),
                            ("elapsed_time_s", np.NaN),
                        ]
                    )

                else:
                    start_time_s = perf_counter()
                    _ = con.execute(query)
                    elapsed_time_s = perf_counter() - start_time_s
                    logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                    result = con.df()
                    n_returned_rows = result.shape[0]
                    # print(result)

                    d = dict(
                        [
                            ("engine", "DuckDB"),
                            ("file_type", "duckdb"),
                            ("scale_factor", scale_factor),
                            ("query", i + 1),
                            ("n_returned_rows", n_returned_rows),
                            ("elapsed_time_s", elapsed_time_s),
                        ]
                    )

            timings.append(d)

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df


def run_queries_duckdb_on_parquet(
    subfolders, queries_duckdb, logger, tmp_dir_path=None
):
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"DuckDB / .parquet - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_duckdb, scale_factor)
        query_count = len(queries)

        parquet_file_paths = glob.glob(os.path.join(folder_path, "*.parquet"))
        parquet_file_count = len(parquet_file_paths)
        logger.info(f"Found {parquet_file_count} Parquet files")

        # list tables
        table_names = []
        parquet_files_dict = {}
        for parquet_file_path in parquet_file_paths:
            file_name = os.path.basename(parquet_file_path)
            table_name = os.path.splitext(file_name)[0]
            if table_name[-3:].isdigit():
                table_name = table_name[:-4]
            table_names.append(table_name)
            if table_name in parquet_files_dict:
                parquet_files_dict[table_name].append(parquet_file_path)
            else:
                parquet_files_dict[table_name] = [parquet_file_path]
        table_names = list(set(table_names))

        con = duckdb.connect()
        # _ = con.execute("PRAGMA enable_object_cache;")
        if tmp_dir_path is not None:
            _ = con.execute(f"SET temp_directory='{tmp_dir_path}'")
        # load the files
        for table_name in table_names:
            q = f"""CREATE VIEW IF NOT EXISTS {table_name} AS SELECT *
                FROM read_parquet({parquet_files_dict[table_name]})"""
            con.execute(q)

        for i, query in enumerate(queries):
            query_tag = get_query_tag(query)
            logger.info(f"query {i+1} / {query_count} : tag {query_tag}")
            if ((tpc_name == "tpch") and (i + 1 == 21) and (scale_factor == 100.0)) or (
                (tpc_name == "tpcds") and (i + 1 == 68) and (scale_factor >= 1.0)
            ):
                d = dict(
                    [
                        ("engine", "DuckDB"),
                        ("file_type", "parquet"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", np.NaN),
                        ("elapsed_time_s", np.NaN),
                    ]
                )

            else:
                start_time_s = perf_counter()
                _ = con.execute(query)
                elapsed_time_s = perf_counter() - start_time_s
                logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                result = con.df()
                n_returned_rows = result.shape[0]

                d = dict(
                    [
                        ("engine", "DuckDB"),
                        ("file_type", "parquet"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", n_returned_rows),
                        ("elapsed_time_s", elapsed_time_s),
                    ]
                )

            timings.append(d)

        con.close()

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df


def run_queries_duckdb_on_lance(subfolders, queries_duckdb, logger, tmp_dir_path=None):
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"DuckDB / .lance - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_duckdb, scale_factor)
        query_count = len(queries)

        lance_file_paths = glob.glob(os.path.join(folder_path, "*.lance"))
        lance_file_count = len(lance_file_paths)
        logger.info(f"Found {lance_file_count} Lance files")

        # list tables
        table_names = []
        lance_files_dict = {}
        for lance_file_path in lance_file_paths:
            file_name = os.path.basename(lance_file_path)
            table_name = os.path.splitext(file_name)[0]
            if table_name[-3:].isdigit():
                table_name = table_name[:-4]
            table_names.append(table_name)
            if table_name in lance_files_dict:
                lance_files_dict[table_name].append(lance_file_path)
            else:
                lance_files_dict[table_name] = [lance_file_path]
        table_names = list(set(table_names))

        con = duckdb.connect()
        # _ = con.execute("PRAGMA enable_object_cache;")
        if tmp_dir_path is not None:
            _ = con.execute(f"SET temp_directory='{tmp_dir_path}'")
        # load the files
        for table_name in table_names:
            locals()[table_name] = lance.dataset(lance_files_dict[table_name][0])

        for i, query in enumerate(queries):
            query_tag = get_query_tag(query)
            logger.info(f"query {i+1} / {query_count} : tag {query_tag}")
            if ((tpc_name == "tpch") and (i + 1 == 21) and (scale_factor == 100.0)) or (
                (tpc_name == "tpcds") and (i + 1 == 68) and (scale_factor >= 1.0)
            ):
                d = dict(
                    [
                        ("engine", "DuckDB"),
                        ("file_type", "lance"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", np.NaN),
                        ("elapsed_time_s", np.NaN),
                    ]
                )

            else:
                start_time_s = perf_counter()
                _ = con.execute(query)
                elapsed_time_s = perf_counter() - start_time_s
                logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                result = con.df()
                n_returned_rows = result.shape[0]

                d = dict(
                    [
                        ("engine", "DuckDB"),
                        ("file_type", "lance"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", n_returned_rows),
                        ("elapsed_time_s", elapsed_time_s),
                    ]
                )

            timings.append(d)

        con.close()

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df


def run_queries_hyper_on_hyper(subfolders, queries_hyper, logger):
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"Hyper / .hyper - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_hyper, scale_factor)
        query_count = len(queries)

        hyper_file_path = os.path.join(folder_path, "data.hyper")
        logger.info(f"Hyper file path : {hyper_file_path}")

        with HyperProcess(
            telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
        ) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                database=hyper_file_path,
                create_mode=CreateMode.NONE,
            ) as con:
                _ = con.execute_command("SET schema 'Export';")

                for i, query in enumerate(queries):
                    query_tag = get_query_tag(query)
                    logger.info(f"query {i+1} / {query_count} : tag {query_tag}")

                    start_time_s = perf_counter()
                    result = con.execute_query(query)
                    elapsed_time_s = perf_counter() - start_time_s
                    logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                    n_returned_rows = 0
                    while result.next_row():
                        n_returned_rows += 1
                        # print(result.get_values())
                    result.close()

                    d = dict(
                        [
                            ("engine", "Hyper"),
                            ("file_type", "hyper"),
                            ("scale_factor", scale_factor),
                            ("query", i + 1),
                            ("n_returned_rows", n_returned_rows),
                            ("elapsed_time_s", elapsed_time_s),
                        ]
                    )

                    timings.append(d)

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df


def run_queries_hyper_on_parquet(subfolders, queries_hyper, logger):
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"Hyper / .parquet - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_hyper, scale_factor)
        query_count = len(queries)

        parquet_file_paths = glob.glob(os.path.join(folder_path, "*.parquet"))
        parquet_file_count = len(parquet_file_paths)
        logger.info(f"Found {parquet_file_count} Parquet files")

        # list tables
        table_names = []
        parquet_files_dict = {}
        for parquet_file_path in parquet_file_paths:
            file_name = os.path.basename(parquet_file_path)
            table_name = os.path.splitext(file_name)[0]
            if table_name[-3:].isdigit():
                table_name = table_name[:-4]
            table_names.append(table_name)
            if table_name in parquet_files_dict:
                parquet_files_dict[table_name].append(parquet_file_path)
            else:
                parquet_files_dict[table_name] = [parquet_file_path]
        table_names = list(set(table_names))

        hyper_file_path = os.path.join(folder_path, "tmp.hyper")
        parameters = {}
        parameters["external_table_sample_size_factor"] = "0.005"
        with HyperProcess(
            telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
            parameters=parameters,
        ) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                database=hyper_file_path,
                create_mode=CreateMode.CREATE_AND_REPLACE,
            ) as con:
                for table_name in table_names:
                    file_array = ["'" + c + "'" for c in parquet_files_dict[table_name]]
                    if len(file_array) == 1:
                        file_array_str = file_array[0]
                    else:
                        file_array_str = ", ".join(file_array)
                        file_array_str = "ARRAY[" + file_array_str + "]"

                    q = f"""CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS {table_name}
                        FOR {file_array_str} """
                    _ = con.execute_command(q)

                for i, query in enumerate(queries):
                    query_tag = get_query_tag(query)
                    logger.info(f"query {i+1} / {query_count} : tag {query_tag}")

                    start_time_s = perf_counter()
                    result = con.execute_query(query)
                    elapsed_time_s = perf_counter() - start_time_s
                    logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                    n_returned_rows = 0
                    while result.next_row():
                        n_returned_rows += 1
                    result.close()

                    d = dict(
                        [
                            ("engine", "Hyper"),
                            ("file_type", "parquet"),
                            ("scale_factor", scale_factor),
                            ("query", i + 1),
                            ("n_returned_rows", n_returned_rows),
                            ("elapsed_time_s", elapsed_time_s),
                        ]
                    )
                    timings.append(d)

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df


def run_queries_datafusion_on_parquet(subfolders, queries_datafusion, logger):
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"Datafusion / .parquet - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_datafusion, scale_factor)
        query_count = len(queries)

        parquet_file_paths = glob.glob(os.path.join(folder_path, "*.parquet"))
        parquet_file_count = len(parquet_file_paths)
        logger.info(f"Found {parquet_file_count} Parquet files")

        # list tables
        table_names = []
        parquet_files_dict = {}
        for parquet_file_path in parquet_file_paths:
            file_name = os.path.basename(parquet_file_path)
            table_name = os.path.splitext(file_name)[0]
            if table_name[-3:].isdigit():
                table_name = table_name[:-4]
            table_names.append(table_name)
            if table_name in parquet_files_dict:
                parquet_files_dict[table_name].append(parquet_file_path)
            else:
                parquet_files_dict[table_name] = [parquet_file_path]
        table_names = list(set(table_names))

        # test
        #
        # runtime = (
        #     datafusion.RuntimeConfig().with_disk_manager_os().with_fair_spill_pool(10000000)
        # )
        # config = (
        #     datafusion.SessionConfig()
        #     .with_create_default_catalog_and_schema(True)  # .with_default_catalog_and_schema("foo", "bar")
        #     .with_target_partitions(1)
        #     .with_information_schema(True)
        #     .with_repartition_joins(False)
        #     .with_repartition_aggregations(False)
        #     .with_repartition_windows(False)
        #     .with_parquet_pruning(False)
        #     # .set("datafusion.execution.parquet.pushdown_filters", "true")
        # )
        # ctx = datafusion.SessionContext(config, runtime)

        # wrong initial version (e.g. config not being passed into the context)
        ctx = datafusion.SessionContext()
        # config = datafusion.Config()
        # config.set("datafusion.execution.parquet.enable_page_index", "true")
        # config.set("datafusion.execution.parquet.pushdown_filters", "true")
        # config.set("datafusion.execution.parquet.reorder_filters", "true")

        logger.info("Register the Parquet files")
        start_time_s = perf_counter()
        for table_name in table_names:
            if len(parquet_files_dict[table_name]) > 1:
                raise ValueError(
                    "Cannot handle multiple part Parquet files with Datafusion"
                )
            ctx.register_parquet(table_name, parquet_files_dict[table_name][0])
        elapsed_time_s = perf_counter() - start_time_s
        logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

        for i, query in enumerate(queries):
            query_tag = get_query_tag(query)
            logger.info(f"query {i+1} / {query_count} : tag {query_tag}")

            skip = False
            if (
                ((tpc_name == "tpch") and (i + 1 == 18) and (scale_factor == 30.0))
                or ((tpc_name == "tpch") and (i + 1 == 7) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 17) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 18) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 21) and (scale_factor == 100.0))
            ):
                skip = True

            if skip:
                d = dict(
                    [
                        ("engine", "Datafusion"),
                        ("file_type", "parquet"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", np.NaN),
                        ("elapsed_time_s", np.NaN),
                    ]
                )

            else:
                start_time_s = perf_counter()
                df = ctx.sql(query)
                result = df.collect()
                # import code

                # code.interact(local=dict(globals(), **locals()))
                elapsed_time_s = perf_counter() - start_time_s
                logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                n_returned_rows = 0
                for _, item in enumerate(result):
                    n_returned_rows += item.num_rows

                d = dict(
                    [
                        ("engine", "Datafusion"),
                        ("file_type", "parquet"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", n_returned_rows),
                        ("elapsed_time_s", elapsed_time_s),
                    ]
                )

            timings.append(d)

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df


def run_queries_ballista_on_parquet(subfolders, queries_datafusion, logger):
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"Ballista / .parquet - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_datafusion, scale_factor)
        query_count = len(queries)

        parquet_file_paths = glob.glob(os.path.join(folder_path, "*.parquet"))
        parquet_file_count = len(parquet_file_paths)
        logger.info(f"Found {parquet_file_count} Parquet files")

        # list tables
        table_names = []
        parquet_files_dict = {}
        for parquet_file_path in parquet_file_paths:
            file_name = os.path.basename(parquet_file_path)
            table_name = os.path.splitext(file_name)[0]
            if table_name[-3:].isdigit():
                table_name = table_name[:-4]
            table_names.append(table_name)
            if table_name in parquet_files_dict:
                parquet_files_dict[table_name].append(parquet_file_path)
            else:
                parquet_files_dict[table_name] = [parquet_file_path]
        table_names = list(set(table_names))

        ctx = pyballista.SessionContext("localhost", 50050)
        # ctx = pyballista.SessionContext()

        logger.info("Register the Parquet files")
        start_time_s = perf_counter()
        for table_name in table_names:
            if len(parquet_files_dict[table_name]) > 1:
                raise ValueError(
                    "Cannot handle multiple part Parquet files with Ballista"
                )
            ctx.register_parquet(table_name, parquet_files_dict[table_name][0])
        elapsed_time_s = perf_counter() - start_time_s
        logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

        for i, query in enumerate(queries):
            query_tag = get_query_tag(query)
            logger.info(f"query {i+1} / {query_count} : tag {query_tag}")

            skip = False

            # only run certain queries
            # if i + 1 != 1 or tpc_name != "tpch" or scale_factor != 1.0:
            #     skip = True

            if skip:
                d = dict(
                    [
                        ("engine", "Ballista"),
                        ("file_type", "parquet"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", np.NaN),
                        ("elapsed_time_s", np.NaN),
                    ]
                )

            else:
                start_time_s = perf_counter()
                df = ctx.sql(query)
                result = df.collect()
                # import code

                # code.interact(local=dict(globals(), **locals()))
                elapsed_time_s = perf_counter() - start_time_s
                logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                n_returned_rows = 0
                for _, item in enumerate(result):
                    n_returned_rows += item.num_rows

                d = dict(
                    [
                        ("engine", "Ballista"),
                        ("file_type", "parquet"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", n_returned_rows),
                        ("elapsed_time_s", elapsed_time_s),
                    ]
                )

            timings.append(d)

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df


def run_queries_datafusion_on_lance(subfolders, queries_datafusion, logger):
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"Datafusion / Lance - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_datafusion, scale_factor)
        query_count = len(queries)

        lance_file_paths = glob.glob(os.path.join(folder_path, "*.lance"))
        lance_file_count = len(lance_file_paths)
        logger.info(f"Found {lance_file_count} Lance files")

        if len(lance_file_count) == 0:
            logger.warning(f"No Lance files found in {folder_path}, skipping...")
            continue

        # list tables
        table_names = []
        lance_files_dict = {}
        for lance_file_path in lance_file_paths:
            file_name = os.path.basename(lance_file_path)
            table_name = os.path.splitext(file_name)[0]
            if table_name[-3:].isdigit():
                table_name = table_name[:-4]
            table_names.append(table_name)
            lance_files_dict[table_name] = lance_file_path
        table_names = list(set(table_names))

        ctx = datafusion.SessionContext()

        logger.info("Register the Lance files")
        start_time_s = perf_counter()
        for table_name in table_names:
            lance_path = lance_files_dict[table_name]
            # Read Lance dataset
            dataset = lance.dataset(lance_path)
            ctx.register_dataset(table_name, dataset)
            # # Convert to Arrow table
            # arrow_table = dataset.to_table()
            # # Register table with DataFusion
            # df = datafusion.from_arrow_table(arrow_table)
            # ctx.register_table(table_name, df)
        elapsed_time_s = perf_counter() - start_time_s
        logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

        for i, query in enumerate(queries):
            query_tag = get_query_tag(query)
            logger.info(f"query {i+1} / {query_count} : tag {query_tag}")

            skip = False
            if (
                ((tpc_name == "tpch") and (i + 1 == 18) and (scale_factor == 30.0))
                or ((tpc_name == "tpch") and (i + 1 == 7) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 17) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 18) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 21) and (scale_factor == 100.0))
            ):
                skip = True

            try:
                if skip:
                    d = dict(
                        [
                            ("engine", "Datafusion"),
                            ("file_type", "lance"),
                            ("scale_factor", scale_factor),
                            ("query", i + 1),
                            ("n_returned_rows", np.NaN),
                            ("elapsed_time_s", np.NaN),
                        ]
                    )

                else:
                    start_time_s = perf_counter()
                    df = ctx.sql(query)
                    result = df.collect()

                    elapsed_time_s = perf_counter() - start_time_s
                    logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                    n_returned_rows = 0
                    for _, item in enumerate(result):
                        n_returned_rows += item.num_rows

                    d = dict(
                        [
                            ("engine", "Datafusion"),
                            ("file_type", "lance"),
                            ("scale_factor", scale_factor),
                            ("query", i + 1),
                            ("n_returned_rows", n_returned_rows),
                            ("elapsed_time_s", elapsed_time_s),
                        ]
                    )
            except Exception as e:
                logger.exception(f"Error executing query {i+1} ", e)
                # logger.error(f"Error executing query {i+1} : {e}")
                d = dict(
                    [
                        ("engine", "Datafusion"),
                        ("file_type", "lance"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", np.NaN),
                        ("elapsed_time_s", np.NaN),
                    ]
                )

            timings.append(d)

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df


def run_queries_postgresql(subfolders, queries_postgresql, logger):
    timings = []

    cwd = os.getcwd()
    file_path = os.path.join(cwd, "pg_credentials.json")
    logger.info(f"credentials file path : {file_path}")

    with open(file_path) as json_file:
        auth = json.load(json_file)

    conn = psycopg2.connect(
        dbname=auth["database"],
        user=auth["username"],
        password=auth["password"],
        host=auth["server"],
        port=auth["port"],
    )

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        logger.info("==== BEGIN ====")
        logger.info(
            f"PostgreSQL - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        queries = get_queries(queries_postgresql, scale_factor)
        query_count = len(queries)

        use_schema_query = f"SET search_path TO '{folder_name}';"
        curs = conn.cursor()
        curs.execute(use_schema_query)
        conn.commit()
        curs.close()

        for i, query in enumerate(queries):
            query_tag = get_query_tag(query)
            logger.info(f"query {i+1} / {query_count} : tag {query_tag}")

            curs = conn.cursor()

            start_time_s = perf_counter()
            curs.execute(query)
            conn.commit()
            elapsed_time_s = perf_counter() - start_time_s
            logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

            data = curs.fetchall()
            n_returned_rows = len(data)

            curs.close()

            d = dict(
                [
                    ("engine", "PostgreSQL"),
                    ("file_type", np.NaN),
                    ("scale_factor", scale_factor),
                    ("query", i + 1),
                    ("n_returned_rows", n_returned_rows),
                    ("elapsed_time_s", elapsed_time_s),
                ]
            )

            timings.append(d)

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    conn.close()

    timings_df = pd.DataFrame(timings)
    return timings_df


from polars_queries import PL_QUERIES


def run_queries_polars_on_parquet(subfolders, queries_polars, logger):
    timings = []

    for folder_path in subfolders:
        start_time_step = perf_counter()
        folder_name = os.path.basename(os.path.normpath(folder_path))
        scale_factor = float(folder_name.split("_")[-1])
        if folder_name.startswith("tpch"):
            tpc_name = "tpch"
        elif folder_name.startswith("tpcds"):
            tpc_name = "tpcds"
        logger.info("==== BEGIN ====")
        logger.info(
            f"Polars / .parquet - folder : {folder_name}, scale_factor : {scale_factor}"
        )
        query_count = len(PL_QUERIES)

        parquet_file_paths = glob.glob(os.path.join(folder_path, "*.parquet"))
        parquet_file_count = len(parquet_file_paths)
        logger.info(f"Found {parquet_file_count} Parquet files")

        # list tables
        table_names = []
        parquet_files_dict = {}
        for parquet_file_path in parquet_file_paths:
            file_name = os.path.basename(parquet_file_path)
            table_name = os.path.splitext(file_name)[0]
            if table_name[-3:].isdigit():
                table_name = table_name[:-4]
            table_names.append(table_name)
            if table_name in parquet_files_dict:
                parquet_files_dict[table_name].append(parquet_file_path)
            else:
                parquet_files_dict[table_name] = [parquet_file_path]
        table_names = list(set(table_names))

        # Load Parquet files into Polars DataFrames
        dataframes = {}
        for table_name in table_names:
            if len(parquet_files_dict[table_name]) > 1:
                df = pl.concat(
                    [pl.scan_parquet(file) for file in parquet_files_dict[table_name]]
                )
            else:
                df = pl.scan_parquet(parquet_files_dict[table_name][0])
            dataframes[table_name] = df

        for i, query in enumerate(PL_QUERIES):
            logger.info(f"query {i+1} / {query_count} : tag {i}")

            skip = False
            if (
                ((tpc_name == "tpch") and (i + 1 == 18) and (scale_factor == 30.0))
                or ((tpc_name == "tpch") and (i + 1 == 7) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 17) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 18) and (scale_factor == 100.0))
                or ((tpc_name == "tpch") and (i + 1 == 21) and (scale_factor == 100.0))
            ):
                skip = True

            if skip:
                d = dict(
                    [
                        ("engine", "Polars"),
                        ("file_type", "parquet"),
                        ("scale_factor", scale_factor),
                        ("query", i + 1),
                        ("n_returned_rows", np.NaN),
                        ("elapsed_time_s", np.NaN),
                    ]
                )
            else:
                try:
                    start_time_s = perf_counter()
                    result = query(dataframes).collect(new_streaming=True)
                    # result = pl.SQLContext(dataframes).execute(query).collect()
                    elapsed_time_s = perf_counter() - start_time_s
                    logger.info(f"Elapsed time (s) : {elapsed_time_s:10.3f}")

                    n_returned_rows = len(result)

                    d = dict(
                        [
                            ("engine", "Polars"),
                            ("file_type", "parquet"),
                            ("scale_factor", scale_factor),
                            ("query", i + 1),
                            ("n_returned_rows", n_returned_rows),
                            ("elapsed_time_s", elapsed_time_s),
                        ]
                    )
                except Exception as e:
                    logger.error(f"Error executing query {i+1}: {str(e)}")
                    d = dict(
                        [
                            ("engine", "Polars"),
                            ("file_type", "parquet"),
                            ("scale_factor", scale_factor),
                            ("query", i + 1),
                            ("n_returned_rows", np.nan),
                            ("elapsed_time_s", np.nan),
                        ]
                    )

            timings.append(d)

        logger.info("====  END  ====")
        elapsed_time_step = perf_counter() - start_time_step
        logger.info(f"Elapsed time (s) : {elapsed_time_step:10.3f}")

    timings_df = pd.DataFrame(timings)
    return timings_df

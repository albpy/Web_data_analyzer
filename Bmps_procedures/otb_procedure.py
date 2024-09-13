import asyncpg

import json

from datetime import datetime

from collections import OrderedDict

import polars as pl

import traceback

import asyncio

import psycopg2

from sqlalchemy import create_engine, text

import time

import pyarrow as pa
import pyarrow.parquet as pq

from sqlalchemy import create_engine, schema, Table

from databases import with_db_connection


@with_db_connection
async def get_notices(conn):
    # Execute a query that generates notices
    await conn.execute("CREATE TEMP TABLE temp_table (id serial);")

    # Get the notices
    notices = conn.get_notices()

    # Print the notices
    for notice in notices:
        print(notice.message)


@with_db_connection
async def execute_stored_procedure(
    conn,
    history_date_from,
    history_date_to,
    forecast_date_from,
    forecast_date_to,
    sales_channel,
    product_family,
    sub_families,
    suppliers,
    category,
    sub_category,
    sku,
    top_items,
    store_class,
):
    print(time.process_time(), "start_to_execute main the stored procedure")

    top_items = int(max(top_items)) if top_items else None

    history_date_from = datetime.strptime(history_date_from, "%Y-%m-%d").date()
    history_date_to = datetime.strptime(history_date_to, "%Y-%m-%d").date()
    forecast_date_from = datetime.strptime(forecast_date_from, "%Y-%m-%d").date()
    forecast_date_to = datetime.strptime(forecast_date_to, "%Y-%m-%d").date()

    try:
        # Set the datestyle to ensure the correct date format
        await conn.execute("SET datestyle = 'ISO, DMY';")
        # Our query used to call get_sp_otb to with 2 parameters.
        query = """
            CALL get_sp_otb_1(
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
        """

        # Execute the stored procedure with parameters
        await conn.execute(
            query,
            history_date_from,
            history_date_to,
            forecast_date_from,
            forecast_date_to,
            sales_channel,
            product_family,
            sub_families,
            suppliers,
            category,
            sub_category,
            sku,
            top_items,
            store_class,
        )
        print("get_sp_otb executed")
        print(time.process_time(), "end of main the stored procedure")

    except:
        print(traceback.format_exc(), "sp_joins")


@with_db_connection
async def execute_stored_proc_sku_wise_calc(
    conn,
    forecast_date_from,
    forecast_date_to,
):

    forecast_date_from = datetime.strptime(forecast_date_from, "%Y-%m-%d").date()
    forecast_date_to = datetime.strptime(forecast_date_to, "%Y-%m-%d").date()

    try:
        # Our query used to call get_sp_otb to with 2 parameters.
        query = """
            CALL get_sp_otb_itemwise_calculations_1($1, $2)
                
                """

        await conn.execute(query, forecast_date_from, forecast_date_to)
        print("sku wise calc executed")
        print(time.process_time(), "end of sku wise stored proc")

    except:
        print(traceback.format_exc(), "sp_joins")


@with_db_connection
async def CallSubfilterProcedure(conn, table_name:str):
    print("subfilter collecting")

    try:
        # Our query used to call get_sp_otb to with 2 parameters.
        query = """
            CALL get_sp_otb_get_filter($1)
        """
        await conn.execute(
            query,
            table_name
        )
        print("sku wise calc executed")
        print(time.process_time(), "end of sku wise stored proc")

    except:
        print(traceback.format_exc())


@with_db_connection
async def get_apply_secondary_filters(conn, sub_filter_state: bool, secondary_filter: dict):
    print("The main grouping")
    print("we are calling main groupby process")

    try:
        # Our query used to call get_sp_otb to with 2 parameters.
        # params = table_name, secondary_filter, sub_filter_state, group,filter_condition
        # Wrapping the dictionary
        wrapped_dict = {"secondary_filter": secondary_filter}
        filter_data = json.dumps(wrapped_dict)
        print(filter_data, "json format")
        query = """
                CALL get_sp_otb_commit_secondary_filter(
                $1, $2)
        """
        await conn.execute(query, sub_filter_state, filter_data)

        print("secondary filter applied")
    except:
        print(traceback.format_exc(), "the main agg")


@with_db_connection
async def mainClusterCaller(
    conn,
    table: str,
    group: list,
    sub_filter_state: bool,
    mean_agg_cols: list,
    sum_agg_cols: list,
    distribution_cols: list,
    kpi_rank_cols: list,
    article_score_cols: list,
    coefficient_score: str,
    coefficient_score_mix_percent: str,
):
    print("we are calling main groupby process")
    print(table,
    group,
    sub_filter_state,
    mean_agg_cols,
    sum_agg_cols,
    distribution_cols,
    kpi_rank_cols,
    article_score_cols,
    coefficient_score,
    coefficient_score_mix_percent, 'main agg arguements')

    try:
        max_ = list()
        print(max_)
        # Our query used to call get_sp_otb to with 2 parameters.
        query = """
            CALL get_sp_otb_main_aggregation($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
        """
        await conn.execute(
            query,
            table,
            group or None,
            sub_filter_state,
            mean_agg_cols or None,
            sum_agg_cols or None,
            kpi_rank_cols or None,
            distribution_cols or None,
            article_score_cols or None,
            coefficient_score,
            coefficient_score_mix_percent,
        )

        print("main aggregation executed")
        print(time.process_time(), "end of sku wise stored proc")

    except asyncpg.exceptions.PostgresError as e:
        # Log or print the exact error
        print(f"Database error occurred: {e}")
        # Optionally, you can re-raise the exception or handle it as needed
        print(traceback.format_exc())

        raise



@with_db_connection
async def filter_data_for_expanding(conn, columns_to_filter, values_to_filter):
    print("filter data for drill down")

    try:
        # Our query used to call get_sp_otb to with 2 parameters.
        print(columns_to_filter, values_to_filter, "the expand heirarchy")
        query = """
            CALL get_sp_otb_drill_down_filter($1, $2)
        """
        await conn.execute(query, columns_to_filter, values_to_filter)
        print("sku wise calc executed")
        print(time.process_time(), "end of sku wise stored proc")

    except:
        print(traceback.format_exc())


@with_db_connection
async def get_sp_otb_expand_heirarchy(
    conn,
    table_name:str,
    group: list,
    mean_agg_cols: list,
    sum_agg_cols: list,
    distribution_cols: list,
    kpi_rank_cols: list,
    article_score_cols: list,
    max_cols: list,
    coefficient_score: str,
    coefficient_score_mix_percent: str,
    filter_columns, 
    filter_values
):
    """table -> item_counter_drilldown"""

    print("drilldown procedure start")

    try:
        # Our query used to call get_sp_otb to with 2 parameters.
        print(
            table_name,
            group,
            mean_agg_cols,
            sum_agg_cols,
            distribution_cols,
            kpi_rank_cols,
            article_score_cols,
            max_cols,
            coefficient_score,
            coefficient_score_mix_percent,
            filter_columns, 
            filter_values,
            "get_sp_otb_expand_heirarchy",
        )
        query = """
            CALL get_sp_otb_commit_drilldown($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """
        await conn.execute(
            query,
            table_name,
            group,
            mean_agg_cols,
            sum_agg_cols,
            distribution_cols,
            kpi_rank_cols,
            article_score_cols,
            max_cols,
            coefficient_score,
            coefficient_score_mix_percent,
            filter_columns, 
            filter_values,
        )
        print("sku wise calc executed")
        print(time.process_time(), "end of sku wise stored proc")

    except:
        print(traceback.format_exc())


@with_db_connection
async def editable_col_calculations(
    conn,
    row,
    columnID,
    newValue,
    columns_to_filter,
    values_to_filter,
    original,
    increase,
    child,
):
    
    print(row, "row is",
    columnID, "col id is",
    newValue, "newVal is",
    columns_to_filter, "cols to filter",
    values_to_filter, "values to filter",
    original, "original",
    increase, "increase",
    child,"child")

    try:
        row = json.dumps(row)

        query = """
            CALL get_sp_otb_editable_columns_operations($1, $2, $3, $4, $5, $6, $7, $8)
        """
        await conn.execute(
            query,
            row,
            columnID,
            newValue,
            columns_to_filter,
            values_to_filter,
            original,
            increase,
            child,
        )

        print("editable column op done")
        print(time.process_time(), "end of editable column stored proc")
    except:
        print(traceback.format_exc(), "editable column stored proc err")


@with_db_connection
async def execute_stored_proc_final_data_output(conn):
    print("we are calling final caculation stored proc for otb")

    try:
        # Our query used to call get_sp_otb to with 2 parameters.
        query = """
            CALL get_sp_otb_output_data_calc()
        """
        await conn.execute(query)
        print("sku wise calc executed")
        print(time.process_time(), "end of sku wise stored proc")

    except:
        print(traceback.format_exc())


@with_db_connection
async def getData_final(conn):

    try:
        # Fetch results from temp_result table
        records = await conn.fetch("""SELECT * FROM otb_main_data""")

        Rec_to_dict = lambda record: OrderedDict(record)

        dictionary_of_records = [Rec_to_dict(record) for record in records]
        print(dictionary_of_records[0:5], len(dictionary_of_records), "getData dictsize")

        final_data = pl.from_records(dictionary_of_records, infer_schema_length=10000000)

        decimal_cols = [col for col in final_data.columns if final_data[col].dtype == pl.Decimal]

        # Cast Decimal columns to Float64
        final_data = final_data.with_columns(
            [
                (
                    pl.col(col).cast(pl.Float64).fill_null(0)
                    if col in decimal_cols
                    else pl.col(col).fill_null(0)
                )
                for col in final_data.columns
            ]
        )
        print(time.process_time(), "end of fetch data")

    except:
        print(traceback.format_exc(), "fetching calculated itemwise ra_and_stock_temp")
    print(final_data, "the filter dataframe")
    return final_data


@with_db_connection
async def get_heirarchial_data(conn):

    try:
        # Fetch results from temp_result table
        records = await conn.fetch("""SELECT * FROM otb_drilled_out""")

        Rec_to_dict = lambda record: OrderedDict(record)

        dictionary_of_records = [Rec_to_dict(record) for record in records]
        print(dictionary_of_records[0:5], len(dictionary_of_records), "getData dictsize")

        heirarchial_data = pl.from_records(dictionary_of_records, infer_schema_length=10000000)

        decimal_cols = [
            col for col in heirarchial_data.columns if heirarchial_data[col].dtype == pl.Decimal
        ]

        # Cast Decimal columns to Float64
        heirarchial_data = heirarchial_data.with_columns(
            [
                (
                    pl.col(col).cast(pl.Float64).fill_null(0)
                    if col in decimal_cols
                    else pl.col(col).fill_null(0)
                )
                for col in heirarchial_data.columns
            ]
        )
        print(time.process_time(), "end of fetch data")

    except:
        print(traceback.format_exc(), "fetching calculated itemwise ra_and_stock_temp")
    print(heirarchial_data, "the filter dataframe")
    return heirarchial_data


# --------------------------------------------------------------------------------------------------------------------------------------
@with_db_connection
async def getSubfilterData(conn):

    try:
        # Fetch results from temp_result table
        records = await conn.fetch("""SELECT * FROM otb_sub_filter""")

        Rec_to_dict = lambda record: OrderedDict(record)

        dictionary_of_records = [Rec_to_dict(record) for record in records]
        print(dictionary_of_records[0:5], len(dictionary_of_records), "getData dictsize")

        filter_frame = pl.from_records(dictionary_of_records, infer_schema_length=10000000)

        decimal_cols = [
            col for col in filter_frame.columns if filter_frame[col].dtype == pl.Decimal
        ]

        # Cast Decimal columns to Float64
        filter_frame = filter_frame.with_columns(
            [
                (
                    pl.col(col).cast(pl.Float64).fill_null(0)
                    if col in decimal_cols
                    else pl.col(col).fill_null(0)
                )
                for col in filter_frame.columns
            ]
        )
        print(time.process_time(), "end of fetch data")

    except:
        print(traceback.format_exc(), "fetching calculated itemwise ra_and_stock_temp")
    print(filter_frame, "the filter dataframe")
    return filter_frame


"""---------------------------------------------------------------------------------------------------------------"""


@with_db_connection
async def getData(conn):

    # if not hasattr(getData, "has_been_called"):
    #     getData.has_been_called = True
    #     count = 0

    print("We are getting data from ra_and_stock")
    print(time.process_time(), "started to fetch data")
    # count+=1
    # print(count, "the_count")

    try:
        # Fetch results from temp_result table

        # query = f"SELECT * FROM {table}"
        records = await conn.fetch(f"""SELECT * FROM item_counter""")

        Rec_to_dict = lambda record: OrderedDict(record)
        print(Rec_to_dict, "dfafghaf")
        dictionary_of_records = [Rec_to_dict(record) for record in records]
        # print(dictionary_of_records[0], len(dictionary_of_records), '\n', len(dictionary_of_records[0]), "getData dictsize")

        # Convert the records to a Polars DataFrame
        df = pl.from_records(dictionary_of_records, infer_schema_length=10000000)
        # Create a list of Decimal columns to cast
        decimal_cols = [col for col in df.columns if df[col].dtype == pl.Decimal]

        # Cast Decimal columns to Float64
        df = df.with_columns(
            [
                (
                    pl.col(col).cast(pl.Float64).fill_null(0)
                    if col in decimal_cols
                    else pl.col(col).fill_null(0)
                )
                for col in df.columns
            ]
        )
        print(time.process_time(), "end of fetch data")

    except:
        print(traceback.format_exc(), "fetching calculated itemwise ra_and_stock_temp")
    print(df.shape, "output data shape")

    return df


@with_db_connection
async def getFilteredData(conn):

    # if not hasattr(getData, "has_been_called"):
    #     getData.has_been_called = True
    #     count = 0

    print("We are getting data from ra_and_stock")
    print(time.process_time(), "started to fetch data")

    try:
        # Fetch results from temp_result table

        # query = f"SELECT * FROM {table}"
        records = await conn.fetch(f"""SELECT * FROM otb_min_filter""")

        Rec_to_dict = lambda record: OrderedDict(record)
        print(Rec_to_dict, "dfafghaf")
        dictionary_of_records = [Rec_to_dict(record) for record in records]
        print(
            dictionary_of_records[0],
            len(dictionary_of_records),
            "\n",
            len(dictionary_of_records[0]),
            "getData dictsize",
        )

        df = pl.from_records(dictionary_of_records, infer_schema_length=10000000)

        decimal_cols = [col for col in df.columns if df[col].dtype == pl.Decimal]

        # Cast Decimal columns to Float64
        df = df.with_columns(
            [
                (
                    pl.col(col).cast(pl.Float64).fill_null(0)
                    if col in decimal_cols
                    else pl.col(col).fill_null(0)
                )
                for col in df.columns
            ]
        )
        print(time.process_time(), "end of fetch data")

    except:
        print(traceback.format_exc(), "fetching calculated itemwise ra_and_stock_temp")
    print(df.shape, "output data shape")

    return df


@with_db_connection
async def getMaindata(conn):

    try:
        # Fetch results from temp_result table

        # query = f"SELECT * FROM {table}"
        records = await conn.fetch(f"""SELECT * FROM otb_main_data""")

        Rec_to_dict = lambda record: OrderedDict(record)
        # print(Rec_to_dict, 'dfafghaf')
        dictionary_of_records = [Rec_to_dict(record) for record in records]
        print(
            dictionary_of_records[0],
            len(dictionary_of_records),
            "\n",
            len(dictionary_of_records[0]),
            "getData dictsize",
        )

        df = pl.from_records(dictionary_of_records, infer_schema_length=10000000)

        decimal_cols = [col for col in df.columns if df[col].dtype == pl.Decimal]

        # Cast Decimal columns to Float64
        df = df.with_columns(
            [
                (
                    pl.col(col).cast(pl.Float64).fill_null(0)
                    if col in decimal_cols
                    else pl.col(col).fill_null(0)
                )
                for col in df.columns
            ]
        )
        print(time.process_time(), "end of fetch data")

    except:
        print(traceback.format_exc(), "fetching calculated itemwise ra_and_stock_temp")
    print(df.shape, "output data shape")

    return df

@with_db_connection
async def check_table_exists(conn, table_name):
    query = """
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = $1
    );
    """
    result = await conn.fetchval(query, table_name)
    print(result, 'table_exists')
    return result

@with_db_connection
async def get_table_columns(conn, table_name):
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND table_name = $1;
    """
    columns = await conn.fetch(query, table_name)
    print([record['column_name'] for record in columns], 'columns of table')
    return [record['column_name'] for record in columns]


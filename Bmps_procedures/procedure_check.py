import psycopg2

# from sqlalchemy import create_engine, text
from sqlalchemy import create_engine, schema, Table
import traceback

import pandas as pd

import fastparquet

import numpy as np

from sqlalchemy.dialects import postgresql as pg
from pandas.api.types import DatetimeTZDtype



# db_url = 'postgresql+psycopg2://mohit:password@localhost:5433/bmaps' # 43.204.167.201
# engine = create_engine(db_url)

# try:
#     conn = engine.connect()
#     conn.execute(text("SELECT * FROM ra_and_stock_temp;"))

# except:
#     print(traceback.format_exc(), "writin skuID to the database")

# # def get_table():
# #     conn = None
# #     try:
# #         conn = psycopg2.connect(config)
# #         cur = conn.cursor()
# #         cur.execute("CALL get_sp_otb();")
# #         conn.commit()
# #         cur.close()
# #     except (Exception, psycopg2.DatabaseError) as error:
# #         print(error)
# #     finally:
# #         if conn is not None:
# #             conn.close()


# # get_table()
db_url = 'postgresql+psycopg2://mohit:password@localhost:5433/bmaps' # 43.204.167.201
# engine = create_engine(db_url)
# conn = engine.connect()

def __get_dtype(column, sqltype):
    # import sqlalchemy.dialects as sqld
    from sqlalchemy.types import (Integer, Float, Boolean, DateTime,
                                  Date, TIMESTAMP)

    if isinstance(sqltype, Float):
        return float
    elif isinstance(sqltype, Integer):
        # Since DataFrame cannot handle nullable int, convert nullable ints to floats
        if column.nullable:
            return float
        # TODO: Refine integer size.
        return np.dtype('int64')
    elif isinstance(sqltype, TIMESTAMP):
        # we have a timezone capable type
        if not sqltype.timezone:
            return np.dtype('datetime64[ns]')
        return DatetimeTZDtype
    elif isinstance(sqltype, DateTime):
        # Caution: np.datetime64 is also a subclass of np.number.
        return np.dtype('datetime64[ns]')
    elif isinstance(sqltype, Date):
        # Caution: np.datetime64 is also a subclass of np.number.
        # return np.datetime64('datetime64[ns]')
        return 'datetime64[ns]'

    elif isinstance(sqltype, Boolean) or isinstance(sqltype, pg.BOOLEAN):
        return bool
    elif isinstance(sqltype, pg.INTEGER) or isinstance(sqltype, pg.BIGINT):
        return np.dtype('int64')
    elif isinstance(sqltype, pg.FLOAT) or isinstance(sqltype, pg.NUMERIC):
        return np.dtype('float64')
    elif isinstance(sqltype, pg.VARCHAR) or isinstance(sqltype, pg.TEXT):
        return np.dtype('object')
    elif isinstance(sqltype, pg.DATE):
        return np.dtype('datetime64[D]')
    elif isinstance(sqltype, pg.TIMESTAMP):
        return np.dtype('datetime64[ns]')
    elif isinstance(sqltype, pg.BIT):
        return np.dtype('object') 
    # Catch all type - handle provider specific types in another elif block
    return object
def __write_parquet(output_path: str, batch_array, column_dict, write_index: bool,
                    compression: str, append: bool):
    # Create the DataFrame to hold the batch array contents
    b_df = pd.DataFrame(batch_array, columns=[str(col) for col in column_dict])
    # Cast the DataFrame columns to the sqlalchemy column analogues
    b_df = b_df.astype(dtype=column_dict)
    # Write to the parquet file (first write needs append=False)
    fastparquet.write(output_path, b_df,
                      write_index=write_index, compression=compression, append=append)

def table_to_parquet(output_path: str, table_name: str, con,
                     batch_size: int = 10000, write_index: bool = True,
                     compression: str = None):
    # Get database schema using sqlalchemy reflection
    db_engine = create_engine(con)
    # db_metadata = 
     # Create MetaData instance
    db_metadata = schema.MetaData()

    # Reflect the table using the engine
    db_metadata.reflect(bind=db_engine)
    # schema.MetaData(bind=db_engine)
    db_table = Table(table_name, db_metadata, autoload=True)

    # Get the columns for the parquet file
    column_dict = dict()
    for column in db_table.columns:
        print(column, column.type,'column type')
        dtype = __get_dtype(column, column.type)
        column_dict[column.name] = dtype

    # # Query the table
    # result = db_table.select().execute()

    # row_batch = result.fetchmany(size=batch_size)
    # append = False
# Query the table
    with db_engine.connect() as conn:
        result = conn.execute(db_table.select())
        row_batch = result.fetchmany(size=batch_size)
        append = False
        while(len(row_batch) > 0):
            __write_parquet(output_path, row_batch,
                            column_dict, write_index, compression, append)
            append = True

        row_batch = result.fetchmany(size=batch_size)

table_to_parquet('getData.parquet', table_name='item_counter', con=db_url)

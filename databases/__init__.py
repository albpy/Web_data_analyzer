import asyncpg
import traceback
from functools import wraps

async def get_db_connection():
    try:
        conn = await asyncpg.connect(
            user="postgres",
            password="admin",
            database="webdata_analyzer",
            host="localhost",
            port=5432,
        )
        print("Connection established")
        return conn
    except:
        print(traceback.format_exc(), "Failed to establish connection")
        return None

def with_db_connection(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        conn = await get_db_connection()
        if conn is None:
            print("Failed to establish database connection")
            return
        try:
            result = await func(conn, *args, **kwargs)
            return result
        except:
            print(traceback.format_exc())
        finally:
            await conn.close()
            print("Connection closed")
    return wrapper

## **** Testing ****

# import asyncio
# async def test_get_db_connection():
#     conn = await get_db_connection()
#     # await asyncio.gather(get_db_connection()) - TO run all the coroutine concurrently
#     if conn:
#         print("Connection object:", conn)
#         await conn.close()  # Make sure to close the connection after testing
#     else:
#         print("No connection object returned")
# asyncio.run(test_get_db_connection())
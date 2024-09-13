import asyncpg
import traceback
from functools import wraps

async def get_db_connection():
    try:
        conn = await asyncpg.connect(
            user="mohit",
            password="password",
            database="bmaps",
            host="localhost",
            port=5433,
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

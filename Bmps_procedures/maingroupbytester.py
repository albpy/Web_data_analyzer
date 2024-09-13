import asyncpg
import asyncio
import json

sf = {'family': [], 'sub_family': [], 'supplier': [], 'category': [], 'dom_comm': [], 'sub_category': [], 'extended_sub_category': [], 'sub_category_supplier': [], 'HistoricalYear': [], 'history_Day': [], 'history_Quarter': [], 'history_dates': [], 'history_month': [], 'history_week': [], 'BudgetYear': [], 'season': [], 'country': [], 'region': [], 'area': [], 'city': [], 'Store_Name': [], 'month': [], 'week': [], 'BudgetDate': [], 'Quarter': [], 'Day': [], 'Channel': [], 'article_score': []}

async def handle_notice(connection, pid, channel, payload):
    print(f"Notification received: {payload}")

async def main(sf):
    conn = await asyncpg.connect(
        user="mohit",
        password="password",
        database="bmaps",
        host="localhost",
        port=5433,
    )
    print("Connection established")
     # Add listener for notifications
    await conn.add_listener('notice', handle_notice)


    try:
        # Define the parameter to pass to the stored procedure
        # parameter_value = 123  # Replace with the actual value you want to pass
        
        # Our query used to call get_sp_otb_main_aggregation with 1 parameter.
        query = """
            CALL get_sp_otb_main_aggregation($1::json)
        """
        await conn.execute(query, json.dumps(sf))
        print("Stored procedure executed successfully")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await conn.close()
        print("Connection closed")

# Run the main function
asyncio.run(main(sf))

import pandas as pd
import json
import polars as pl

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from core.database import get_db
from .schemas import Filters, LoginData, Echelons
from .parameters import OTB
from .operations import Operations
from .kpi_analysis import kp_Operations

from fastapi.responses import JSONResponse, StreamingResponse

# from .query import otb_query, filter_details, change_percent, change_value

from .schemas import LoginData,ExportData

import traceback

import io
from typing import Optional
# from otb_calc import create_ra, form_base_data, inititalize_columns

# TEMP = {'export_data':None}


from routes.tinyfilters import get_subFilter_from_table
from otb_procedure import (
    execute_stored_procedure,
    get_apply_secondary_filters,
    editable_col_calculations,
    getSubfilterData,
    check_table_exists,
    CallSubfilterProcedure
)

# from rapidframes import QueryBuilder, RapiDF



otb = APIRouter(prefix="/otb")  # session = Session()
OTB = OTB()
Operations = Operations()
kp_operations = kp_Operations()

# import sys, os
# sys.stdout = open(os.devnull, 'w')
# Revising stores
# older data store
# updation_data = pl.DataFrame
# want_to_update_new_otb_mix = {'update' : updation_data}

# revised_budget_to_update = pl.DataFrame
# new_revised = {'revised_budget' :  revised_budget_to_update} # revised_budget

# do_revise = None
# do_revise_dict = {'do_revise':do_revise} # to do revice
# message = ''
# gp = None
# current_group_kpi = {'revising_group' : gp}

data_store = {}


max_col = OTB.MAX_COLS
avg_col = OTB.AVG_COLS
sum_col = OTB.SUM_COLS
float_cols = OTB.FLOAT_COLS
int_cols = OTB.INT_COLS
HEIRARCHY = OTB.HEIRARCHY
percent_col = OTB.PERCENT_COLS
tabs = json.dumps(OTB.TABS)
arts = OTB.SCORES
rank_col = OTB.RANK_COLS

DATA = pl.DataFrame()
TEMP = {"key": DATA, "update_flag": False, "data_filter": {}, 'export_data':None, "changed_columns":[]}
channel_flag = False

drill_down_cor = OTB.drill_down_cor
drill_down_display = OTB.drill_down_display
get_session_id = OTB.get_session_id
update_users_tables = OTB.update_users_tables
save_table = OTB.save_table

# *************Stock market
DATA = pd.DataFrame() 
DATA_DICT = {"default" : DATA, "requested" : DATA}
#**************************
@otb.get("/get_default")
async def get_default():
    DATA_DICT['default'] = pd.read_csv("bmps_data/stock_data/ADANIPORTS.csv")
    data_json = f"""{DATA_DICT['default'].to_json(orient='split')}"""
    return JSONResponse(content={"data" : data_json})

@otb.get("/get_requested_file")
async def get_requested_file(filename : Optional[str] = Query(None)):
    # DATA_DICT['requested'] = 
    path = "bmps_data/stock_data/"
    DATA_DICT["requested"] = pd.read_csv(path+filename)
    print(DATA_DICT["requested"].head())
    print(DATA_DICT["requested"].head()["Symbol"].unique())

    data_json = f"""{DATA_DICT['requested'].to_json(orient='split')}"""
    print(f"requested filename {filename}")
    return JSONResponse(content={"data" : data_json})


@otb.get("/sub_filters")
async def sub_filters():
    # print(OTB.SUB_FILTER, 'the_sub_fils')
    return JSONResponse(content=OTB.SUB_FILTER)

@otb.post("/export/to_excel", response_class=StreamingResponse)
async def export(drop_list:ExportData):

    data_to_stream = OTB.process_stream_data(TEMP,drop_list.hidden_cols)

    def get_response_data():
        output = io.BytesIO()
        data_to_stream.to_excel(output)
        output.seek(0)
        return output.getvalue()

    streaming_data = get_response_data()

    def get_from_stream():
        yield streaming_data

    headers = {
        "Content-Disposition": "attachment; filename=sp.xlsx",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    return StreamingResponse(get_from_stream(), headers=headers)

@otb.post("/save")
async def save_data(login_data: LoginData, db: Session = Depends(get_db)):
    data = TEMP["key"]
    wr = False
    # data = pl.from_pandas(data)
    # print(type(data), "cdcd")
    data = OTB.calculate_df(data, wr)
    # module_id = 'otb_table'
    module_id = "otb_table"
    table_name = module_id
    OTB.save_table_to_db(db, data, table_name)
    return JSONResponse(content={"message": "done"})


# Whether user need to update the revised
@otb.post("/update_otb_amount")
async def user_want_revised_amount(update: bool = True):
    TEMP["key"] = kp_operations.update_budget_amount(TEMP["key"])
    TEMP["update_flag"] = True


@otb.websocket("/get_data_ws")
async def get_data_ws(websocket: WebSocket, db: Session = Depends(get_db)):
    print("1")
    DATA = OTB.DATA
    await websocket.accept()
    try:
        while True:
            data_filter = await websocket.receive_json()
            print(data_filter, "SDFD")
            print(DATA_DICT["default"].columns)
            path = "bmaps_data/stock_data/" + data_filter['company']
            # file_path = 'data.json'

            # filters = Filters(**data_filter)
            # print(filters)
            # secondary = data_filter["secondary_filter"]
            # group_by_id = Echelons(**secondary)

            # secondary_filter = data_filter["secondary_filter"]

            if data_filter["cumulative"]==True:
                mx_cols = {col : "max" for col in max_col}
                avg_cols = {col : "mean" for col in avg_col}
                sm_cols = {col : "sum" for col in sum_col}
                # Merge dictionaries with priority to the last one if there are overlapping keys
                combined_dict = {**mx_cols, **avg_cols, **sm_cols}
                print(combined_dict)
                data = pd.read_csv(path)
                if data_filter['history_date_range']['fro'] is not None:
                    data = filter_date(data, data_filter)
                datas = data.agg(combined_dict) 
                datas = pd.DataFrame([datas])
            if data_filter["cumulative"]==False:
                if data_filter['history_date_range']['fro'] is not None:
                    data = pd.read_csv(path)
                    datas = filter_date(data, data_filter)
            
            print(datas)
            data_json = f"""{datas.to_json(orient='split')}"""            
            # print(data_json)
            # # json.dumps() function will convert a subset of Python objects into a json string.
            await websocket.send_text(data_json)
            # await websocket.send_text(json.dumps({"long":"hi"}))
            # response = {"message": "Data received", "received_data": json.dumps(data_filter)}
            # await websocket.send_text(json.dumps(response))
    # 
    except WebSocketDisconnect:
        # Handle disconnection gracefully
        print("WebSocket connection closed unexpectedly")

def filter_date(data, data_filter):
    data = data[(data['Date'] >= data_filter['history_date_range']['fro']) & (data['Date'] <= data_filter['history_date_range']['to'])]
    return data
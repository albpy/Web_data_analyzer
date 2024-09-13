import pandas as pd
import json
import polars as pl

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
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
            file_path = 'data.json'

            filters = Filters(**data_filter)
            print(filters)
            secondary = data_filter["secondary_filter"]
            group_by_id = Echelons(**secondary)

            secondary_filter = data_filter["secondary_filter"]

            
            if TEMP["update_flag"]:
                print("update_flag_true")
                DATA = TEMP["key"]
                TEMP["update_flag"] = False

            if data_filter["fetch_from_db"]:
                history_date_from = filters.history_date_range.fro
                history_date_to = filters.history_date_range.to
                forecast_date_from = filters.forecast_date_range.fro
                forecast_date_to = filters.forecast_date_range.to
                sales_channel = filters.sales_channel
                product_family = filters.product_family
                sub_families = filters.sub_families
                suppliers = filters.suppliers
                category = filters.category
                sub_category = filters.sub_category
                sku = filters.sku
                top_items = filters.top_items
                store_class = filters.top_items

                table_ch_selthru = None
                table_ch_mkd = None

                try:

                    await execute_stored_procedure(
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
                    # await check_table_exists('ra_sales_item_stock_kpi_joined')
                    # await get_table_columns('ra_sales_item_stock_kpi_joined')

                    # await execute_stored_proc_sku_wise_calc(forecast_date_from, forecast_date_to)
                    print("sku wise calculations done in stored procedure")
                    await check_table_exists('item_counter')

                    # DATA = await getData()

                except:
                    print(traceback.format_exc())


             
                await OTB.initial_frame_calculation(DATA)

                # print(DATA["INVOICEDATE"].unique(), "historical_year_values")

                # print(round(DATA.estimated_size("mb"), 2), " MB memory size of data step4")
                # print(round(DATA.estimated_size("gb"), 2), " GB memory size of data step4")
                # data = DATA
                global group
                group = []
            kpi_all_selection = data_filter["select_all_kpi"]
           

            sub_filter_state = False
            filter_condition = None
            # filter_condition, sub_filter_state, group = OTB.secondary_filter(
            #     DATA, filters, sub_filter_state, group, filter_condition
            # )
    
            TEMP['key'] = DATA


            if not sub_filter_state == True:
                group = []
                filter_condition = None
                sub_filter_state = False

            # if secondary_filter["article_score"] != []:
            #     DATA = OTB.call_kpi(DATA, data_filter)

            #'kpi_selection_flag'

            if data_filter["table_changes"] != {}:
                row = data_filter["table_changes"]["row"]
                columnID = data_filter["table_changes"]["columnId"]
                newValue = data_filter["table_changes"]["newValue"]

                if columnID == 'adjusted_markdown_percent':
                    table_ch_mkd = True
                if columnID == 'adjusted_sellthru_percent':
                    table_ch_selthru = True

                print('row is;', row), 
                print(columnID, newValue, "colid and new value")
                print(DATA.columns)

                # (
                #     child,
                #     other_filter_condition,
                #     filter_condition,
                #     parent,
                #     columns_to_filter,
                #     values_to_filter,
                #     group,
                #     DATA,
                # ) = OTB.table_change_filter(
                #     group, HEIRARCHY, data_filter, DATA, row, filter_condition
                # )
                group, sub_filter_state = OTB.secondary_filter(filters, sub_filter_state, group)
                print(group, 'table_group before the change filter')
                child, columns_to_filter, values_to_filter, group = OTB.table_change_filter(
                     group, HEIRARCHY, data_filter, row
                )
                print(child, "child")
                print(columns_to_filter, values_to_filter, 'table col val to filter')
                print(group, 'table_group')
                if row[columnID] == None:
                    original = 0
                else:
                    original  = row[columnID]
                increase = newValue - original
                print('increase is,', increase)
                # DATA, data = Operations.edit_tables(
                #     DATA,
                #     data,
                #     row,
                #     group,
                #     newValue,
                #     columnID,
                #     columns_to_filter,
                #     sub_filter_state,
                #     parent,
                #     child,
                #     other_filter_condition,
                #     filter_condition,
                # )
                await editable_col_calculations(row, columnID, newValue, columns_to_filter, values_to_filter, original, increase, child)
                
                
                # if columnID == "Check_box":

                #     (
                #         child,
                #         other_filter_condition,
                #         filter_condition,
                #         parent,
                #         columns_to_filter,
                #         values_to_filter,
                #         group,
                #         DATA,
                #     ) = OTB.table_change_filter(
                #         group, HEIRARCHY, data_filter, DATA, row, filter_condition
                #     )
                #     if child is None:  # and parent == None:
                #         DATA = kp_operations.apply_kpi_for_main_data(
                #             DATA, newValue
                #         )  # , row
                #     if child is not None:
                #         DATA = kp_operations.calculate_revised_budget(
                #             DATA, child, other_filter_condition, newValue
                #         )

            if kpi_all_selection != "":
                # manage kpi all selection option
                #     print(DATA.columns, 'select all columns')
                DATA = kp_operations.apply_kpi_for_main_data(DATA, int(kpi_all_selection))

            if data_filter["group_by"]["status"]:
                print("main grouping")
                
                sub_filter_state = any(values != [] for key, values in data_filter["secondary_filter"].items() if key != 'article_score')
                group, sub_filter_state = OTB.secondary_filter(filters, sub_filter_state, group)
                group, sub_filter_state = OTB.apply_heirarchial_filters(group_by_id, sub_filter_state, group)
                print(sub_filter_state, 'sfsss main')
                if sub_filter_state:
                    await get_apply_secondary_filters(sub_filter_state, data_filter["secondary_filter"])
                    await CallSubfilterProcedure('otb_min_filter')
                    df = await getSubfilterData()
                    print(df, "main sub filters")
                    OTB.SUB_FILTER = get_subFilter_from_table(df, OTB.SUB_FILTER, group)
                else:
                    await get_apply_secondary_filters(sub_filter_state, data_filter["secondary_filter"])
                    await CallSubfilterProcedure('item_counter')
                    df = await getSubfilterData()
                    print(df, "main sub filters")
                    OTB.SUB_FILTER = get_subFilter_from_table(df, OTB.SUB_FILTER, group)

  

                # data, filter_condition = await Operations.apply_group_by(
                #     # DATA,
                #     # data,
                #     data_filter,
                #     sub_filter_state,
                #     group,
                #     # filters,
                #     # filter_condition,
                # )
                    print(group, 'main')

                data = await Operations.apply_group_by(data_filter, sub_filter_state, group)
                
                df = await getSubfilterData()
                OTB.SUB_FILTER = get_subFilter_from_table(df, OTB.SUB_FILTER, group)
                # print(data.columns, "columns_after_main_gpby")
                # print(data['channel'])

            if data_filter["expand"]["status"]:  # Function to expand the channel

                sub_filter_state = any(values != [] for key, values in data_filter["secondary_filter"].items() if key != 'article_score')

                if sub_filter_state:
                    await get_apply_secondary_filters(sub_filter_state, data_filter["secondary_filter"])
                    await CallSubfilterProcedure('otb_min_filter')
                    df = await getSubfilterData()
                    print(df, "expand sub filters")
                    OTB.SUB_FILTER = get_subFilter_from_table(df, OTB.SUB_FILTER, group)
                else:
                    await get_apply_secondary_filters(sub_filter_state, data_filter["secondary_filter"])
                    await CallSubfilterProcedure('item_counter')
                    df = await getSubfilterData()
                    print(df, "main sub filters")
                    OTB.SUB_FILTER = get_subFilter_from_table(df, OTB.SUB_FILTER, group)



                group, sub_filter_state = OTB.secondary_filter(filters, sub_filter_state, group)
                group, sub_filter_state = OTB.apply_heirarchial_filters(group_by_id, sub_filter_state, group)

                print(group, 'exp')

                data = await Operations.expand_hierarchy(data_filter, sub_filter_state, group)
                df = await getSubfilterData()
                OTB.SUB_FILTER = get_subFilter_from_table(df, OTB.SUB_FILTER, group)

            try:
                wr = False

                data = OTB.calculate_df(data, wr, table_ch_mkd, table_ch_selthru)
                # await execute_stored_proc_final_data_output()
                # data = await getData_final()

                secondary = data_filter["secondary_filter"]
                scores_m = secondary["article_score"]
                art_cols = [f"pl.col('{col}').sum()" for a, col in arts.items() if a in scores_m]
                agg_dict = [eval(expr)for expr in [
                        f"pl.col('{col}').mean()" for col in avg_col if col in data.columns
                    ]
                    + [f"pl.col('{col}').sum()" for col in sum_col if col in data.columns]
                    + [f"pl.col('{col}').max()" for col in rank_col]
                    + [
                        f"pl.col('{col}').sum()"
                        for col in ["new_budget_mix", "revised_budget_amount"]
                        if col in data.columns
                    ]
                    + [
                        f"pl.col('{col}').mean()"
                        for a, col in arts.items()
                        if a in scores_m
                    ]
                    + [
                        f"pl.col('{col}').mean()"
                        for col in ["coefficient_score"]
                        if len(art_cols) != 0
                    ]
                    + [
                        f"pl.col('{col}').sum()"
                        for col in ["coefficient_score_mix_percent"]
                        if len(art_cols) != 0
                    ]
                    # + [f"pl.col('total_squ_count').n_unique()"]
                ]

                
                bottom_column = data.select(agg_dict)
                bottom_column = OTB.bottom_margin_calculation(bottom_column, table_ch_mkd, table_ch_selthru)
                # print(bottom_column.select("FirstMargin_percent"), "The bcolmn fm")
                try:
                    if type(bottom_column) != dict:
                        bottom_column = bottom_column.to_dict()
                        bottom_column = {
                            key: bottom_column[key][0] for key in list(bottom_column.keys())
                        }
                    bottom_column = pd.Series(bottom_column)
                    bottom_column[int_cols] = bottom_column[int_cols].fillna(0).astype(int)
                except:
                    print(traceback.format_exc(), 'the err')
                
                # bottom_column[int_cols]    = bottom_column[int_cols].fill_nan(0).cast(pl.Int64)
                # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
                # bottom_column[float_cols]  = bottom_column[float_cols].astype(float).round(2)
                try:    
                    bottom_column[float_cols] = bottom_column[float_cols].astype(float)
                except:
                    print(traceback.format_exc(), 'bo_co_ser')
            # bottom_column = bottom_column.to_pandas()
            except Exception as e:

                bottom_column = data

            TEMP["key"] = DATA
            size = len(data)
            if size == 1:
                editable_cols = json.dumps(
                    [
                        "Logistic%",
                        "DisplayItemQty",
                        "COR_EOLStock_value",
                        "adjusted_markdown_percent",
                        "adjusted_sellthru_percent",
                        # "DisplayItemValue",
                    ]
                )
            else:
                editable_cols = json.dumps(OTB.EDITABLE_COLS)

            if "sort" in data_filter:
                datas = Operations.sort_and_clean(data_filter, data, filters)
                TEMP['export_data'] = datas
                # sel_all_kpi = data_filter['select_all_kpi']
                kpi_all_selection = kp_operations.check_all_selection(kpi_all_selection)

                data_json = f"""{datas.to_json(orient='split')[:-1]}, "select_all_kpi":{json.dumps(kpi_all_selection)},"editable_cols":{editable_cols}, "percent_col":{json.dumps(percent_col)},"tabs":{tabs} ,"items":{size},"total":{bottom_column.to_json()} {datas.to_json(orient='split')[-1]}"""


                await websocket.send_text(data_json)
    
    except WebSocketDisconnect:
        # Handle disconnection gracefully
        print("WebSocket connection closed unexpectedly")



"""Size calculation in polars"""

# print(DATA.columns, 'of create_ra')
# print(round(DATA.estimated_size('mb'),2)," MB memory size of data step2")
# print(round(DATA.estimated_size('gb'),2)," GB memory size of data step2")

"""Function saves to db"""

# @otb.post("/save")
# async def save_data(login_data:LoginData):
#     df = TEMP['key']
#     table_name = 'otb_table'
#     OTB.save_table(df,table_name,'RA_DATA')
#     return JSONResponse(content={"message":"done"})

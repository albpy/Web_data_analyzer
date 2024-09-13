import numpy as np
import pandas as pd
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import text
import datetime
import calendar
import traceback
from typing import List,Dict,Optional,Union,Tuple
from .schemas import Filters, Echelons
from datetime import datetime
import polars as pl
import subprocess
# from rapidframes import RapiDF
from time import ctime
import os
# from otb_procedure import count_sku
from routes.tinyfilters import call_filter, get_subFilter_from_table
from otb_procedure import getData, execute_stored_proc_final_data_output, CallSubfilterProcedure, getSubfilterData

from routes.names import compositeCols, conversionCols, functionalFields, performanceCols, TABS, filterCols

class OTB():

    MAX_COLS: list = compositeCols.MAX_COLS
                        
    AVG_COLS: list    = compositeCols.AVG_COLS

    SUM_COLS: list    = compositeCols.SUM_COLS
    
    FLOAT_COLS: list  = conversionCols.FLOAT_COLS

    INT_COLS: list    = conversionCols.INT_COLS
    
    HEIRARCHY: list   = functionalFields.HEIRARCHY
    
    SUB_FILTER : dict = functionalFields.SUB_FILTER

    PERCENT_COLS: list = functionalFields.PERCENT_COLS
    
    TABS: dict        = {
                        'BudgetValue':     TABS.BudgetValue,
                                            
                        'BudgetCost':       TABS.BudgetCost,

                        'BudgetQuantity':   TABS.BudgetQuantity,
                                            
                        'BudgetMargin':     TABS.BudgetMargin
                        }
    

    EDITABLE_COLS : list = functionalFields.EDITABLE_COLS

    time_columns = ['historical_year', 'Budget_Year']

    SCORES: dict      = performanceCols.SCORES

    ARTICLE_SCORES : list = performanceCols.ARTICLE_SCORES

    RANK_COLS : list = performanceCols.RANK_COLS

    filter_details :dict  = filterCols.filter_details


    DATA              = pl.DataFrame()
    temp          = {}
    

    async def initial_frame_calculation(self,df:pl.DataFrame) ->Tuple[pl.DataFrame,pl.DataFrame]:
        '''
        Get data from DB to dataframe and preprocessing dataframe
            Args:
                df     : get a pl dataframe from rapidframe
            Returns:
                df     : data frame containing data
        '''
        print('processing...')
        
        group = []

        await CallSubfilterProcedure('item_counter')
        df = await getSubfilterData()
        print(df, "the_fiter_data")
        for co, st in zip(df.columns, df.dtypes):
            print(co, st)
            
        self.SUB_FILTER = get_subFilter_from_table(df, self.SUB_FILTER, group)
        wr=False
        # self.DATA = self.calculate_df(df,wr)
        # await execute_stored_proc_final_data_output()
        # self.DATA = await getData()
        # self.DATA[self.MAX_COLS]        = self.DATA[self.MAX_COLS].astype(str)
        # self.DATA[self.MAX_COLS] = self.DATA[self.MAX_COLS].cast(pl.Utf8)
        # print(self.DATA.select(pl.all().null_count()).to_dicts()[0],'lst DATA null')

        # return self.DATA

    def calculate_df(self, data: pl.DataFrame,wr:bool=False, table_ch_mkd: bool=False, table_ch_selthru: bool =False):
        '''
        Calculation on the Dataframe
            Args:
                df : DataFrame to be calculated
            Returns:
                data: Calculated data frame
        '''
        # print('calculating the df...')
        # print(data.dtypes, 'data_types')
        # print(data.describe(), 'data_info')

        try:
            data = data.with_columns((((pl.col('budget_amount')/pl.col('net_sales_ly')).replace({np.inf:0,-np.inf:0}).fill_nan(0))*100).alias('budget_vpy'))
            
            data = data.with_columns((((pl.col('budget_amount')/pl.col('net_sales_lly')).replace({np.inf:0,-np.inf:0}).fill_nan(0))*100).alias('budget_vppy'))
            
            total_budget = data.select(pl.col('budget_amount').sum())
            
            data = data.with_columns((pl.when(pl.col('budget_amount')/total_budget!=0).then((pl.col('budget_amount')/total_budget)*100).otherwise(0)).alias('relative_budget_percent'))

            data = data.with_columns(((pl.col('budget_amount')/pl.col('budget_amount').sum())*100).alias('budget_percent'))

            if wr:
                data = data.with_columns(((pl.col('budget_amount').sum()) *0.01* (pl.col('budget_percent'))).alias('budget_amount'))

            data = data.with_columns(
            pl.when(pl.col('net_sales_ly').is_null())
            .then(0)
            .otherwise((pl.col('ACT_FCT') / pl.col('net_sales_ly')).replace({np.inf: 0, -np.inf: 0}).fill_nan(0) * 100)
            .alias('LYvsACT_FCT_percent')
            )

            data = data.with_columns((((pl.col('ACT_FCT')/(pl.col('total_sku_count_ly'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))).alias('SALES_ACT_FCT_per_sku_ly'))

            data = data.with_columns((((pl.col('net_sales_lly')/(pl.col('net_sales_lly').sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))*100).alias('LLYvsACT_FCT_percent'))
            
            data = data.with_columns((((pl.col('ACT_FCT')/(pl.col('total_sku_count_lly'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))*100).alias('SALES_ACT_FCT_per_sku_lly'))

            data = data.with_columns(initial_average_retail_price = pl.col('budget_amount')/ pl.col('budget_qty'))

            data = data.with_columns(SupplyCost = pl.col('budget_cost')-(pl.col('budget_cost')*(pl.col('Logistic%')/100)))

            data = data.with_columns(((((pl.col('budget_amount')-pl.col('SupplyCost')))/(pl.col('budget_amount'))).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100).alias('FirstMargin_percent'))        

            data = data.with_columns((((pl.col('net_sales_ly') - pl.col('cost_of_goods_ly'))/pl.col('net_sales_ly')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)*100).alias('ly_margin_percent'))
            
            data = data.with_columns(((pl.col('net_sales_ly').fill_nan(0))-pl.col('cost_of_goods_ly')).alias('ly_margin'))

            data = data.with_columns(((pl.col('margin_act_forecast') /(pl.col('ly_margin')))*100).alias('margin_act_forecast_vs_ly_margin_percent'))

            data = data.with_columns(((pl.col('margin_act_forecast') /pl.col('total_sku_count'))).alias('margin_act_or_forecast_per_sku'))

            data = data.with_columns(((pl.col('budget_amount')-pl.col('budget_cost'))/pl.col('budget_amount') * 100).alias('budget_gross_margin_percent'))

            # data = data.with_columns(((pl.col('budget_amount')-pl.col('budget_cost'))/pl.col('budget_amount') * 100).alias('adjusted_budget_gross_margin_percent'))

            data = data.with_columns(((pl.col('budget_amount')-pl.col('budget_cost'))).alias('budget_gross_margin'))

            data = data.with_columns(((pl.col('budget_gross_margin')/(pl.col('ly_margin_percent')*100)).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100).alias('budget_vs_py_margin_percent'))

            data = data.with_columns(StockatRetailPrice = (pl.col('initial_average_retail_price') * pl.col('stock_on_hand_qty')).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))

            data = data.with_columns(StockatCostPrice = pl.col('stock_on_hand_qty') * (pl.col('initial_average_retail_price') * (1-(pl.col('ly_margin_percent')/100))))
            
            data = data.with_columns(DisplayItemValue = pl.col('DisplayItemQty')* pl.col("initial_average_retail_price"))

            data = data.with_columns((((pl.col('StockatRetailPrice')) - (pl.col('DisplayItemValue')) - (pl.col('COR_EOLStock_value'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('TYForecast'))       

            data = data.with_columns(GrossSales = (pl.col('budget_amount')/(100-pl.col('markdown_percent'))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0) * 100)
    #---
            data = data.with_columns(supply_retail_value = pl.col('SupplyCost')/(1-pl.col('FirstMargin_percent')/100))

            data = data.with_columns(proposed_sellthru_percent=((pl.col('cost_of_goods_ly')/(pl.col('cost_of_goods_ly')+pl.col('StockatCostPrice'))) * 100))
            
            data = data.with_columns(markdown_percent = pl.col('ly_customer_disc')/(pl.col('ly_customer_disc') + pl.col('net_sales_ly')) * 100)

            data = data.with_columns(markdown_percent = pl.col('markdown_percent').fill_nan(0))


            if table_ch_selthru == True:
                data = data.with_columns(retail_value_including_markdown = pl.col("supply_retail_value")/(1-(pl.col('markdown_percent')/100)))            
                data = data.with_columns(PurchaseRetailValueatGrossSale = pl.col('retail_value_including_markdown')/(pl.col('adjusted_sellthru_percent')/100))
                # print(data.select(pl.col(['adjusted_markdown_percent', 'adjusted_sellthru_percent', 'supply_retail_value', 'markdown_percent', 'retail_value_including_markdown'])), 'proposed sell thu check')
                # print(data.select(pl.col(['PurchaseRetailValueatGrossSale'])), 'proposed sell thu check1')

            elif table_ch_mkd == True:
                data = data.with_columns(retail_value_including_markdown = pl.col("supply_retail_value")/(1-(pl.col('adjusted_markdown_percent')/100)))
                data = data.with_columns(PurchaseRetailValueatGrossSale = pl.col('retail_value_including_markdown')/(pl.col('proposed_sellthru_percent')/100))
            
            else:
                print("we are in else... table check")
                data = data.with_columns(retail_value_including_markdown = pl.col("supply_retail_value")/(1-(pl.col('markdown_percent')/100)))
                data = data.with_columns(PurchaseRetailValueatGrossSale = pl.col('retail_value_including_markdown')/(pl.col('proposed_sellthru_percent')/100))
                data = data.with_columns(adjusted_sellthru_percent=(pl.col('retail_value_including_markdown')/(pl.col('PurchaseRetailValueatGrossSale'))*100))
                data = data.with_columns(MarkdownValue=pl.col("retail_value_including_markdown")-pl.col("supply_retail_value"))
                data = data.with_columns(adjusted_markdown_percent = 100-(pl.col('supply_retail_value')/pl.col('retail_value_including_markdown'))*100)
                # print(data.select(pl.col(['PurchaseRetailValueatGrossSale'])), 'proposed sell thu check1')

            
            data = data.with_columns(MarkdownValue=pl.col("retail_value_including_markdown")-pl.col("supply_retail_value"))

            # data = data.with_columns((((pl.col('PurchaseRetailValueatGrossSale'))/(pl.col('initial_average_retail_price'))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('OTBquantity'))
            data = data.with_columns(otb_retail_value_at_gross_sale = (pl.col('PurchaseRetailValueatGrossSale')-pl.col('TYForecast')))
            # print(data.select(pl.col(['otb_retail_value_at_gross_sale', 'PurchaseRetailValueatGrossSale', 'TYForecast'])))
            data = data.with_columns(OTBorPurchaseCost=(pl.col('otb_retail_value_at_gross_sale')*(1-(pl.col('FirstMargin_percent')/100)).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)))
            # data = data.with_columns(OTBorPurchaseCost = pl.col('OTBorPurchaseCost').cast(float).fill_nan(0))
            data = data.with_columns(OTBquantity=(pl.col('OTBorPurchaseCost')/((pl.col('budget_cost'))/(pl.col('budget_qty')))))

            print(data.select(pl.col(['adjusted_markdown_percent'])), 'the_disc')

        except:
            print(traceback.format_exc(), 'sku_wise calculations')


#********************************************Polaris*************************************************************************************************************************************
        return data
    
    def aggregation(self):
        agg_dict = ({col : 'mean' for col in self.AVG_COLS})
        agg_dict.update({col: 'sum' for col in self.SUM_COLS})
        return agg_dict
    

    
    # def secondary_filter(self,data: pl.DataFrame,filter_data: Filters,sub_filter_state:bool,
    #             group:List,filter_condition:[Optional]=None) ->Union[None,pl.Series,bool,List]:
    def secondary_filter(self, filter_data: Filters, sub_filter_state:bool,
                group:List,filter_condition:[Optional]=None) ->Union[None,pl.Series,bool,List]:
        '''
        Apply secondary filters to the data based on the provided filter_data.

        Args:
            data (pl.DataFrame): DataFrame to apply filters.
            filter_data (Filters): Filters containing secondary filter values.
            sub_filter_state (bool): State of sub-filters.
            group (List): List of selected group columns/sub filter columns.
            filter_condition (Optional): Existing filter condition.

        Returns:
            Union[None, pl.Series, bool, List]: Updated filter condition, sub_filter_state, and group.

        '''

        # Mapping between filter_data keys and column names
        # print("In the Secondary filter")
        key_map = {'Store_Name':'Store','region':'Region','Channel':'Channel','BudgetYear':'Budget_Year','Quarter':'budget_quarter',
        'month':'budget_month','week':'budget_week','Day':'budget_day','BudgetDate':'Budget_date','HistoricalYear':'historical_year','history_Quarter':'history_quarter',
        'history_month':'history_month','history_week':'history_week','history_Day':'history_day','history_dates':"INVOICEDATE", 'article_score' : OTB().SCORES
        }
    
        try:
            # print(filter_data.secondary_filter.dict(),"DICTIONARY")
            # Iterate over filter_data keys and values
            for key,values in filter_data.secondary_filter.dict().items():
                # Skip if values are empty
                if not values or key == 'article_score':
                    continue

                
                sub_filter_state = True
                # obtaind filter condition based on the key and values
                # if key_map[key] in ['Budget_Year']:
                    
                #     values = [str(i) for i in values]
                #     new_condition = data[key_map[key]].is_in(values)
                # else:
                #     # values = [str(i) for i in values]
                #     # print(key_map[key])
                #     # print(values)
                #     new_condition = data[key_map[key]].is_in(values)
                group.append(key_map[key])

                # if filter_condition  is None:
                #     filter_condition = new_condition
                #     # print(filter_condition,"FCC")
                # else:
                #     filter_condition = filter_condition & new_condition
                    # print(filter_condition,"FCC11")

        except Exception as e:
            print(traceback.format_exc())
            print(f"Error applyting filters:{e}")
        
        # If sub_filter_state is not True, reset group, filter_condition, and sub_filter_state
        print(sub_filter_state, 'sfs_sec_fil')
        if not sub_filter_state == True:
            group = []
            filter_condition = None
            sub_filter_state = False    
        return group, sub_filter_state #filter_condition,sub_filter_state,
    

    
    # def apply_heirarchial_filters(self,data: pl.DataFrame, group_by_id: Echelons,
    #                             sub_filter_state:bool, group:List,filter_condition:[Optional]=None) ->Union[None,pl.Series,bool,List]:
        
    def apply_heirarchial_filters(self, group_by_id : Echelons, sub_filter_state : bool, group : list):
    
        # print('we are applying hierarchial filter')
        group_map = {'family' : 'family', 'sub_family' : 'sub_family', 'supplier' : 'supplier' , 'category' : 'category_name', 'dom_comm' : 'dom_comm', 
        'sub_category' : 'sub_category', 'extended_sub_category' : 'extended_sub_category', 'sub_category_supplier' : 'sub_category_supplier'}
        try:
            for key, values in group_by_id.dict().items(): 
                if not values: continue

                # print(f'key is {key} & value is {values}')


                # sub_filter_state = True
                # new_condition = data[group_map[key]].is_in(values)
                
                group.append(group_map[key])

                # if filter_condition is None:
                #     filter_condition = new_condition
                # else:
                #     filter_condition = filter_condition & new_condition
        except Exception as e:
            print(traceback.format_exc())
            print(f"Error apply_heirarchial_filters:{e}")

        
        if not sub_filter_state == True:
            # print("Secondary filter apply :: subfilter state is false, group is empty")
            group = []
            filter_condition = None
            sub_filter_state = False
        else:
            print('Secondary filter is subfilter is true')
            print('group achieved is: ', group)



        # return filter_condition, sub_filter_state, group
        return group, sub_filter_state


    def table_change_filter(self, group:List, heirarchy:List, data_filter:Dict, row:Dict, DATA:Optional[pl.Series]=None,
                         filter_condition:Optional[pl.Series]=None) -> Union[None, List]:
        
        """
    Apply primary filter based on group/sub filter columns,heirarchy, and row data.

    Args:
        group (List): List of group columns.
        heirarchy (List): List of heirarchy columns.
        data_filter (Dict): Dictionary containing filter information.
        DATA (pl.DataFrame): Input DataFrame.
        row (Dict): Dictionary containing selected row data.
        filter_condition (Optional[pl.Series]): Existing filter condition.

    Returns:
        Union[None, pl.Series, List, pl.DataFrame]: Resulting filter condition, other filter condition, existing filter condition,
        parent filter condition, columns to filter, values to filter, group columns, and the input DataFrame.

        """

        columns_to_filter = []
        values_to_filter = []
        # Extract columns and values to filter based on the selected subfilters and hierarchy
        print(group, 'grrrp, table_change')
       
        for i in group+heirarchy:
            print(i)
            if i in row:
                print("in row")
                columns_to_filter.append(i)
                values_to_filter.append(row[i]) # data_filter['table_changes']["row"]
        child = filter_condition
        other_filter_condition = None
        parent = None
        
        child = True if columns_to_filter!=[] else False
        
        ############################################333
        # for col, val in zip(columns_to_filter, values_to_filter):
        #     if child is None:
        #         # print("in if")
        #         # print('ch_None')
        #         child = (DATA[col] == val)
        #         other_filter_condition = (DATA[col] != val)
        #         parent = None

        #     else:
        #         # print("in else")
        #         # print('Hv_chd')
        #         parent = child
        #         other_filter_condition = child & (DATA[col] != val)
        #         child = child & (DATA[col] == val)
        # print(child, 'and' ,parent)
        # return child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA
        return child, columns_to_filter, values_to_filter, group

    def change_percent(self, grouped_df,other_grouped_df,increase,colID):

        summation = grouped_df[colID].fill_nan(0).sum()
        #   grouped_df[colID] = colID + (colID*increase)/summation
        grouped_df = grouped_df.with_columns((grouped_df[colID]+(grouped_df[colID]*increase)/summation).alias(colID))
        #   unsummation = other_grouped_df[colID].sum()
        unsummation = other_grouped_df[colID].fill_nan(0).sum()
        
        other_grouped_df = other_grouped_df.with_columns((other_grouped_df[colID] - (other_grouped_df[colID]*increase)/unsummation).alias(colID))
        
        frames =  [grouped_df,other_grouped_df]
        
        df = pl.concat(frames)
        
        return df

    def  ue(self, grouped_df,other_grouped_df,  increase,colID):
        summation = grouped_df[colID].sum()
        # grouped_df[colID] = grouped_df[colID] + (grouped_df[colID]*increase)/summation
        # print(grouped_df.columns, 'grouped df cols')
        
        grouped_df = grouped_df.with_columns(pl.col('colID')+(pl.col('colID')*increase)/summation)
        
        return pl.concat([grouped_df,other_grouped_df])
                    

    def distribute_value(self, child_df, siblings_df, increase, columnID):
        child_sum =child_df[columnID].sum()
        child_df[columnID] =  (child_sum+increase)*(child_df[columnID]/child_sum)
        siblings_sum =siblings_df[columnID].sum()
        siblings_df[columnID] = (siblings_sum-increase)*(siblings_df[columnID]/siblings_sum)
        return child_df,siblings_df
    
    def drill_down_percentage(self,df,columnID,newValue,ref_col):
        df[columnID] = (df[ref_col]/df[ref_col].mean()) * newValue
        return df
    
    def drill_down_cor(self,df,columnID,newValue):

        # print(df.select(pl.col('StockatRetailPrice')).sum())

        df = df.with_columns(StockatRetailPrice = pl.col('StockatRetailPrice').replace({np.inf:0,-np.inf:0}).fill_null(0).fill_nan(0))

        df = df.with_columns((((pl.col('StockatRetailPrice')/pl.col('StockatRetailPrice').sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0).cast(pl.Float64) * newValue).alias(columnID))

        return df[columnID]

    def drill_down_display(self,df,columnID,newValue):

        df = df.with_columns(columnID = (pl.col('initial_average_retail_price')/pl.col('initial_average_retail_price').sum())*newValue)
        
        return df[columnID] 

    def get_max_session_id(email_address,module_id):
        email_address = email_address.replace('"',"'")
        module_id = module_id.replace('"',"'")

        query = f"""  SELECT "session_id","table_name" FROM users_tables
        WHERE "session_id" IN (SELECT COALESCE(MAX("session_id"),0) FROM users_tables WHERE "email_address" 
        = '{email_address}' AND module_id = '{module_id}') """
        return query

    def get_session_id(self,db: Session,email_address:str,module_id:str):

        result = db.execute(text(get_max_session_id(email_address,module_id)))
        rows = [list(row) for row in result]
        df = pd.DataFrame(columns=['session_id','table_name'], data=rows)
        values = df['session_id'].values
        table_maps = {'one':'two','two':'three','three':'one','zero':'one'}

        if values.size == 0:
            session_id = 1
            table_last_label = 'zero'
        else:
            table_name = df['table_name'].values[0]
            session_id = int(values[0]) + 1
            table_last_label = table_name.split('_')[-1]
        table_label = table_maps[table_last_label]
        return session_id,table_label


    def update_users_tables(self,db:Session,email_address,module_id,session_id,table_name):
        new_record = Users_tables(email_address=email_address,module_id=module_id,session_id=session_id,table_name=table_name)
        db.add(new_record)
        db.commit()
        db.refresh(new_record)

    def save_table_to_db(self,db:Session,df:pl.DataFrame,table_name:str):
        
        # print(df.columns)
        max_icols = ["description","Department","category_name","Family","SubFamily","sub_category",
                    "extended_sub_category","sub_category_supplier","assembly_code_nickname","ENDOFLife","dom_comm","status",
                    "Store","area",'Region',"history_quarter","history_week","history_month",
                    "Budget_Year","budget_day","supplier", 'budget_quarter', 'budget_week', 'budget_month']
        avg_icols = []
        sumi_cols = ['budget_amount',"budget_cost",'budget_qty', 'PurchaseRetailValueatGrossSale', 'OTBorPurchaseCost', 'OTBquantity', 
                     'stock_on_hand_qty'#, 'stock_on_hand_qty_ly', 'stock_on_hand_qty_lly'
                     ] #bud_am,cos ,qty
        subset = ["ITEMID","Channel","INVENTLOCATIONID","Budget_date"]      # otborcost, purcseat, otb qty  
        
        ap_columns = {'Channel' : 'channel',
                        'budget_amount':'budget_amount',
                        'PurchaseRetailValueatGrossSale' : 'otb_amount',
                        'OTBorPurchaseCost' : 'otb_cost',
                        'OTBquantity' : 'otb_qty',
                        # 'Budget_date':'BDate', #netsale,lly, ty, sales_cost_ly, SALESQTY
                        'Budget_Year':'budget_year',
                        'category_name':'category_name','budget_day':'budget_day','ITEMID':'ITEMID',
                        'budget_cost':'budget_cost','budget_qty':'budget_qty','budget_quarter':'budget_quarter',
                        'Store':'Store','budget_month':'budget_month','Region':'Region',
                        'SubFamily':'SubFamily','budget_week':'budget_week',
                        'stock_on_hand_qty' : 'stock_qty_actual',
                        'stock_on_hand_qty_ly' : 'stock_qty_ly',
                        'stock_on_hand_qty_lly' : 'stock_qty_lly'
                        }
        
        # columns = ["Channel","INVENTLOCATIONID","ITEMID","Budget_date","PurchaseRetailValueatGrossSale","OTBorPurchaseCost","OTBquantity",'budget_amount','budget_cost',"budget_qty"]
        # df = pl.from_pandas(df[columns])
        # sum_cols = [f"pl.col('{col}').sum()" for col in ["PurchaseRetailValueatGrossSale","OTBorPurchaseCost","OTBquantity",'budget_amount','budget_cost',"budget_qty"]]
        
        # agg_dict = [eval(expr) for expr in sum_cols]
        # df = df.groupby(["Channel","INVENTLOCATIONID","ITEMID","Budget_date"]).agg(agg_dict)
        # df = df.rename({'Channel' : 'channel','Budget_date':'BDate','budget_amount':'budget_amount','budget_cost':'BudgetCost','budget_qty':'BudgetQTY'})
        # # return None
        # df =df.to_pandas()
        # df.to_csv(f"{table_name}.tsv",header=True,index=False,sep='\t')
        agg_dict = [eval(expr) for expr in [f"pl.col('{col}').mean()" for col in avg_icols ]
                    + [f"pl.col('{col}').sum()" for col in sumi_cols] +
                     [f"pl.col('{col}').max()" for col in max_icols]]
        # print(round(df.estimated_size('gb'),2)," GB memory size of data")

        df = df.group_by(subset).agg(agg_dict)
        # print(df.columns, 'colllls')

        df = df.to_pandas()
        # print(df.columns.to_list(), 'boooooooo')
        df = df[["ITEMID","description","Department","category_name","family","SubFamily","sub_category",
		"extended_sub_category","sub_category_supplier","assembly_code_nickname","ENDOFLife","dom_comm","status",
		"Channel","INVENTLOCATIONID","Store","area",'Region','budget_amount', 'budget_cost','budget_qty',
        "budget_quarter","budget_week", 'budget_month','Budget_date',"Budget_Year",'budget_day',"supplier",
        'PurchaseRetailValueatGrossSale', 'OTBorPurchaseCost', 'OTBquantity', 'stock_on_hand_qty', 'stock_on_hand_qty_ly',
        'stock_on_hand_qty_lly'
        ]]
        



        df = df.rename(columns=ap_columns)
        # df.head(0).to_sql(table_name, con=db.bind, if_exists='replace', index=False)
        # print(df.columns.to_list(), 'sooooooooooo')
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # print(f"{ctime()} - csv conversion begins")
        df.to_csv(f"{table_name}.csv",header=True,index=False,sep='\t')
        # print(f"{ctime()} - csv conversion ends")
        # print(f"{ctime()} - writing data to db begins")

        
        
        with db.bind.raw_connection().cursor() as cursor:
            try:
                cursor.execute(f"TRUNCATE TABLE {table_name};")
                with open(f'{table_name}.csv', 'r',) as file:
                    cursor.execute("BEGIN;")
                    cursor.copy_expert(sql = f"COPY {table_name} FROM STDIN WITH DELIMITER E'\t' CSV HEADER;",file=file)
                cursor.execute("COMMIT;")
            except:
                print(traceback.format_exc())

                cursor.connection.rollback()
            finally:
                cursor.close()
                os.remove(f'{table_name}.csv')
        # print(f"{ctime()} - writing data to db ends")
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


        # df.head(1).to_sql(table_name,con=db.bind,if_exists='replace',index=False,method=None, chunksize=5000)
        # db.commit()


    def save_table(self,df:pl.DataFrame,table_name:str,LIBRARY_NAME:str):
        df = df.to_pandas()
        # return None
        STORAGE_URL = 'lmdb://databases/bmaps_data?map_size=50GB'
        rf = RapiDF(STORAGE_URL)
        lib = rf.get_library(LIBRARY_NAME, create_if_missing=True)
        lib.write(table_name, df)
    

    def bottom_margin_calculation(self, df:pl.DataFrame, table_ch_mkd : bool = None, table_ch_selthru : bool = None):
        print("we are at bottom margin calculations")

        try:
            # Ensure required columns exist and have valid data
            required_columns = [
                'cost_of_goods_ly', 'stock_cost_ly', 'net_sales_ly', 'budget_amount', 'SupplyCost',
                'net_sales_lly', 'initial_average_retail_price', 'stock_on_hand_qty', 'DisplayItemValue',
                'COR_EOLStock_value', 'ly_customer_disc', 'budget_cost', 'budget_qty'
            ]
            
            # missing_columns = [col for col in required_columns if col not in df.columns]
            # if missing_columns:
            #     raise ValueError(f"Missing columns in the dataframe: {missing_columns}")
            
            # Fill NaN and None values in all relevant columns
            for col in required_columns:
                # df = df.with_columns(pl.col(col).fill_nan(0).fill_none(0).alias(col))
                print(df.select(pl.col(col)), 'coll inspection')

        except:
            print(traceback.format_exc(), 'djhfsajd')
        try:

            df = df.with_columns(initial_average_retail_price = pl.col('budget_amount')/ pl.col('budget_qty'))

            df = df.with_columns(ly_margin_percent = ((pl.col('net_sales_ly')-pl.col('cost_of_goods_ly'))/pl.col('net_sales_ly'))*100)
            # df = df.with_columns(SupplyCost = pl.col('budget_cost')-(pl.col('budget_cost')*(pl.col('Logistic%')/100)))
            # df = df.with_columns(SupplyCost = pl.col('SupplyCost').sum())
            df = df.with_columns((100 - (pl.col('SupplyCost')/(pl.col('budget_cost')))*100).alias('Logistic%'))
            # Logistic% = (sum(budget_cost) - sum(SupplyCost))/sum(budget_cost) * 100
            df = df.with_columns(((((pl.col('budget_amount')-pl.col('SupplyCost')))/(pl.col('budget_amount'))).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100).alias('FirstMargin_percent'))        
            df = df.with_columns(budget_vpy = (((pl.col('budget_amount')/pl.col('net_sales_ly')).replace({np.inf:0,-np.inf:0}).fill_nan(0))*100))
            df = df.with_columns(budget_vppy = (((pl.col('budget_amount')/pl.col('net_sales_lly')).replace({np.inf:0,-np.inf:0}).fill_nan(0))*100))
            # df = df.with_columns(PurchasedRetailValueGrossSale = ((pl.col('budget_amount')/(100-pl.col('markdown_percent')))/(pl.col('proposed_sellthru_percent')/100)).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)*100)
            # df = df.with_columns(PurchasedRetailValueGrossSale = pl.col('initial_average_retail_price') * pl.col('stock_on_hand_qty'))
            df = df.with_columns(StockatRetailPrice = (pl.col('initial_average_retail_price') * pl.col('stock_on_hand_qty')).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))

            # df = df.with_columns(StockatRetailPrice = (pl.col('initial_average_retail_price') * pl.col('stock_on_hand_qty')).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
            # df = df.with_columns(TYForecast = ((pl.col('PurchasedRetailValueGrossSale')) - (pl.col('DisplayItemValue')) - (pl.col('COR_EOLStock_value'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
            df = df.with_columns(TYForecast = ((pl.col('StockatRetailPrice')) - (pl.col('DisplayItemValue')) - (pl.col('COR_EOLStock_value'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
            df = df.with_columns(((pl.col('budget_amount')-pl.col('budget_cost'))/pl.col('budget_amount') * 100).alias('budget_gross_margin_percent'))
            # df = df.with_columns(((pl.col('budget_amount')-pl.col('budget_cost'))/pl.col('budget_amount') * 100).alias('adjusted_budget_gross_margin_percent'))


            # df = df.with_columns(PurchaseRetailValueatGrossSale = ((pl.col('TYForecast')-pl.col('PurchasedRetailValueGrossSale'))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
            df = df.with_columns(LYvsACT_FCT_percent = ((pl.col('net_sales_lly')/(pl.col('net_sales_lly').sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))*100)
            df = df.with_columns(markdown_percent = pl.col('ly_customer_disc')/(pl.col('ly_customer_disc') + pl.col('net_sales_ly')) * 100)
            df = df.with_columns(markdown_percent = pl.col('markdown_percent').fill_nan(0))

            # df = df.with_columns(adjusted_markdown_percent = pl.col('Discount')/(pl.col('budget_amount') + pl.col('Discount')))
            # df = df.with_columns(MarkdownValue = (pl.col('ly_customer_disc') + pl.col('net_sales_ly')) * pl.col('markdown_percent'))
            
            df = df.with_columns(supply_retail_value = pl.col('SupplyCost')/(1-(pl.col('FirstMargin_percent')/100)))
            # "MarkdownValue" = (supply_retail_value) * markdown_percent,
            df = df.with_columns(MarkdownValue=pl.col("supply_retail_value")/(1-pl.col("markdown_percent")/100)-pl.col("supply_retail_value"))
            # print(df.select(pl.col(['supply_retail_value', 'markdown_percent'])))
            df = df.with_columns(StockatCostPrice = pl.col('stock_on_hand_qty') * (pl.col('initial_average_retail_price') * (1-(pl.col('ly_margin_percent')/100))))

            df = df.with_columns(proposed_sellthru_percent=((pl.col('cost_of_goods_ly')/(pl.col('cost_of_goods_ly')+pl.col('StockatCostPrice'))) * 100))
            if table_ch_selthru == True:
                print("editing table sell thru")
                df = df.with_columns(retail_value_including_markdown = pl.col("supply_retail_value")/(1-(pl.col('markdown_percent')/100)))            
                df = df.with_columns(PurchaseRetailValueatGrossSale = pl.col('retail_value_including_markdown')/(pl.col('adjusted_sellthru_percent')/100))
                

            elif table_ch_mkd == True:
                print('table_ch_mkd is true')
                df = df.with_columns(retail_value_including_markdown = pl.col("supply_retail_value")/(1-(pl.col('adjusted_markdown_percent')/100)))
                df = df.with_columns(PurchaseRetailValueatGrossSale = pl.col('retail_value_including_markdown')/(pl.col('proposed_sellthru_percent')/100))
                df = df.with_columns(adjusted_markdown_percent = 100-(pl.col('supply_retail_value')/pl.col('retail_value_including_markdown'))*100)

            else:
                df = df.with_columns(retail_value_including_markdown = pl.col("supply_retail_value")/(1-(pl.col('markdown_percent')/100)))
                df = df.with_columns(PurchaseRetailValueatGrossSale = pl.col('retail_value_including_markdown')/(pl.col('proposed_sellthru_percent')/100))
                df = df.with_columns(adjusted_markdown_percent= pl.col('markdown_percent'))

            df = df.with_columns(adjusted_sellthru_percent=(pl.col('retail_value_including_markdown')/(pl.col('PurchaseRetailValueatGrossSale'))*100))
            df = df.with_columns(MarkdownValue=pl.col("supply_retail_value")/(1-pl.col("adjusted_markdown_percent")/100)-pl.col("supply_retail_value"))
            print(df.select(pl.col(['MarkdownValue', 'supply_retail_value', 'adjusted_markdown_percent'])), 'check teh mkdvalue')
            
            print(df.select(pl.col(['PurchaseRetailValueatGrossSale', 'proposed_sellthru_percent','retail_value_including_markdown'])), 'seltru botoom not showing up')
            print(df.select(pl.col(['cost_of_goods_ly', 'StockatCostPrice','markdown_percent', 'supply_retail_value'])), 'seltru botoom not showing up')

            # df = df.with_columns((((pl.col('cost_of_goods_ly')/(pl.col('cost_of_goods_ly')+pl.col('stock_cost_ly'))).replace({np.inf:0, -np.inf:0}).fill_nan(0))*100).alias('proposed_sellthru_percent'))   

            # df = df.with_columns(OTBorPurchaseCost = (pl.col('PurchaseRetailValueatGrossSale')*(1-(pl.col('FirstMargin_percent')/100)).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)))
            df = df.with_columns(otb_retail_value_at_gross_sale = (pl.col('PurchaseRetailValueatGrossSale')-((pl.col('TYForecast')))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

            df = df.with_columns(OTBorPurchaseCost = (pl.col('otb_retail_value_at_gross_sale')*(1-(pl.col('FirstMargin_percent')/100)).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)))
            
            df = df.with_columns(OTBquantity=(pl.col('OTBorPurchaseCost')/((pl.col('budget_cost'))/(pl.col('budget_qty')))))
            df = df.with_columns(DisplayItemValue = pl.col('DisplayItemQty')* pl.col("initial_average_retail_price"))

            print(df.select(pl.col(['adjusted_markdown_percent'])), 'the_disc')

            # df = df.with_columns(OTBquantity = ((pl.col('PurchaseRetailValueatGrossSale'))/(pl.col('initial_average_retail_price'))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
            # df = df.with_columns(OTBquantity = ((pl.col('PurchaseRetailValueatGrossSale'))/(pl.col('initial_average_retail_price'))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
        
        except:
            print(traceback.format_exc())
        print("bottom mg calc completed")
       

        return df

    def call_kpi(self,df: pl.DataFrame,data_filter : Dict):

        """
        
        Calculate Coefficient scores for the given DataFrame.

        Parameters:
        - df (pandas.DataFrame): The DataFrame containing relevant data.
        - data_filter (dict): A dictionary containing filters for KPI calculations.
        - sku (int): The SKU value.
        - subset: The subset value.

        returns:
        pandas.DataFrame: A DataFrame with calculated KPI scores.
        """
        
        
        total = len(df)
        average_len = 0       
        df = df.with_columns((pl.lit(0.0)).alias("coefficient_score"))

        secondary_filter = data_filter['secondary_filter']
        if secondary_filter['article_score'] != []:
            try:
                
                for scores in secondary_filter['article_score']:
    #  ARTICLE_SCORES : list = ['sale', 'abc', 'ae', 'speed', 'terminal', 'margin', 'sell', 'markdown', 'core', 'quartile', 'sortimeter']
   
                    if scores == 'sale':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sale")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))
                    if scores == 'abc':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_abc")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'ae':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_ae")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'speed':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_speed")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'terminal':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_terminal")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'margin':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_margin")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'sell':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sell")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'markdown':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_markdown")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'core':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_core")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'quartile':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_quartile")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'sortimeter':
                        average_len += 1  
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sortimeter")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))
              
                df = df.with_columns(((pl.col("coefficient_score")/average_len).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))
                df = df.with_columns(coefficient_score_mix_percent = (pl.col('coefficient_score')/pl.col('coefficient_score').sum()).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100)

            except Exception as e:

                print(f"Error:{e}")
                print(traceback.format_exc())

        return df

    def process_stream_data(self,TEMP:dict,cols_to_hide:List) -> pd.DataFrame:
        """
        Processed data to export using export api.

        Args:
            TEMP: A dictionary containing data and configuration information.
                - 'export_data': The raw data for export.
                - 'changed_columns': A list indicating which budget columns were changed (used for dropping).
                - 'filter': A dictionary containing filter information (including the tab name).
                - 'total_row': A Pandas Series representing the total row data.
            cols_to_hide: A list of column names to hide from the final DataFrame.

        Returns:
            A Pandas DataFrame containing the processed and formatted budget data.
        """

        # handle export dynamic budget columns and their name
        datas = TEMP['export_data']
        if TEMP['changed_columns']:
            start = max(TEMP['changed_columns'])
        else:
            start = 0
        # drop the columns if not added from frontend through input
        # for i in range(start+1,6):
        #     datas = datas.drop(f"BudgetAmount-{i}%")
        #     datas = datas.drop(f"BudgetAmount-{i}")
        #     datas = datas.drop(f"Quantity-{i}")
        #     datas = datas.drop(f"BudgetMargin-{i}")
        #     datas = datas.drop(f"Budget_GrossMargin-{i}%")
        #     datas = datas.drop(f"BudgetCostofGoods-{i}")
        #     datas = datas.drop(f"Deficit-{i}")

        # rename budget related columns to front end display name
        columns = datas.columns
        # name_dict = dict(zip(SP.Budget_name_cols,SP.Budget_rename_cols))
        drop_dict = {}
        datas = datas[[cols for cols in self.TABS['BudgetValue'] if cols in columns]]
        # for cols in columns:
        #     if cols not in set(self.TABS['BudgetValue']).union(self.TABS['BudgetQuantity']).union(self.TABS['BudgetCost']).union(self.TABS['BudgetMargin']):
        #         datas.drop(cols, inplace= True)

        # # filter required columns
        # if TEMP['filter']['tab_name'] == 'BudgetValue':
        #     orderd_columns = [col for col in self.TABS["BudgetValue"] if col in columns]
        #     datas = datas[orderd_columns].rename(drop_dict)

        # if TEMP['filter']['tab_name'] == 'BudgetCost':
        #     orderd_columns = [col for col in self.TABS["BudgetCost"] if col in columns]
        #     datas = datas[orderd_columns].rename(drop_dict)

        # if TEMP['filter']['tab_name'] == 'BudgetQuantity':
        #     orderd_columns = [col for col in self.TABS["BudgetQuantity"] if col in columns]
        #     datas = datas[orderd_columns].rename(drop_dict)

        # if TEMP['filter']['tab_name'] == 'BudgetMargin':
        #     orderd_columns = [col for col in self.TABS["BudgetMargin"] if col in columns]
        #     datas = datas[orderd_columns].rename(drop_dict)

        
        # total_row = TEMP['total_row'].to_frame().transpose()


        # # handle columns availablity and unavailability for total row
        # for col in total_row:
        #     if col not in orderd_columns:
        #         total_row = total_row.drop(col,axis=1)
        # for col in orderd_columns:
        #     if col not in total_row.columns:
        #         if col in self.MAX_COLS:
        #             total_row[col] = ""
        #         elif col in self.FLOAT_COLS:
        #             total_row[col] = 0.0
        #         elif col in self.INT_COLS:
        #             total_row[col] = 0
        #         else:
        #             total_row[col] = ""
        # total_row.index=["Total"]
        # datas = pd.concat([datas.to_pandas(),total_row[orderd_columns].rename(columns=drop_dict)])  
        # columns = datas.columns

        # # rename columns to display columns
        # rename_columns = [col.replace("_"," ").upper() for col in columns]  
        # datas = datas.rename(columns=dict(zip(columns,rename_columns)))
        # cols_to_hide = [col.upper() for col in cols_to_hide]
        # # hide hidden cols
        # cols_to_hide = set(datas.columns).intersection(set(cols_to_hide))   
        # if cols_to_hide:
        #     return  datas.drop(cols_to_hide,axis=1)
        # else:
        return datas
#----------------------------------------------Formulas---------------------------------------------------
# 'Deficit'                         = (('budget_amount')-('final_price')) *('budget_qty')

# SalesActualsByForecast            = (sales_actual/budget_amount)*100

# gross_sales                       =  (budget_amount/(100-markdown_percent))

# initial_average_retail_price      = ('History_Net_Sales'/'SALESQTY')* stock_on_hand_qty

# SupplyCost                        = Budget Cost of Goods - (Budget Cost of Goods * Logistics% )

# Original_BudgetAmount             = budget_amount

# StockatRetailPrice                = 'initial_average_retail_price'*stock_on_hand_qty'   

# 'SKU_COUNT'                       = 'ITEMID'

# 'unit_buy_by_sku'                 = 'net sales'/total_sku_count

# 'MarkdownValue'                   = 'gross_sales' - 'budget_amount'

# 'PurchasedRetailValueGrossSale'   =  'budget_amount'/(1-(100-pl.col('proposed_sellthru_percent')))

# 'budget_qty'                      = 'budget_amount'/'final_price'

# budget_gross_margin_percent       = ('Original_BudgetAmount'*100)-('Original_BudgetCostofGoods')/('Original_BudgetAmount')

# 'OTBorPurchaseCost'               = 'PurchaseRetailValueatGrossSale' * (100 - 'FirstMargin_percent')

# PurchasedRetailValueGrossSale     = gross_sales * (proposed_sellthru_percent)/100

# PurchaseRetailValueatGrossSale    = PurchasedRetailValueGrossSale  - TYForecast

# PurchasedRetailValueGrossSale     = budget_amount/(1 - ((100 - proposed_sellthru_percent)/100))

# PO                                =  budget_amount + (initial_average_retail_price*stock_on_hand_qty)

# budget_gross_margin_percent       = (100*(budget_amount-BudgetCostofGoods))/budget_amount

# COR_EOLStock_value           = (('StockatRetailPrice)/('StockatRetailPrice.sum())) * newValue













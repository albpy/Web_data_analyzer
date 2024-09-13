import polars as pl
import numpy as np
import traceback
import subprocess
import time
import pandas as pd
from .parameters import OTB
from .schemas import Filters
from typing import Dict, List, Optional, Union, Tuple
from routes.getFiltergroups import getFilterdetails
from otb_procedure import (mainClusterCaller, get_apply_secondary_filters, getMaindata, 
                        filter_data_for_expanding, get_sp_otb_expand_heirarchy, get_heirarchial_data,
                        getSubfilterData)
from routes.tinyfilters import call_filter, get_subFilter_from_table


max_col    = OTB.MAX_COLS
avg_col    = OTB.AVG_COLS
sum_col    = OTB.SUM_COLS
float_cols = OTB.FLOAT_COLS
int_cols   = OTB.INT_COLS
HEIRARCHY  = OTB.HEIRARCHY
rank_col   = OTB.RANK_COLS
arts       = OTB.SCORES
cut_cols   = []
OTB = OTB()

filters = getFilterdetails()

class Operations:
#    def edit_tables():

#     return DATA, data 

    def edit_tables(self, DATA : pl.DataFrame, data : pl.DataFrame, row : Dict, group : List, newValue : int, 
    columnID : str, columns_to_filter : List, sub_filter_state : bool, parent : [Optional] = None, child : [Optional] 
    = None,other_filter_condition:[Optional]=None, filter_condition:[Optional]=None) ->pl.DataFrame:
        """
        This function edits tables based on the provided parameters and returns the updated DataFrame.

        Args:
            DATA (pl.DataFrame): The main DataFrame to be edited.
            data (pl.DataFrame): The output DataFrame.
            row (Dict): The selected row from front end.
            group (List): List of selected subfilters columns.
            newValue (int): The new value to be set.
            columnID (str): The ID of the column to be edited.
            columns_to_filter (List): Columns to be used for filtering/hierachical columns.
            sub_filter_state (bool): Sub-filter state , indicating sub_filter selected or not.
            parent: [Optional] = None): Parent filter condition ,series of bool/None.
            child: [Optional] = None): Child filter condition/child location, series of bool/None.
            other_filter_condition: [Optional] = None): Other filter condition/sibling location, series of bool/None.
            filter_condition: [Optional] = None): Filter condition ,series of bool/None..

            Returns:
                pl.DataFrame: The updated DataFrame.

            """
        # print(columnID, 'col-id')
        if row[columnID] == None:
                original = 0
        else:
            original  = row[columnID]
        increase = newValue - original 
        if columnID == 'Logistic%':
            # DATA = DATA.with_columns(((pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100)))).alias('SupplyCost'))
            # DATA = DATA.with_columns((((pl.col('budget_amount')-pl.col('SupplyCost'))*100)/pl.col('budget_amount')).alias('FirstMargin%'))
            # DATA = DATA.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))).alias('OTBorPurchaseCost'))
            # print("Logistic column is edited")
            try:              

                # DATA['Logistic%'].loc[child] = newValue
                if child is None:
                    # print(parent.value_counts(), 'parent_logistics')
                    # print('Logistic child is none')
                    DATA = DATA.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('Logistic%'))
                    
                    DATA = DATA.with_columns(((pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100)))).alias('SupplyCost'))
                    DATA = DATA.with_columns((((pl.col('budget_amount')-pl.col('SupplyCost'))*100)/pl.col('budget_amount')).alias('FirstMargin%'))
                    DATA = DATA.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))).alias('OTBorPurchaseCost'))
                else:
                    data_child = DATA.filter(list(child))
                    data_child = data_child.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('Logistic%'))
                    data_child = data_child.with_columns(((pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100)))).alias('SupplyCost'))
                    data_child = data_child.with_columns((((pl.col('budget_amount')-pl.col('SupplyCost'))*100)/pl.col('budget_amount')).alias('FirstMargin%'))
                    data_child = data_child.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))).alias('OTBorPurchaseCost'))
                    
                    data_other = DATA.filter(list(child.not_()))

                    DATA = pl.concat([data_child, data_other])

#                     
            except Exception as e:
                print(f"Error Logistic_p is: {e}")
                # DATA['Logistic%'] = newValue
                DATA = DATA.with_columns(pl.lit(newValue).alias('Logistic%'))

        if columnID == 'DisplayItemQty':
            try:
                # DATA['DisplayItemQty'].loc[child] = drill_down_display(DATA.loc[child],columnID,newValue)
                DATA_child = DATA.filter(list(child))
                DATA_non_child = DATA.filter(list(child.not_()))
                # print(OTB.drill_down_display(DATA.filter(list(child)),columnID,newValue), 'drill_dw_dis of DisplayItem')
                DATA_child = DATA_child.with_columns(DisplayItemValue = OTB.drill_down_display(DATA.filter(list(child)),columnID,newValue))
                DATA = pl.concat([DATA_child,DATA_non_child])
                # print(DATA, "Concated DisplayItem")

                # try:
                #     DATA['DisplayItemQty'].loc[child] = newValue
                # except:
                #     DATA['DisplayItemQty'] = newValue
            except Exception as e:

                # print(f"Error DisplayItem is: {e}")
                # DATA = DATA.with_columns(DisplayItem = OTB.drill_down_display(DATA,columnID,newValue))
                # DATA = DATA.with_columns(DisplayItemQty = (pl.col('initial_average_retail_price')/pl.col('initial_average_retail_price').sum()).replace({np.inf:0,-np.inf:0}).fill_nan(0)*newValue)
                DATA = DATA.with_columns(DisplayItemValue = (pl.col('initial_average_retail_price')*newValue).replace({np.inf:0,-np.inf:0}).fill_nan(0))
                print(DATA.select(pl.col(["DisplayItemValue", 'DisplayItemQty', 'initial_average_retail_price']).mean()), "disp values")

                # DATA['DisplayItemQty'].value_counts().write_csv('stretpr.csv')
                # print(DATA.select(pl.col(['initial_average_retail_price', 'DisplayItemQty']).sum()), newValue, 'xxxxxx')
                # print(DATA['DisplayItemQty'].sum(), 'Disscplay')
                # print(DATA['DisplayItemQty'].value_counts())
        # if columnID == 'adjusted_budget_gross_margin_percent':
        #     DATA['adjusted_budget_gross_margin_percent'].loc[child] = newValue
        
        if columnID == 'COR_EOLStock_value':
            # try:
            # print(child,'child')
                            
            # DATA['COR_EOLStock_value'].loc[child] = drill_down_cor(DATA.loc[child],columnID,newValue)
            # data1 = DATA.group_by(list(set(['Channel'])))#.agg(agg_dict) # ,  maintain_order = True
            # for name, data in data1:
            #     print(name)
            #     print(data.select(pl.col(['Channel', 'proposed_sellthru_percent', 'PurchaseRetailValueatGrossSale', 'TYForecast', 'OTBorPurchaseCost', 'PurchasedRetailValueGrossSale'])), 'corr')
            #     print(data.select(pl.col(['Channel', 'proposed_sellthru_percent', 'PurchaseRetailValueatGrossSale', 'TYForecast', 'OTBorPurchaseCost', 'PurchasedRetailValueGrossSale']).sum()), 'corr')
           
            try:
                # print(DATA['COR_EOLStock_value'].filter(list(child)), 'ccch')
                
                DATA_child = DATA.filter(list(child))
                DATA_non_child = DATA.filter(list(child.not_()))
                
                DATA_child = DATA_child.with_columns((((pl.col('StockatRetailPrice')/pl.col('StockatRetailPrice').sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0).cast(pl.Float64) * newValue).alias(columnID))
                
                DATA_child = DATA_child.with_columns(StockatRetailPrice = pl.col('StockatRetailPrice').fill_null(0).replace({np.inf:0,-np.inf:0}).fill_nan(0).cast(pl.Float64))
                
                DATA_child = DATA_child.with_columns(TYForecast = pl.col('StockatRetailPrice')-pl.col('DisplayItemValue')-pl.col('COR_EOLStock_value').cast(pl.Float64))
                DATA_child = DATA_child.with_columns(PurchasedRetailValueGrossSale = (pl.col('gross_sales') * (pl.col('proposed_sellthru_percent'))/100).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
                DATA_child = DATA_child.with_columns((((pl.col('PurchasedRetailValueGrossSale'))-pl.col('TYForecast')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('PurchaseRetailValueatGrossSale'))
                DATA_child = DATA_child.with_columns(OTBorPurchaseCost = pl.col('PurchaseRetailValueatGrossSale')-((100-pl.col('FirstMargin%'))))
                DATA_child = DATA_child.with_columns((((pl.col('PurchaseRetailValueatGrossSale'))/(pl.col('initial_average_retail_price'))).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('OTBquantity'))

                
                # df = df.with_columns(columnID = pl.col('StockatRetailPrice').cast(pl.Float64)/pl.col('StockatRetailPrice').cast(pl.Float64).fill_nan(0).sum()) * int(newValue)

                # DATA_child = DATA_child.with_columns(COR_EOLStock_value = OTB.drill_down_cor(DATA.filter(list(child)),columnID,newValue))
                # print(OTB.drill_down_cor(DATA.filter(list(child)),columnID,newValue), 'drill_dw_cor of DisplayItem')
                DATA_non_child = DATA_non_child.with_columns(COR_EOLStock_value = pl.col('COR_EOLStock_value').cast(pl.Float64))
                DATA = pl.concat([DATA_child,DATA_non_child])
                # print(DATA, 'Concated COR_EOLStock_value')

            except Exception as e:
                # print('Exception CoRR', e, traceback.format_exc())#.replace({np.inf:0,-np.inf:0}).fill_nan(0).cast(pl.Int32) * newValue
                DATA = DATA.with_columns((((pl.col('StockatRetailPrice')/pl.col('StockatRetailPrice').sum()).replace({np.inf:0,-np.inf:0}).fill_nan(0)) * newValue).alias(columnID))
                # print(newValue, 'news')
                # print(DATA[columnID].sum(), DATA['StockatRetailPrice'].sum(), 'sto,cor')
                # print(f"Error COR_EOLStock_value is: {e}")
        if columnID == 'markdown_percent':
            try:
                # DATA['markdown_percent'].loc[child] = newValue
                DATA_child = DATA.filter(list(child))
                # print("Data_child", DATA_child)
                DATA_child_other = DATA.filter(list(child.not_()))
                # print("Data_Child_not", DATA_child_other)
                DATA_child = DATA_child.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('markdown_percent'))
                # print(DATA_child, "ADded DATA markdown")
                DATA = pl.concat([DATA_child, DATA_child_other])
                # print(DATA, 'Markdown concated data')
            except Exception as e:
                # print(f"Error Markdown is: {e}")
                # DATA['markdown_percent'] = newValue
                DATA = DATA.with_columns(pl.lit(newValue).alias('markdown_percent'))
                # print(DATA, "Markdown except %")
        if columnID == 'proposed_sellthru_percent':
            # print(DATA['initial_average_retail_price'].sum(), 'init_av_ret_price')
            # print(DATA['PurchaseRetailValueatGrossSale'].is_null().sum(), 'PurchaseRetailValueatGrossSale')
            # print(DATA['PurchasedRetailValueGrossSale'].is_null().sum(), 'PurchaseRetailValueGrossSale')
            
            try:
                # DATA['proposed_sellthru_percent'].loc[child] = newValue
                data_child = DATA.filter(list(child))
                data_other = DATA.filter(list(child.not_()))
                data_child = data_child.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('proposed_sellthru_percent'))
                
                data_child =data_child.with_columns(PurchasedRetailValueGrossSale = pl.col('budget_amount')/(1-((100-pl.col('proposed_sellthru_percent'))/100)))
                data_child =data_child.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
                data_child =data_child.with_columns(OTBorPurchaseCost = pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))
                # print(set(data_child.columns)-set(data_other.columns),'yyyy')
                # print(set(data_other.columns)-set(data_child.columns))
                DATA = pl.concat([data_child,data_other])
                # print(DATA, 'Concated proposed_sellthru_percent')

            except Exception as e:
                # print(f"Error ProposedSellThru is: {e}")
                # DATA['proposed_sellthru_percent'] = newValue
                DATA = DATA.with_columns(pl.lit(newValue).alias('proposed_sellthru_percent'))
                # DATA.with_columns(newValue = pl.col('proposed_sellthru_percent'))
                # print(DATA, "ProposedSellThru except")
                

        if columnID == 'act_forecast_vs_budget_percent':
            # print(f'Column_id is:: {columnID}')
            # print(newValue, 'newvalue')

            budget_amount = (newValue*row['ACT_FCT'])/100
            # print(budget_amount, 'New_bud_amnt')

            # print(child.value_counts(), 'child_act_fct')
            # print(other_filter_condition.value_counts(), 'other_act_fct')
            # print('There is parent::', parent) 
            budget_amount_sum = DATA['budget_amount'].sum()
            # newValue = (DATA.loc[child]['RelativeBudget%'].sum()*budget_amount)/DATA.loc[child]['budget_amount'].sum()
            newValue = (100*budget_amount)/budget_amount_sum
            columnID = 'budget_percent'
            data_child = DATA.filter(list(child))
            data_other = DATA.filter(list(child.not_()))
            # DATA = OTB.change_percent(grouped_df=DATA.filter(list(child)), other_grouped_df=DATA.filter(list(other_filter_condition)), increase=increase, colID=columnID)

            # data_child = data_child.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('act_forecast_vs_budget_percent'))
            # data_child = data_child.with_columns(((pl.col('act_forecast_vs_budget_percent')*pl.col('sales_actual'))/100).alias('budget_amount')) 
            # data_child = data_child.with_columns(((pl.col('budget_amount')/pl.col('budget_amount').sum())*100).alias('budget_percent'))

            # DATA = pl.concat([data_child, data_other])
            
            # print(DATA.select(pl.all().is_null().sum()).to_dicts()[0], 'print the nulls od DATA after edited')
            # print(DATA['act_forecast_vs_budget_percent'].value_counts(), 'the data after agg')

        if columnID == 'budget_qty':
            # budget_amount = newValue*row['Price']
            budget_amount = newValue*row['initial_average_retail_price']
            if parent is None: budget_amount_sum = DATA['budget_amount'].sum()
            # else: budget_amount_sum = DATA.loc[parent]['budget_amount'].sum()
            else:
                budget_amount_DATA = DATA.filter(list(parent))
                budget_amount_sum = budget_amount_DATA['budget_amount'].sum()
            if budget_amount < budget_amount_sum:
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = 'budget_percent'
        if columnID == 'budget_vpy':
            budget_amount = newValue*row['History_Net_Sales']
            if parent is None: budget_amount_sum = DATA['budget_amount'].sum()
            # else: budget_amount_sum = DATA.loc[parent]['budget_amount'].sum()
            else:
                budget_amount_DATA = DATA.filter(list(parent))
                budget_amount_sum = DATA['budget_amount'].sum()
            if budget_amount < budget_amount_sum:
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = 'budget_percent'
        if columnID == 'adjusted_budget_gross_margin_percent':
            budget_amount =row['BudgetCostofGoods']/(1- 0.01*newValue)
            if parent is None: 
                # print('parent is', parent)
                # print(DATA, 'DATA______')
                budget_amount_sum = DATA['budget_amount'].sum()
            else: 
                # print('There is parent is:', parent)
                # budget_amount_sum = DATA.loc[parent]['budget_amount'].sum()
                budget_amount_DATA = DATA.filter(list(parent))
                budget_amount_sum = DATA['budget_amount'].sum()
            if budget_amount < budget_amount_sum:
                # print('budget_amount < budget_amount_sum')
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = 'budget_percent'

        # if columnID == 'Adjusted Budget Gross Margin%':
        #     DATA.loc[child] = drill_down_percentage(DATA.loc[child],'Adjusted Budget Gross Margin%',newValue,'Budget Gross Margin%')

        if columnID == 'budget_percent':

            s = data['relative_budget_percent'].sum()
            increase = (s*newValue/100) - (s*row[columnID]/100)

            if len(columns_to_filter) == 1:
                print('len(columns_to_filter)==1')
                # DATA.loc[child], DATA.loc[other_filter_condition] = OTB.distribute_value(child_df=DATA.loc[child],siblings_df=DATA.loc[other_filter_condition],increase=increase,columnID =columnID)
                # DATA = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                DATA = OTB.change_percent(grouped_df=DATA.filter(list(child)), other_grouped_df=DATA.filter(list(other_filter_condition)), increase=increase, colID=columnID)
            else:
                print('bud_perc colID greater than 1')
                DATA_parent = OTB.change_percent(grouped_df = DATA.filter(list(child)), other_grouped_df = DATA.filter(list(other_filter_condition)), increase = increase, colID = columnID)
                DATA_siblings = DATA.filter(list(parent.not_()))
                DATA = pl.concat([DATA_siblings,DATA_parent])
        
        
        elif columnID not in ['Logistic%','DisplayItemQty','adjusted_budget_gross_margin_percent','COR_EOLStock_value','markdown_percent','proposed_sellthru_percent', 'Check_box']:
            print(columnID, 'Not in distingushed ones')
            if len(columns_to_filter) == 1:
                print('len of col to filter is:', 1)
                # DATA = change_value(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                DATA = OTB.change_percent(grouped_df=DATA.filter(list(child)), other_grouped_df=DATA.filter(list(other_filter_condition)), increase=increase, colID=columnID)

            else:
                print('len of col to filter is:', len(columns_to_filter))
                # DATA.loc[parent] = change_value(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                DATA = OTB.change_value(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)

        if columnID == 'budget_percent':
            
            summation = DATA['budget_amount'].sum()

            DATA = DATA.with_columns(budget_amount = DATA[columnID]*summation/100)
            
            DATA = DATA.with_columns(((pl.col('budget_amount').cast(pl.Float64)+pl.col('sales_actual').cast(pl.Float64))).alias('ACT_FCT'))
            
            DATA = DATA.with_columns(((pl.col("budget_amount")/pl.col("initial_average_retail_price")).round()).alias('budget_qty'))

            DATA = DATA.with_columns(((pl.col('budget_gross_margin')/pl.col('budget_amount'))*100).alias('budget_margin_percent'))
            
            DATA = DATA.with_columns(((pl.col('budget_amount'))+((pl.col('initial_average_retail_price'))*(pl.col('stock_on_hand_qty')))).alias('PO'))

            DATA = DATA.with_columns(SalesActualsByForecast = (pl.col('sales_actual').cast(pl.Float64)/pl.col('budget_amount')) *100)
#------
            DATA = DATA.with_columns(((DATA['budget_amount']/DATA['budget_amount'].sum())*100).alias("relative_budget_percent"))
#------
            DATA = DATA.with_columns(((pl.col('budget_amount') /pl.col('total_sku_count')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('budget_per_sku_qty_total'))

            #initial_average_retail_price - Price
            DATA = DATA.with_columns(((pl.col('budget_amount')-(pl.col('initial_average_retail_price')*pl.col('budget_qty')))).alias('Deficit'))

            DATA = DATA.with_columns(((pl.col('budget_amount')/pl.col("ACT_FCT")).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias("act_forecast_vs_budget_percent"))

            DATA = DATA.with_columns((((pl.col('budget_amount') - pl.col('BudgetCostofGoods'))/pl.col('budget_amount'))*100).alias('adjusted_budget_gross_margin_percent'))

            DATA = DATA.with_columns(((pl.col('budget_amount')-(pl.col('budget_amount')*pl.col('adjusted_budget_gross_margin_percent'))/100)).alias('BudgetCostofGoods'))

            DATA = DATA.with_columns((100*(pl.col('budget_amount')-pl.col('SupplyCost'))/pl.col('budget_amount')).alias('FirstMargin%'))
            
            DATA = DATA.with_columns(((pl.col('GrossSales')-pl.col('budget_amount'))/pl.col('GrossSales')).alias('markdown_percent'))
            
            DATA = DATA.with_columns(((pl.col('budget_amount')/(100-pl.col('markdown_percent'))).alias('GrossSales')))
            
            DATA = DATA.with_columns(((pl.col('GrossSales')*pl.col('proposed_sellthru_percent'))).alias('PurchasedRetailValueGrossSale'))
            
            DATA = DATA.with_columns((pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast')).alias('PurchaseRetailValueatGrossSale'))
            
            DATA = DATA.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*(100 - pl.col('FirstMargin%')))).alias('OTBorPurchaseCost'))

    #    elif columnID == 'Logistic%':
            # DATA['SupplyCost'] = DATA['BudgetCostofGoods'] - (DATA['BudgetCostofGoods']*(DATA['Logistic%']/100))
            # DATA['FirstMargin%'] = 100*(DATA['budget_amount']-DATA['SupplyCost'])/DATA['budget_amount']
            # DATA['OTBorPurchaseCost'] = DATA['PurchaseRetailValueatGrossSale'] * ((100 - DATA['FirstMargin%']))/100
            
            # DATA = DATA.with_columns(((pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100)))).alias('SupplyCost'))
            # DATA = DATA.with_columns((((pl.col('budget_amount')-pl.col('SupplyCost'))*100)/pl.col('budget_amount')).alias('FirstMargin%'))
            # DATA = DATA.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))).alias('OTBorPurchaseCost'))
        
        

        elif columnID == 'markdown_percent':
            
            DATA =DATA.with_columns(GrossSales = (pl.col('budget_amount')/(100-pl.col('markdown_percent'))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)*100)
            DATA =DATA.with_columns(MarkdownValue = (pl.col('GrossSales')-pl.col('budget_amount')))
            DATA = DATA.with_columns(PurchasedRetailValueGrossSale = ((pl.col('budget_amount')/(100-pl.col('markdown_percent')))/(pl.col('proposed_sellthru_percent')/100)).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
            DATA =DATA.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
            DATA =DATA.with_columns(OTBorPurchaseCost = (pl.col('PurchasedRetailValueGrossSale')*(100-pl.col('FirstMargin%'))))
        
        # elif columnID == 'proposed_sellthru_percent':

        #     # DATA['PurchasedRetailValueGrossSale'] = DATA['GrossSales'] * DATA['proposed_sellthru_percent']
        #     # DATA['GrossSales'] = DATA['PurchasedRetailValueGrossSale']/DATA['proposed_sellthru_percent']
        #     # DATA['markdown_percent'] = (DATA['GrossSales'] - DATA['budget_amount'])/DATA['GrossSales']
        #     # DATA['PurchasedRetailValueGrossSale'] = DATA['budget_amount'] / (DATA['proposed_sellthru_percent'].replace(0,pd.NA)/100)
        #     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        #     # DATA['PurchasedRetailValueGrossSale'] = DATA['budget_amount'] / (1 - ((100 - data['proposed_sellthru_percent']) / 100))
        #     DATA =DATA.with_columns(PurchasedRetailValueGrossSale = pl.col('budget_amount')/(1-((100-pl.col('proposed_sellthru_percent'))/100)))
        #     # DATA['PurchaseRetailValueatGrossSale']= DATA['PurchasedRetailValueGrossSale']   - DATA['TYForecast']
        #     DATA =DATA.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
        #     # DATA['OTBorPurchaseCost'] = DATA['PurchaseRetailValueatGrossSale'] * ((100 - DATA['FirstMargin%']))/100
        #     DATA =DATA.with_columns(OTBorPurchaseCost = pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))
            
        elif columnID == 'DisplayItemQty':
            DATA = DATA.with_columns(initial_average_retail_price = pl.col('initial_average_retail_price').replace({np.inf:0,-np.inf:0}).fill_nan(0))
            DATA = DATA.with_columns(DisplayItemValue = pl.col('initial_average_retail_price')*newValue)
            print(DATA.select(pl.col(["DisplayItemValue", 'DisplayItemQty', 'initial_average_retail_price']).sum()), "disp values")
            DATA = DATA.with_columns(TYForecast = pl.col('StockatRetailPrice')- pl.col('DisplayItemValue')-pl.col('COR_EOLStock_value'))
            # print(DATA.select(pl.col(['StockatRetailPrice', 'DisplayItemValue', 'COR_EOLStock_value']).sum()), 'st__disp')
            DATA = DATA.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
            DATA = DATA.with_columns(OTBorPurchaseCost=pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))
            
        elif columnID == 'COR_EOLStock_value':
            try:
                print('we are here')
                # DATA = DATA.with_columns(TYForecast = pl.col('StockatRetailPrice')-pl.col('DisplayItemValue')-pl.col('COR_EOLStock_value').cast(pl.Float64))
                # DATA = DATA.with_columns(PurchasedRetailValueGrossSale = (pl.col('gross_sales') * (pl.col('proposed_sellthru_percent'))/100).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
                # DATA = DATA.with_columns((((pl.col('PurchasedRetailValueGrossSale'))-pl.col('TYForecast')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('PurchaseRetailValueatGrossSale'))
                # DATA = DATA.with_columns(OTBorPurchaseCost = pl.col('PurchaseRetailValueatGrossSale')-((100-pl.col('FirstMargin%'))))
                # DATA = DATA.with_columns((((pl.col('PurchaseRetailValueatGrossSale'))/(pl.col('initial_average_retail_price'))).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('OTBquantity'))
            except:
                print('crrrrel', traceback.format_exc())
        return DATA, data
    
    # async def apply_group_by(self, DATA : pl.DataFrame, data : pl.DataFrame, data_filter : dict, sub_filter_state : bool, group : List, filters : Filters, filter_condition : [Optional] = None)->Union[None,pl.Series,pl.DataFrame]:
    async def apply_group_by(self, data_filter : dict, sub_filter_state : bool, group : List)->Union[pl.DataFrame]:

        """
            Apply group-by operation on the given DataFrame based on specified parameters.

            Args:
                DATA (pl.DataFrame): The main DataFrame.
                data_filter (Dict): input filter response.
                sub_filter_state (bool): Sub-filter state/ subfilter selected or not.
                group (List): List of columns used for grouping/subfilter columns.
                filters (Filters): Filters object.
                filter_condition: filter condition for main data /series of bool.

            Returns:
                Union[None, pl.Series, pl.DataFrame]: The result of the group-by operation filter_condtion and dataframe.

        """
        # print(DATA.columns, 'data cols before agg')
        # print('we are finding corr gp0')

        # filter_condition,sub_filter_state,group = OTB.secondary_filter(DATA,filters,sub_filter_state,group,filter_condition)

        # await get_apply_secondary_filters('item_ counter', data_filter["secondary_filter"], sub_filter_state, group, filter_condition)

        # if filter_condition is not None:
        #     # Add filter_index column based on filter_condition
        #     DATA = DATA.with_columns(filter_index=filter_condition)
        #     DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))

    
        secondary = data_filter['secondary_filter']
        print(secondary, 'passing_to_procedure')
        scores_m = secondary['article_score']
        print(scores_m, 'passing_to_procedure0')
        sub_filter_state = any(values != [] for key, values in data_filter["secondary_filter"].items() if key != 'article_score')
        print(sub_filter_state, "sun_filter_state_lambda")
        print(group, 'grpmain')
        
        if not len(data_filter['group_by']['columns']) == 0: # The main filter columns selected
            print("we gpby cols =0")
            # group = filters.getmainFilterDetails(data_filter, OTB.filter_details)
            print('>_0_len_g')
            for i in  data_filter['group_by']['columns']:
                if i in OTB.filter_details:
                    group.append(OTB.filter_details[i])
            
            limit = max_col.index(group[-1])
            
            # mean_cols = [f"pl.col('{col}').mean()" for col in avg_col if col!= 'total_sku_count']
            # sum_cols =  [f"pl.col('{col}').sum()" for col in sum_col ] # if col!= 'total_sku_count'
            # rank_cols = [f"pl.col('{col}').max()" for col in rank_col]
            # dist_cols = [f"pl.col('{col}').sum()" for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns]
            # art_cols = [f"pl.col('{col}').mean()" for a, col in arts.items() if a in scores_m]
            # cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
            # cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
            
            # print(art_cols, 'art_colsssss')
            # agg_dict = [eval(expr) for expr in mean_cols+sum_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2] #+uniques
           
            mean_agg_cols = avg_col
            sum_agg_cols = sum_col
            distribution_cols = ['new_budget_mix', 'revised_budget_amount']
            kpi_rank_cols = ['Check_box']
            article_score_cols = [col for a, col in arts.items() if a in scores_m if a in scores_m]
            # cumulative_scores = ['coefficient_score', 'coefficient_score_mix_percent']
            coefficient_score, coefficient_score_mix_percent = ('coefficient_score', 'coefficient_score_mix_percent') if len(article_score_cols) != 0 else ('', '')
            
            # await mainClusterCaller(group, sub_filter_state, mean_agg_cols, 
            #                                 sum_agg_cols, distribution_cols, kpi_rank_cols, article_score_cols, 
            #                                 coefficient_score, coefficient_score_mix_percent)
            # data = await getMaindata()

            # print(data, "the data grp")
            sub_filter_state = any(values != [] for key, values in data_filter["secondary_filter"].items() if key != 'article_score')
            print(sub_filter_state, "sun_filter_state_lambda")

#             if filter_condition is not None:
            
            if sub_filter_state:
                print("WITH MFS WITH SFS")
                try:
                    # filter_condition = DATA["filter_index"]==1
                    table_name = 'otb_min_filter'
                    await mainClusterCaller(table_name, group, sub_filter_state, mean_agg_cols, 
                                            sum_agg_cols, distribution_cols, kpi_rank_cols, article_score_cols, 
                                            coefficient_score, coefficient_score_mix_percent)
                    data = await getMaindata()
                    # print(group, "the group")
                    # print(agg_dict, "the aggregation dict")

                    # data = DATA.group_by(group).agg(agg_dict) #, maintain_order = True
                    # print(data.columns, 'Data columns_on the no fil condition')
#--------------------------------------------------------------------------call_filter------------------------------------------------------------------
                    # OTB.SUB_FILTER = OTB.call_filter(DATA.filter(list(filter_condition)), OTB.SUB_FILTER, group, DATA)
                except Exception as e:
                    print(f'error of grpby stts none fl_condition is {e}')
                    pass
            else:
                print("WITH MFS no SFS")
                try:
                    # print("no_filter_g")
                    # print(group, 'grp_g')
                    table_name = 'item_counter'
                    await mainClusterCaller(table_name, group, sub_filter_state, mean_agg_cols, 
                                            sum_agg_cols, distribution_cols, kpi_rank_cols, article_score_cols, 
                                            coefficient_score, coefficient_score_mix_percent)
                    data = await getMaindata()
                    
                    #----------------------------------Primary filter------------------------------------------
                    # data = OTB.main_filters(DATA, data_filter, sub_filter_state,group,filter_condition)
                    #------------------------------------------------------------------------------------------
                    
                    # data = DATA.group_by(list(set(group))).agg(agg_dict) #, maintain_order = True
                except Exception as e:
                    # print(traceback.format_exec())
                    print(group)
                    print(f"Error in grpby sttsxxx with cols is {e}")           
            # ________________________________________Primarly______________________________________________________
        else:
            #---
            print('0_g_len')
            
        
            # mean_cols = [f"pl.col('{col}').mean()" for col in avg_col]
            # sum_cols = [f"pl.col('{col}').sum()" for col in sum_col ] # if col != 'total_sku_count'
            # rank_cols = [f"pl.col('{col}').max()" for col in rank_col]
            # dist_cols = [f"pl.col('{col}').sum()" for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns]
            # art_cols = [f"pl.col('{col}').mean()" for a, col in arts.items() if a in scores_m]
            # cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
            # cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0 ] 
            
            # agg_dict = [eval(expr) for expr in mean_cols+sum_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2]  #+uniques            print('total cols is: ', agg_dict )
            mean_agg_cols = avg_col
            sum_agg_cols = sum_col
            distribution_cols = ['new_budget_mix', 'revised_budget_amount']
            kpi_rank_cols = ['Check_box']
            article_score_cols = [col for a, col in arts.items() if a in scores_m if a in scores_m]
            # cumulative_scores = ['coefficient_score', 'coefficient_score_mix_percent']
            coefficient_score, coefficient_score_mix_percent = ('coefficient_score', 'coefficient_score_mix_percent') if len(article_score_cols) != 0 else ('', '')

            if not sub_filter_state:
                print('No main filter No sub_filter_state')
                try:
                    # Apply group-by without filter conditions
                    # print(DATA.columns, 'no sfs cols_problems')
                    # print(group, "the group")
                    # print(agg_dict, "the aggregation dict")
                    table_name = 'item_counter'
                    print(f"No MFS No SFS group is : {group}")
                    await mainClusterCaller (table_name, group, sub_filter_state, mean_agg_cols, 
                                            sum_agg_cols, distribution_cols, kpi_rank_cols, article_score_cols, 
                                            coefficient_score, coefficient_score_mix_percent)
                    data = await getMaindata()
                    print(data, "the data grp 1")

                    # data = DATA.select(agg_dict)
                    # print(data.columns, 'columns of no sfs')
                except Exception as e:
                    print(f"Error in grpby status without cols, S_F_S is {e}")
                    print(traceback.format_exc())
            else:
                try:
                    print("No MFS WITH SFS ")
                    print(f"No MFS WITH SFS group is {group}")
                    table_name = 'otb_min_filter'

                    await mainClusterCaller(table_name, group, sub_filter_state, mean_agg_cols, 
                                            sum_agg_cols, distribution_cols, kpi_rank_cols, article_score_cols, 
                                            coefficient_score, coefficient_score_mix_percent)
                    data = await getMaindata()
                    
                    print(data, "the data grp 2")

                    # data = DATA.group_by(list(set(group)), maintain_order = True).agg(agg_dict) #, maintain_order = True
                    
                   
                    # OTB.SUB_FILTER = OTB.call_filter(DATA.filter(list(filter_condition)), OTB.SUB_FILTER, group, DATA)
                    df = await getSubfilterData()
                    OTB.SUB_FILTER  = get_subFilter_from_table(df, OTB.SUB_FILTER, group)
                except Exception as e:
                    print(f"Error in grpby sttsxxx withot cols, with S_F_S is {e}")
                    print(traceback.format_exc())
                    pass
            print(data.columns, 'cccccccoooolllls')
        # return data, filter_condition
        return data
    

    # def expand_hierarchy(self, DATA : pl.DataFrame, data_filter : Dict, sub_filter_state : bool, group : List, filters : Filters, filter_condition : [Optional] = None) -> Union[None, pl.Series, pl.DataFrame]: 
    async def expand_hierarchy(self, data_filter : Dict, sub_filter_state : bool, group : List, filters : [Optional] = None, filter_condition : [Optional] = None) ->  pl.DataFrame: 
        
        """
        Expand/contract the hierarchy of the given DataFrame based on specified parameters.

        Args:
            DATA (pl.DataFrame): The main DataFrame.
            data_filter (Dict): Filter response from front end.
            sub_filter_state (bool): Sub-filter state/ flag indicating subfilter selected or not.
            group (List): List of columns used for grouping/subfilter columns.
            filters (Filters): Filters object.
            filter_condition: [Optional] = None): filter condition for main data /series of bool.
  
        Returns:
            Union[None, pl.Series, pl.DataFrame]: The main data,
            The result of expandad/contracted dataframe as per the selection and filter_condtion .

        """
        
        print('we are finding corr ex1')
        secondary = data_filter['secondary_filter']
        scores_m = secondary['article_score']
        # mean_cols = [f"pl.col('{col}').mean()" for col in avg_col]
        # sums_cols = [f"pl.col('{col}').sum()" for col in sum_col]
        # rank_cols = [f"pl.col('{col}').max()" for col in rank_col]
        # dist_cols = [f"pl.col('{col}').sum()" for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns]
        # art_cols = [f"pl.col('{col}').mean()" for a, col in arts.items() if a in scores_m]
        # cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
        # cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
        mean_agg_cols = avg_col
        sum_agg_cols = sum_col
        distribution_cols = ['new_budget_mix', 'revised_budget_amount']
        kpi_rank_cols = ['Check_box']
        article_score_cols = [col for a, col in arts.items() if a in scores_m if a in scores_m]
        coefficient_score, coefficient_score_mix_percent = ('coefficient_score', 'coefficient_score_mix_percent') if len(article_score_cols) != 0 else ('', '')
        maxs_cols = []
        columns_to_filter =[]
        values_to_filter = []
        selected_row = data_filter['expand']["row"]
        # Apply secondary filters and update filter_condition, sub_filter_state, and group
        # filter_condition,sub_filter_state,group = OTB.secondary_filter(DATA,filters,sub_filter_state,group,filter_condition)


        #Apply hierachical filters -> selected row not in heirarchy
        for i in selected_row:
            if i in HEIRARCHY:
                columns_to_filter.append(i)
                values_to_filter.append(selected_row[i])
                last_filter = HEIRARCHY.index(columns_to_filter[0])
        
        group_to_filter = []
        group_value_to_filter = []
        print(group, 'grpexp')
        for i in selected_row:
            if i in group:
                group_to_filter.append(i)
                group_value_to_filter.append(selected_row[i])
                group.remove(i)
        
        print(group, 'grpexp2')
        # for col, val in zip(columns_to_filter+group_to_filter, values_to_filter+group_value_to_filter):
        #     if filter_condition is None:
        #         try:
        #             filter_condition = (DATA[col] == val)
        #         except Exception as e:
        #             print(f"wrong heirarchy: {e}")
        #     else:
        #         filter_condition = filter_condition & (DATA[col] == val)
        print(group_to_filter, group_value_to_filter, columns_to_filter, values_to_filter, 'groupssssss to fil')
        #----------------------------------------------------
        filter_columns = columns_to_filter+group_to_filter
        filter_values =  values_to_filter+group_value_to_filter # This
        #----------------------------------------------------
        # await filter_data_for_expanding(columns_to_filter+group_to_filter, values_to_filter+group_value_to_filter)
        # if filter_condition is not None:

        #     # Add filter_index column based on filter_condition
        #     DATA = DATA.with_columns(filter_index=filter_condition)
        #     DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Float64))
        #     print(DATA.columns,"FILTERDATA")
        if columns_to_filter == []:
            print('first drilldown')

            #form subset for calculation 
            last_filter = 0
            group.append(HEIRARCHY[last_filter])
            group = list(set(group))
            print(type(group), 'group_type_error as set')
            # print(group,"FILTERDATA1")
            # print(filter_condition, 'filter_cond')

            # item_group = group + ["ITEMID"]
            # item_group = list(set(item_group).intersection(Budget.group_subset))
            if sub_filter_state:
                print("SFS")
                try:
                    print(group, 'SFS grp')
                    # do initial calculation and aggregation
                    
                    # agg_dict = [eval(expr) for expr in list(mean_cols)+sums_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2] # +uniq_cols
                    # if filter_condition is not None:

                    #     # Add filter_index column based on filter_condition
                    #     DATA = DATA.with_columns(filter_index=filter_condition)
                    #     DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))
                    
                    # filter_condition = DATA["filter_index"]==1
                    
                    # data = DATA.filter(list(filter_condition)).group_by(list(set(group)), maintain_order = True).agg(agg_dict) # , maintain_order = True
                    table_name = 'otb_min_filter'
                    await get_sp_otb_expand_heirarchy(table_name, group, mean_agg_cols, sum_agg_cols, distribution_cols, kpi_rank_cols, article_score_cols, maxs_cols, coefficient_score, coefficient_score_mix_percent, filter_columns, filter_values)
                    data = await get_heirarchial_data()
                    #calculate subfilters options
                    # Budget.SUB_FILTER = Budget.call_filter(datas.filter(list(filter_condition)),Budget.SUB_FILTER,group,DATA,Budget.filter_store)
                except:
                    print(traceback.format_exc())  
            else:
                print('NO SFS')
                # print('we are finding corr ex3')

                #do initial calculation and aggregation

                # agg_dict = [eval(expr) for expr in list(mean_cols)+sums_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2] #+uniq_cols
                
                # if filter_condition is not None:

                #     # Add filter_index column based on filter_condition
                #     DATA = DATA.with_columns(filter_index=filter_condition)
                #     DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Float64))

                    # filter_condition = DATA["filter_index"]==1
                print(group,"No sfs group")
                print(type(group), 'group_type_error as set2')
                table_name = 'item_counter' 
                # data = DATA.group_by(list(set(group)),  maintain_order = True).agg(agg_dict) # 
                await get_sp_otb_expand_heirarchy(table_name, group, mean_agg_cols, sum_agg_cols, distribution_cols, kpi_rank_cols, article_score_cols, maxs_cols, coefficient_score, coefficient_score_mix_percent, filter_columns, filter_values)
                data = await get_heirarchial_data()

               
                # Budget.SUB_FILTER = Budget.call_filter(datas,Budget.SUB_FILTER,group,DATA,Budget.filter_store)    
        else:
            print('second drills onwards')

            try:
                groups = max_col[limit:max_col.index(columns_to_filter[0])+1]
                print('try grp successfull', groups)
            except:
                groups = max_col[:max_col.index(columns_to_filter[0])+1]
                print("print('except' groups)", groups)

            print(type(group), 'group_type_error as set')
            group.append(HEIRARCHY[last_filter+1])

            #form subset for calculation
            # item_group = group+["ITEMID"]
            # item_group = list(set(item_group + HEIRARCHY[:last_filter+1]))
            # item_group = list(set(item_group).intersection(Budget.group_subset))
#-----------------------------------------------------------------------
            # if filter_condition is not None:

            #     # Add filter_index column based on filter_condition
            #     DATA = DATA.with_columns(filter_index=filter_condition)
            #     DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Float64))
#------------------------------------------------------------------------

            # maxs_cols = [f"pl.col('{col}').max()" for col in groups if col not in group]
            maxs_cols = [col for col in groups if col not in group]
            print('rest drill group', group)
            print('rest drill down max cols', maxs_cols)
            # agg_dict = [eval(expr) for expr in mean_cols+sums_cols+maxs_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2] # uniq_cols
                        
            # filter_condition = DATA["filter_index"]==1
            try:
                # data = DATA.filter(list(filter_condition)).group_by(list(set(group)), maintain_order =True).agg(agg_dict) #, , maintain_order = True
                if sub_filter_state:
                    table_name = 'otb_min_filter' 
                else:
                    table_name = 'item_counter'
                await get_sp_otb_expand_heirarchy(table_name, group, mean_agg_cols, sum_agg_cols, distribution_cols, kpi_rank_cols, article_score_cols, maxs_cols, coefficient_score, coefficient_score_mix_percent, filter_columns, filter_values)
                data = await get_heirarchial_data()

            except:
                print(traceback.format_exc())
            #calculate subfilters options
            # Budget.SUB_FILTER = Budget.call_filter(datas.filter(list(filter_condition)),Budget.SUB_FILTER,group,DATA,Budget.filter_store)
        # print(DATA.columns, 'in expand___')
        return data #, filter_condition, group
    
   
    def sort_and_clean(self, data_filter:Dict, data:pl.DataFrame, filters:Filters):
        """
        Prepare data for display in UI,do sorting/pagination/cleaning.

        Args:
            data_filter (Dict): input response dict.
            data (pl.DataFrame): DataFrame to be sorted and cleaned and paginated.

        Returns:
            pl.DataFrame: Sorted ,cleaned and paginated DataFrame.

        """
        # print('we are finding corr ex5')

        try:
            sort_columnid = data_filter['sort']['id']
            # print(sort_columnid,"SORTID")
            # Check if sorting order is ascending or descending and apply sorting
            if data_filter['sort']['desc'] == False:
                datas = data.sort(by =[sort_columnid], descending= False, maintain_order = True)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size] #, maintain_order = True
            elif data_filter['sort']['desc'] == True:
                datas =data.sort(by =[sort_columnid], descending= True, maintain_order = True)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size]    #, maintain_order = True
        except:
            # If there is an exception, fallback to default pagination
            datas = data[(filters.page_number*filters.page_size):((filters.page_number+1)*filters.page_size)]
            
        datas = datas.to_pandas()
        # datas[float_cols] = datas[float_cols].replace([np.nan,np.inf,-np.inf],0).astype(float).round(2)
        # datas[int_cols]   = datas[int_cols].replace([np.nan,np.inf,-np.inf],0).astype(int)

        return datas
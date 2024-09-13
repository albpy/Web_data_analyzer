import traceback
from datetime import datetime
import polars as pl

{'family': [], 
 'sub_family': [], 
 'supplier': [], 
 'category': [], 
 'dom_comm': [], 
 'sub_category': [], 
 'extended_sub_category': [], 
 'sub_category_supplier': [], 
 'HistoricalYear': [], 
 'history_Day': [], 
 'history_Quarter': [], 
 'history_dates': [], 
 'history_month': [], 
 'history_week': [], 
 'BudgetYear': [], 
 'season': [], 
 'country': [], 
 'region': [], 
 'area': [], 
 'city': [], 
 'Store_Name': [], 
 'month': [], 
 'week': [], 
 'BudgetDate': [], 
 'Quarter': [], 
 'Day': [], 
 'Channel': [], 
 'article_score': []}

def call_filter(data: pl.DataFrame, SUB_FILTER: dict, group: list):
    """
    Updates the filter options based on the selected group and stores filter values in filter_store.

    Args:
        data (pl.DataFrame): Filtered dataframe.
        SUB_FILTER (dict): Dictionary to store filter options.
        group (List): List of selected group columns/sub filter columns.

    Returns:
        Dict: Updated SUB_FILTER dictionary.
    """    

    try:
        keys = [
            "store",
            "region",
            "Channel",
            "budget_year",          #                                                                   --> pl.Utf8
            "Quarter",              # budget_quarter - The quarter of the forecasting Year              --> pl.Utf8
            "month",                # budget_month   - The month of the forecasting Year                --> pl.Utf8
            "week",                 # budget_week    - The week of the forecasting Year(Integer week)   --> pl.Utf8
            "Day",                  # budget_day     - The corresponding week                           --> pl.Int8 format
            "date",                 # Budget_date    - The actual dates in the forecast year            --> pl.Date format(%Y-%m-%d) 
            "history_year",
            "history_Quarter",
            "history_month",
            "history_week",
            "history_Day",
            "history_dates",        # history_dates - Date at which sales done.
            "family",
            "sub_family",
            "supplier",
            "category",
            "dom_comm",
            "sub_category",
            "extended_sub_category",
            "sub_category_supplier",
        ]

        values = [
            "Store",
            "Region",
            "Channel",
            "Budget_Year",
            "budget_quarter",
            "budget_month",
            "budget_week",
            "budget_day",
            "Budget_date",
            "historical_year",
            "history_quarter",
            "history_month",
            "history_week",
            "history_day",
            "INVOICEDATE",
            "Family",
            "SubFamily",
            "Supplier",
            "Category",
            "DOM_COMM",
            "SubCategory",
            "ExtendedSubCategory",
            "SubCategorySupplier",
        ]

        for i, v in zip(keys, values):
            if v not in group:
                if v in ["Budget_Year", "historical_year"]:
                    SUB_FILTER[i] = list(data[v].cast(pl.Float32).drop_nulls().unique())
                elif v == "date":
                    SUB_FILTER["date"] = list(data[v].cast(str).unique())
                elif v == "history_dates":
                    SUB_FILTER["i"] = list(data[v].cast(str).unique())
                else:
                    if v in ["history_week", "INVOICEDATE"]:
                        if v == "INVOICEDATE":
                            SUB_FILTER[i] = sorted(
                                list(data[v].drop_nulls().unique()),
                                key=lambda x: (
                                    datetime.strptime(x, "%Y-%m-%d")
                                    if "-" in x
                                    else datetime.strptime("1900-01-01", "%Y-%m-%d")
                                ),
                            )
                        else:
                            SUB_FILTER[i] = sorted(
                                list(data[v].fill_null(0).unique()),
                                key=lambda x: int(x) if x.isdigit() else 0,
                            )
                    else:
                        # store, region, channel, 
                        # budgetQuarter, budgetMonth, budgetWeek, budgetDay, budgetDate, 
                        # historyQuarter, historyMonth, historyDay, 
                        # family, subFamily, supplier, category, domComm, subCategory, extendedSubCategory
                        print('last_v', i, v)
                        


                        SUB_FILTER[i] = list(data[v].unique())

        SUB_FILTER["article_score"] = [
            "sale",
            "abc",
            "ae",
            "speed",
            "terminal",
            "margin",
            "sell",
            "markdown",
            "core",
            "quartile",
            "sortimeter",
        ]

    except Exception as e:
        print(traceback.format_exc())
        print(f"Error: {e}")

    return SUB_FILTER


def get_subFilter_from_table(df:pl.DataFrame, SUB_FILTER:dict, group:list):
    try:
        keys = [
            "store",
            "region",
            "Channel",
            "budget_year",          #                                                                   --> pl.Utf8
            "Quarter",              # budget_quarter - The quarter of the forecasting Year              --> pl.Utf8
            "month",                # budget_month   - The month of the forecasting Year                --> pl.Utf8
            "week",                 # budget_week    - The week of the forecasting Year(Integer week)   --> pl.Utf8
            "Day",                  # budget_day     - The corresponding week                           --> pl.Int8 format
            "date",                 # Budget_date    - The actual dates in the forecast year            --> pl.Date format(%Y-%m-%d) 
            "history_year",
            "history_Quarter",
            "history_month",
            "history_week",
            "history_Day",
            "history_dates",        # history_dates - Date at which sales done.
            "family",
            "sub_family",
            "supplier",
            "category",
            "dom_comm",
            "sub_category",
            "extended_sub_category",
            "sub_category_supplier",
        ]

        values = ["store",       #String
                    "region", #String
                    "Channel", #String
                    "budget_year", #Null
                    "budget_quarter", #String
                    "budget_month", #String
                    "budget_weekday", #Int64
                    "budget_day", #String
                    "budget_dates", #Date
                    "historical_year", #Int64
                    "history_Quarter", #String
                    "history_month", #String
                    "history_week", #String
                    "history_Day", #String
                    "history_datess", #Date
                    "family", #String
                    "sub_Family", #String
                    "Supplier", #String
                    "Category", #String
                    "Dom_Comm", #String
                    "Sub_Category", #String
                    "Extended_sub_category", #String
                    "Sub_Category_supplier", #String
                    "Article_Score"]
        
        for sub_filter, column_name in zip(keys, values):
            if df.shape == (0, 0):
                continue
            if column_name is "Article_Score":
                continue
            if column_name not in  group:
                if column_name in ["budget_year", "historical_year"]:
                    SUB_FILTER[sub_filter] = list(df[column_name].cast(pl.Float32).drop_nulls().unique())
                elif column_name == "budget_dates":
                    SUB_FILTER["date"] = list(df[column_name].cast(str).unique())
                elif column_name == "history_dates":
                    SUB_FILTER["i"] = list(df[column_name].cast(str).unique())
                else:
                    if column_name in ["history_week", "history_datess"]:
                        if column_name == "history_datess":
                            SUB_FILTER[sub_filter] = sorted(
                                list(df[column_name].drop_nulls().unique()),
                                # key=lambda x: (
                                #     datetime.strptime(x, "%Y-%m-%d")
                                #     if "-" in x
                                #     else datetime.strptime("1900-01-01", "%Y-%m-%d")
                                # ),
                            )
                        else:
                            SUB_FILTER[sub_filter] = sorted(
                                list(df[column_name].fill_null(0).unique())#,
                                # key=lambda x: int(x) if x.isdigit() else 0,
                            )
                    else:
                        # store, region, channel, 
                        # budgetQuarter, budgetMonth, budgetWeek, budgetDay, budgetDate, 
                        # historyQuarter, historyMonth, historyDay, 
                        # family, subFamily, supplier, category, domComm, subCategory, extendedSubCategory
                        print('last_v', sub_filter, column_name)
                        


                        SUB_FILTER[sub_filter] = list(df[column_name].unique())

        SUB_FILTER["article_score"] = [
            "sale",
            "abc",
            "ae",
            "speed",
            "terminal",
            "margin",
            "sell",
            "markdown",
            "core",
            "quartile",
            "sortimeter",
        ]


    except:
        print(traceback.format_exc())
    # print(SUB_FILTER, "the_received_subfilter" )
    return SUB_FILTER

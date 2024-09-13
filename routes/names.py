class compositeCols:

    MAX_COLS: list = [
        "Date", "Symbol", "Series" 
    ]

    AVG_COLS: list = [
    'Prev Close', 'Open', 'High', 'Low', 'Last',
    'Close', 'VWAP', 'Volume', 'Turnover', 'Trades', 'Deliverable Volume',
    '%Deliverble'
    ]

    SUM_COLS: list = [
        
    ]


class conversionCols:

    FLOAT_COLS: list = [

    ]
    # 'History_Net_Sales' ,
    INT_COLS: list = ["sales_actual"]
    # ,'unit_buy_by_sku', 'SKU_COUNT', 'unit_buy_by_sku_total', 'BudgetCostofGoods', 'COSTPRICE',
    # 'Displ#ayItem', 'COR_EOLStock_value', 'COSTPRICE']


class performanceCols:

    SCORES: dict = {
        "sale": "article_score_sale",
        "abc": "article_score_abc",
        "ae": "article_score_ae",
        "speed": "article_score_speed",
        "terminal": "article_score_terminal",
        "margin": "article_score_margin",
        "sell": "article_score_sell",
        "markdown": "article_score_markdown",
        "core": "article_score_core",
        "quartile": "article_score_quartile",
        "sortimeter": "article_score_sortimeter",
    }

    ARTICLE_SCORES: list = [
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

    RANK_COLS: list = ["Check_box"]


class functionalFields:

    HEIRARCHY: list = [
        "Channel",
        "family",
        "sub_family",
        "supplier",
        "category_name",
        "dom_comm",
        "sub_category",
        "extended_sub_category",
        "sub_category_supplier",
        "assembly_code_nickname",
        "status",
        "ENDOFLife",
        "description",
        "ITEMID",
    ]
    # SUB_FILTER: dict  = {'region':[], 'season':[], 'area':[], 'budget_year':[],'historical_year':[],'month':[],'week':[],'date':[], 'article_scores' : []}

    SUB_FILTER = {
        "store": [],
        "region": [],
        "season": [],
        "area": [],
        "Channel": [],
        "budget_year": [],
        "Quarter": [],
        "month": [],
        "week": [],
        "date": [],
        "Day": [],
        "historical_year": [],
        "history_Quarter": [],
        "history_month": [],
        "history_week": [],
        "history_Day": [],
        "history_dates": [],
        "article_score": [],
    }
    # 'Channel', 'Region', 'INVENTLOCATIONID', 'Store', 'Department',  'Family', 'SubFamily',
    #                                        'supplier','category_name', 'dom_comm',  'sub_category', 'extended_sub_category', 'sub_category_supplier', 'assembly_code_nickname','status', 'ENDOFLife',
    #    'description','ITEMID','historical_year', 'Budget_Year','Quarter','Month','Week','Budget_date','Day',
    PERCENT_COLS: list = [
        "new_budget_mix",
        "budget_percent",
        "relative_budget_percent",
        "act_forecast_vs_budget_percent",
        "budget_gross_margin_percent",
        "adjusted_budget_gross_margin_percent",
        "Logistic%",
        "markdown_percent",
        "adjusted_markdown_percent",
        "FirstMargin_percent",
        "proposed_sellthru_percent",
        "adjusted_sellthru_percent",
        "LYvsACT_FCT_percent",
        "coefficient_score_mix_percent",
        "budget_vpy",
    ]

    EDITABLE_COLS: list = [
        "budget_percent",
        "budget_qty",
        "adjusted_budget_gross_margin_percent",
        "adjusted_markdown_percent",
        "Logistic%",
        "adjusted_sellthru_percent",
        "DisplayItemQty",
        "COR_EOLStock_value",
    ]

    # , 'order_index'


class TABS:

    columns = [
    'Date', 'Symbol', 'Series', 'Prev Close', 'Open', 'High', 'Low', 'Last',
    'Close', 'VWAP', 'Volume', 'Turnover', 'Trades', 'Deliverable Volume',
    '%Deliverble'
    ]

    BudgetCost = [
        "Channel",
        "Region",
        "INVENTLOCATIONID",
        "Store",
        "Department",
        "category_name",
        "family",
        "sub_family",
        "supplier",
        "dom_comm",
        "sub_category",
        "extended_sub_category",
        "sub_category_supplier",
        "assembly_code_nickname",
        "ITEMID",
        "status",
        "ENDOFLife",
        "description",
        "Budget_Year",
        "historical_year",
        "BudgetCostofGoods",
        "budget_vs_py_cost_percent",  # 'budget_vs_ppy_cost_percent',
        "cost_mix_percent",
        "cost_actuals",
        "cost_act_forecast",
        "cost_act_forecast_vs_budget_perc",
        "cost_of_goods_ly",
        "cost_act_forecast_vs_ly_percent",
        # 'cost_of_goods_lly',
        # 'cost_act_forecast_vs_ppy_percent',
        "cost_act_or_forecast_per_sku",
        "stock_cost_ly",
    ]

    BudgetQuantity = [
        "Channel",
        "Region",
        "INVENTLOCATIONID",
        "Store",
        "Department",
        "category_name",
        "family",
        "sub_family",
        "supplier",
        "dom_comm",
        "sub_category",
        "extended_sub_category",
        "sub_category_supplier",
        "assembly_code_nickname",
        "ITEMID",
        "status",
        "ENDOFLife",
        "description",
        "Budget_Year",
        "historical_year",
        "budget_qty",
        "Budget_Qty_Perc",
        "budget_qty_act_fct_percent",
        "budget_vs_py_qty_percent",
        "budget_vs_ppy_qty_percent",
        "QTY_Buy_By_SKU",
        # 'Quantity_Buy_By_SKU',
        "total_sku_count",
        "unit_buy_by_sku_total",
        "SKU_COUNT",
        "quantity_mix_percent",
        "quantity_actuals",
        "quantity_act_forecast",
        "quantity_act_forecast_vs_budget_percent",
        "sold_qty_ly",
        "quantity_act_forecast_vs_ly_percent",
        "quantity_act_or_forecast_per_sku",
        "quantity_ppy",
        "quantity_act_forecast_vs_ppy_percent",
    ]

    BudgetMargin = [
        "Channel",
        "Region",
        "INVENTLOCATIONID",
        "Store",
        "Department",
        "category_name",
        "family",
        "sub_family",
        "supplier",
        "dom_comm",
        "sub_category",
        "extended_sub_category",
        "sub_category_supplier",
        "assembly_code_nickname",
        "ITEMID",
        "status",
        "ENDOFLife",
        "description",
        "Budget_Year",
        "historical_year",
        "budget_margin_percent",
        "budget_margin_mix_percent",
        # 'retail_price',
        "budget_vs_act_forecast_margin_percent",
        "budget_vs_py_margin_percent",
        "budget_vs_ppy_margin_percent",
        "ly_margin",
        "margin_act_forecast_vs_ly_percent",
        "total_sku_count",
        "margin_budget_per_sku",
        "margin_act_or_forecast_per_sku",
        "lly_margin",
        "margin_act_forecast_vs_ppy_percent",
        "margin_actuals",
        "margin_act_forecast",
        "budget_gross_margin_percent",
        "Historical_Gross_Margin",
        "adjusted_budget_gross_margin_percent",
        "FirstMargin_percent",
        "proposed_sellthru_percent",
    ]

class filterCols:
    
    time_columns = ["historical_year", "Budget_Year"]

    filter_details = {
        "sales_channel": "Channel",
        "product_family": "family",
        "sub_families": "sub_family",
        "category": "category_name",
        "sub_category": "sub_category",
        "suppliers": "supplier",
        "sku": "ITEMID",
    }

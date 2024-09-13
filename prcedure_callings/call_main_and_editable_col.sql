
CALL get_sp_otb_1('2023-03-01', '2023-03-03', '2024-07-01', '2024-07-03', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], Null, ARRAY[]::text[])
select otb_retail_value_at_gross_sale from item_counter;
CALL otb_purchase_retail_value_at_gross_sale()
CALL get_sp_otb_itemwise_calculations_1(
	'2024-07-01',
	'2024-07-02')
	
CALL public.get_sp_otb_main_aggregation(
	'item_counter',
	ARRAY[]::text[],
	False,
	ARRAY['final_price', 'act_forecast_vs_budget_percent', 'SalesActualsByForecast', 'proposed_sellthru_percent', 'retail_value_including_markdown', 'budget_gross_margin_percent', 'Logistic%', 'adjusted_budget_gross_margin_percent', 'MarkdownValue', 'markdown_percent', 'DisplayItemValue'],
	ARRAY['budget_amount', 'budget_cost', 'deficit', 'Original_BudgetAmount', 'budget_percent', 'relative_budget_percent', 'History_Net_Sales', 'budget_qty', 'ACT_FCT', 'Original_BudgetCostofGoods', 'PO', 'COSTPRICE', 'Historical_Gross_Margin', 'sales_actual', 'stock_on_hand_qty', 'StockatCostPrice', 'net_sales_ly', 'net_sales_lly', 'gross_sales_ly', 'initial_average_retail_price', 'total_sku_count', 'ly_customer_disc', 'SupplyCost', 'SALESQTY', 'DisplayItemQty', 'COR_EOLStock_value', 'Budget_Qty_Perc', 'budget_qty_act_fct_percent', 'budget_vs_py_qty_percent', 'budget_vs_ppy_qty_percent', 'QTY_Buy_By_SKU', 'total_sku_count_ty', 'total_sku_count_lly', 'total_sku_count_ly', 'quantity_mix_percent', 'quantity_act_forecast', 'quantity_act_forecast_vs_budget_percent', 'sold_qty_ly', 'quantity_act_forecast_vs_ly_percent', 'quantity_act_or_forecast_per_sku', 'quantity_act_forecast_vs_ppy_percent', 'budget_cost_percent', 'budget_vs_act_forecast_cost_percent', 'cost_budget_per_sku', 'BudgetCostofGoods', 'budget_vs_py_cost_percent', 'budget_vs_ppy_cost_percent', 'cost_mix_percent', 'cost_actuals', 'cost_of_goods_ly', 'cost_act_forecast', 'cost_act_forecast_vs_budget_perc', 'cost_act_forecast_vs_ly_percent', 'cost_act_forecast_vs_ppy_percent', 'cost_act_or_forecast_per_sku', 'budget_margin_percent', 'budget_margin_mix_percent', 'budget_vs_act_forecast_margin_percent', 'margin_actuals', 'margin_act_forecast', 'units_per_sku_total', 'units_per_sku_lly', 'unit_buy_by_sku_total', 'unit_buy_by_sku_ly', 'unit_buy_by_sku_lly', 'initial_average_retail_price_ly', 'initial_average_retail_price_lly', 'budget_per_sku', 'budget_qty_ty', 'supply_retail_value', 'stock_cost_ly', 'gross_sales'],
	ARRAY['Check_box'],
	ARRAY['new_budget_mix', 'revised_budget_amount'],
	ARRAY[]::text[],
	'coefficient_score',
	'coefficient_score_mix_percent'
)

CALL public.get_sp_otb_commit_drilldown(
    'otb_min_filter',
    ARRAY['Channel', 'family'],  -- replace with actual group values
	ARRAY['final_price', 'act_forecast_vs_budget_percent', 'SalesActualsByForecast', 'proposed_sellthru_percent', 'retail_value_including_markdown', 'budget_gross_margin_percent', 'Logistic%', 'adjusted_budget_gross_margin_percent', 'MarkdownValue', 'markdown_percent', 'DisplayItemValue'],
	ARRAY['budget_amount', 'budget_cost', 'deficit', 'Original_BudgetAmount', 'budget_percent', 'relative_budget_percent', 'History_Net_Sales', 'budget_qty', 'ACT_FCT', 'Original_BudgetCostofGoods', 'PO', 'COSTPRICE', 'Historical_Gross_Margin', 'sales_actual', 'stock_on_hand_qty', 'StockatCostPrice', 'net_sales_ly', 'net_sales_lly', 'gross_sales_ly', 'initial_average_retail_price', 'total_sku_count', 'ly_customer_disc', 'SupplyCost', 'SALESQTY', 'DisplayItemQty', 'COR_EOLStock_value', 'Budget_Qty_Perc', 'budget_qty_act_fct_percent', 'budget_vs_py_qty_percent', 'budget_vs_ppy_qty_percent', 'QTY_Buy_By_SKU', 'total_sku_count_ty', 'total_sku_count_lly', 'total_sku_count_ly', 'quantity_mix_percent', 'quantity_act_forecast', 'quantity_act_forecast_vs_budget_percent', 'sold_qty_ly', 'quantity_act_forecast_vs_ly_percent', 'quantity_act_or_forecast_per_sku', 'quantity_act_forecast_vs_ppy_percent', 'budget_cost_percent', 'budget_vs_act_forecast_cost_percent', 'cost_budget_per_sku', 'BudgetCostofGoods', 'budget_vs_py_cost_percent', 'budget_vs_ppy_cost_percent', 'cost_mix_percent', 'cost_actuals', 'cost_of_goods_ly', 'cost_act_forecast', 'cost_act_forecast_vs_budget_perc', 'cost_act_forecast_vs_ly_percent', 'cost_act_forecast_vs_ppy_percent', 'cost_act_or_forecast_per_sku', 'budget_margin_percent', 'budget_margin_mix_percent', 'budget_vs_act_forecast_margin_percent', 'margin_actuals', 'margin_act_forecast', 'units_per_sku_total', 'units_per_sku_lly', 'unit_buy_by_sku_total', 'unit_buy_by_sku_ly', 'unit_buy_by_sku_lly', 'initial_average_retail_price_ly', 'initial_average_retail_price_lly', 'budget_per_sku', 'budget_qty_ty', 'supply_retail_value', 'stock_cost_ly', 'gross_sales'],
	ARRAY['new_budget_mix', 'revised_budget_amount'],
	ARRAY['Check_box'],
	ARRAY[]::text[],
    ARRAY['max_col1', 'max_col2'],  -- replace with actual max column names
    'coefficient_score',  -- replace with actual coefficient score
    'coefficient_score_mix_percent',  -- replace with actual coefficient score mix percent
    ARRAY['columnname1', 'columnname2'],  -- replace with actual column names
    ARRAY['columnvalue1', 'columnvalue2']  -- replace with actual column values
);

CALL public.get_sp_otb_editable_columns_operations(
    '{
	  "Channel": "MINI",
	  "final_price": 0,
	  "act_forecast_vs_budget_percent": 0,
	  "SalesActualsByForecast": 0,
	  "proposed_sellthru_percent": 100,
	  "retail_value_including_markdown": null,
	  "budget_gross_margin_percent": 33.9999999864,
	  "Logistic%": 5,
	  "adjusted_budget_gross_margin_percent": 33.9999999864,
	  "MarkdownValue": null,
	  "markdown_percent": null,
	  "adjusted_markdown_percent": null,
	  "adjusted_sellthru_percent": null,
	  "DisplayItemValue": 0,
	  "DisplayItemQty": 0,
	  "COR_EOLStock_value": 0,
	  "otb_retail_value_at_gross_sale": null,
	  "budget_amount": 1025.712121,
	  "budget_cost": 676.97,
	  "deficit": 1025.7121,
	  "Original_BudgetAmount": 1025.7121,
	  "budget_percent": 0.0079148541,
	  "relative_budget_percent": 0.0079148541,
	  "History_Net_Sales": 0,
	  "budget_qty": 1,
	  "ACT_FCT": 1025.7121,
	  "Original_BudgetCostofGoods": 676.97,
	  "PO": 0,
	  "COSTPRICE": 1051.32,
	  "Historical_Gross_Margin": 0,
	  "sales_actual": 0,
	  "stock_on_hand_qty": 0,
	  "StockatCostPrice": 0,
	  "net_sales_ly": 0,
	  "net_sales_lly": 0,
	  "gross_sales_ly": 0,
	  "initial_average_retail_price": 1025.712121,
	  "total_sku_count": 1,
	  "ly_customer_disc": 0,
	  "SupplyCost": 643.1215,
	  "SALESQTY": 0,
	  "Budget_Qty_Perc": 0.0501,
	  "budget_qty_act_fct_percent": 100,
	  "budget_vs_py_qty_percent": 100,
	  "budget_vs_ppy_qty_percent": 100,
	  "QTY_Buy_By_SKU": 1,
	  "total_sku_count_ty": 879,
	  "total_sku_count_lly": 0,
	  "total_sku_count_ly": 1,
	  "quantity_mix_percent": 0,
	  "quantity_act_forecast": 1,
	  "quantity_act_forecast_vs_budget_percent": 0,
	  "sold_qty_ly": 0,
	  "quantity_act_forecast_vs_ly_percent": 0,
	  "quantity_act_or_forecast_per_sku": 0,
	  "quantity_act_forecast_vs_ppy_percent": 0,
	  "budget_cost_percent": 0.0001,
	  "budget_vs_act_forecast_cost_percent": 0,
	  "cost_budget_per_sku": 0,
	  "BudgetCostofGoods": 676.97,
	  "budget_vs_py_cost_percent": 67697,
	  "budget_vs_ppy_cost_percent": 0,
	  "cost_mix_percent": 0,
	  "cost_actuals": 0,
	  "cost_of_goods_ly": 1051.32,
	  "cost_act_forecast": 676.97,
	  "cost_act_forecast_vs_budget_perc": 0,
	  "cost_act_forecast_vs_ly_percent": 0,
	  "cost_act_forecast_vs_ppy_percent": 0,
	  "cost_act_or_forecast_per_sku": 0,
	  "budget_margin_percent": 34874.2121,
	  "budget_margin_mix_percent": 0,
	  "budget_vs_act_forecast_margin_percent": 0,
	  "margin_actuals": 0,
	  "margin_act_forecast": 0,
	  "units_per_sku_total": 0.0001,
	  "units_per_sku_lly": 0,
	  "unit_buy_by_sku_total": 0.1479,
	  "unit_buy_by_sku_ly": 0,
	  "unit_buy_by_sku_lly": 0,
	  "initial_average_retail_price_ly": 0,
	  "initial_average_retail_price_lly": 0,
	  "budget_per_sku": 0.1479,
	  "budget_qty_ty": 0,
	  "supply_retail_value": 1025.712121,
	  "stock_cost_ly": 0,
	  "gross_sales": 0,
	  "Check_box": 0,
	  "budget_vpy": 0,
	  "budget_vppy": 0,
	  "LYvsACT_FCT_percent": 0,
	  "SALES_ACT_FCT_per_sku_ly": 1025.7121,
	  "LLYvsACT_FCT_percent": 0,
	  "SALES_ACT_FCT_per_sku_lly": 0,
	  "FirstMargin_percent": 37.299999987,
	  "ly_margin_percent": 0,
	  "ly_margin": -1051.32,
	  "margin_act_forecast_vs_ly_margin_percent": 0,
	  "margin_act_or_forecast_per_sku": 0,
	  "budget_gross_margin": 348.742121,
	  "budget_vs_py_margin_percent": 0,
	  "StockatRetailPrice": 0,
	  "TYForecast": 0,
	  "GrossSales": 1025.712121,
	  "PurchaseRetailValueatGrossSale": null,
	  "OTBorPurchaseCost": null,
	  "OTBquantity": null
	}',
    'budget_percent',
    30,
    ARRAY['Channel', 'family'],
    ARRAY['RETAIL', 'AC'],
    10,
    20,
    TRUE
)

select sum(budget_percent) from item_counter where "Channel" = 'RETAIL' AND "family" = 'AC' --AND "family" = 'AC
select sum(budget_amount) from item_counter where "Channel" = 'RETAIL' --AND "family" = 'AC';
select sum(budget_percent) from item_counter where "Channel" != 'RETAIL'
select sum(retail_value_including_markdown) from item_counter 
	SELECT 
    CASE 
        WHEN ("LINEDISC" + "LINEAMOUNT") = 0 THEN 0 
        ELSE "LINEDISC" / ("LINEDISC" + "LINEAMOUNT") 
    END 
FROM salesdata_trnx;

select sum(40 / (SELECT COUNT(*) FROM item_counter WHERE "Channel" = 'RETAIL')) FROM item_counter WHERE "Channel" = 'RETAIL';
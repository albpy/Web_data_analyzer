-- PROCEDURE: public.get_sp_otb_root_frame_calc()

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_root_frame_calc();

CREATE OR REPLACE PROCEDURE public.get_sp_otb_root_frame_calc(
	)
LANGUAGE 'plpgsql'
AS $BODY$
-- DECLARE 
--     result_row RECORD;

	BEGIN

      
-- 	    ALTER TABLE ra_and_stock_temp ADD COLUMN price NUMERIC(20, 4) DEFAULT 0;
-- 		UPDATE ra_and_stock_temp 
-- 		SET price = final_price;
		

		-- df = df.with_columns(Check_box = pl.lit(0).cast(pl.Int8))

ALTER TABLE ra_and_stock_temp 
ADD COLUMN "Check_box" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "DisplayItemQty" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "Logistic%" NUMERIC(20, 4) DEFAULT 5,
ADD COLUMN "DisplayItemValue" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "COR_EOLStock_value" NUMERIC(20, 4) DEFAULT 0.0,
ADD COLUMN "Discount" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN markdown_percent NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "BudgetCostofGoods" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "SupplyCost" NUMERIC(20, 4) DEFAULT 10,
ADD COLUMN "FirstMargin_percent" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN proposed_sellthru_percent NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "Original_BudgetAmount" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "Sold_Quantity" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "StockQuantity" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "SalesActualsByForecast" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "PO" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "MarkdownValue" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "History_Net_Sales" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN deficit NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "Historical_Gross_Margin" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "Budget_Qty_Perc" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN budget_qty_act_fct_percent NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN budget_vs_py_qty_percent NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN budget_vs_ppy_qty_percent NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN dates DATE,
ADD COLUMN "Budget_year" varchar(20),
ADD COLUMN budget_weekday integer DEFAULT 0,
ADD COLUMN budget_quarter varchar(20),
ADD COLUMN budget_month varchar(20),
ADD COLUMN budget_week varchar(20);

-- UPDATE ra_and_stock_temp 
-- SET "DisplayItemValue" = ("DisplayItemQty" * initial_average_retail_price),
--     "Discount" = (initial_average_retail_price * "SALESQTY") - (price * "SALESQTY"),
--     markdown_percent = ("Discount" / budget_amount) + "Discount",
--     "BudgetCostofGoods" = ((budget_amount) - (budget_amount * budget_gross_margin_percent) / 100),
--     "SupplyCost" = ("BudgetCostofGoods" - ("BudgetCostofGoods" * ("Logistic%"/100))),
--     proposed_sellthru_percent = (cost_of_goods_ly / COALESCE(NULLIF(cost_of_goods_ly + stock_cost_ly, 0), 1)) * 100,
--     "Original_BudgetAmount" = budget_amount,
--     "Sold_Quantity" = "SALESQTY",
--     "StockQuantity" = stock_on_hand_qty,
--     "SalesActualsByForecast" = (sales_actual / budget_amount) * 100,
--     "PO" = (budget_amount + (initial_average_retail_price * stock_on_hand_qty)),
--     "MarkdownValue" = (gross_sales - net_sales_ly),
--     "History_Net_Sales" = (net_sales_lly + net_sales_ly + sales_actual),
--     deficit = (budget_amount - (final_price * budget_qty)),
--     "Historical_Gross_Margin" = ("History_Net_Sales" - "COSTPRICE"),
--     "Budget_Qty_Perc" = (budget_qty / COALESCE(NULLIF(s.total_budget_qty, 0), 1)) * 100,
--     budget_qty_act_fct_percent = (budget_qty / COALESCE(NULLIF(quantity_actuals, 0), 1)) * 100,
--     budget_vs_py_qty_percent = (budget_qty / COALESCE(NULLIF(sold_qty_ly, 0), 1)) * 100,
--     budget_vs_ppy_qty_percent = (budget_qty / COALESCE(NULLIF(quantity_ppy, 0), 1)) * 100
-- FROM (
--     SELECT SUM(budget_qty) AS total_budget_qty
--     FROM ra_and_stock_temp
-- ) AS s;

UPDATE ra_and_stock_temp 
SET "DisplayItemValue" = ("DisplayItemQty" * initial_average_retail_price),
--     "Discount" = (initial_average_retail_price * "SALESQTY") - (final_price * "SALESQTY"),
     "Discount" = (initial_average_retail_price * "sold_qty_ly") - (final_price * "sold_qty_ly"),
--      markdown_percent = ("Discount" / budget_amount) ,
--      markdown_percent = (((initial_average_retail_price * "sold_qty_ly") - (final_price * "sold_qty_ly")) / COALESCE(NULLIF((initial_average_retail_price * "sold_qty_ly"),0),1)) * 100 ,
    -- "BudgetCostofGoods" = (budget_amount - (budget_amount * budget_gross_margin_percent)),
    "BudgetCostofGoods" = budget_cost, 
	-- "BudgetCostofGoods" = budget_amount - (1-adjusted_budget_gross_margin_percent),

	-- "SupplyCost" = (budget_amount - (budget_amount * budget_gross_margin_percent)) - (COALESCE((budget_amount - (budget_amount * budget_gross_margin_percent)),0) * ("Logistic%" / 100)),
	"SupplyCost" = budget_cost - (COALESCE(budget_cost,0) * ("Logistic%" / 100)),
	-- "FirstMargin%" = ((budget_amount -  budget_cost - (COALESCE(budget_cost,0) * ("Logistic%" / 100))) / COALESCE(NULLIF(budget_amount,0),1)) * 100,
	"FirstMargin_percent" = (budget_amount -  (budget_cost -(COALESCE(budget_cost,0) * ("Logistic%" / 100))))/ COALESCE(NULLIF(budget_amount,0),1) * 100,
	
	proposed_sellthru_percent = (cost_of_goods_ly / COALESCE(NULLIF(cost_of_goods_ly + stock_cost_ly, 0), 1)) * 100,
    "Original_BudgetAmount" = budget_amount,
    "Sold_Quantity" = "SALESQTY",
    "StockQuantity" = stock_on_hand_qty,
    "SalesActualsByForecast" = (sales_actual / COALESCE(NULLIF(budget_amount,0),1)) * 100,
    "PO" = (budget_amount + (initial_average_retail_price * stock_on_hand_qty)),
--  "MarkdownValue" = (gross_sales - net_sales_ly),
--     "MarkdownValue" =((initial_average_retail_price * "SALESQTY") - (final_price * "SALESQTY")), 
    "MarkdownValue" =((initial_average_retail_price * "sold_qty_ly") - (final_price * "sold_qty_ly")), 
	"markdown_percent" = (((initial_average_retail_price * "sold_qty_ly") - (final_price * "sold_qty_ly")) / COALESCE(NULLIF((initial_average_retail_price * "sold_qty_ly"),0),1))*100,
	
-- 	select sum("markdown_percent") as percent ,sum("MarkdownValue") as value from ra_and_stock_temp;
	
--     "History_Net_Sales" = (net_sales_lly + net_sales_ly + sales_actual),
    deficit = (budget_amount - (final_price * budget_qty)),
--     "Historical_Gross_Margin" = ("History_Net_Sales" - "COSTPRICE"),
    budget_qty_act_fct_percent = (budget_qty / COALESCE(NULLIF(quantity_actuals, 0), 1)) * 100,
    budget_vs_py_qty_percent = (budget_qty / COALESCE(NULLIF(sold_qty_ly, 0), 1)) * 100,
    budget_vs_ppy_qty_percent = (budget_qty / COALESCE(NULLIF(quantity_ppy, 0), 1)) * 100,
	dates ="Budget_date"::date,
	"Budget_year" = extract(year from "Budget_date"::date),
	budget_weekday = extract(dow from "Budget_date"::date)::int + 1,
	budget_quarter = extract(QUARTER from "Budget_date"::date),
	budget_month = to_char("Budget_date"::date, 'Month'),
	budget_week = to_char("Budget_date"::date, 'Day');
	
	-- SELECT budget_week, budget_weekday, budget_month from item_counter

UPDATE ra_and_stock_temp 
    SET "Budget_Qty_Perc" = 
		CASE 
			WHEN s.total_budget_qty = 0 THEN 0
			ELSE budget_qty/COALESCE(NULLIF(s.total_budget_qty, 0), 1) * 100
		END
	
FROM (
    SELECT SUM(budget_qty) AS total_budget_qty
    FROM ra_and_stock_temp
) AS s;

END
$BODY$;
ALTER PROCEDURE public.get_sp_otb_root_frame_calc()
    OWNER TO mohit;

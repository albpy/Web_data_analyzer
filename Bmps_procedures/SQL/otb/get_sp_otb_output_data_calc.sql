-- PROCEDURE: public.get_sp_otb_output_data_calc()

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_output_data_calc();

CREATE OR REPLACE PROCEDURE public.get_sp_otb_output_data_calc(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	progress INT := 0;
BEGIN
	progress := 20;
	INSERT INTO public.progress_status(status, progress) VALUES ('Invoked get_sp_otb_output_data_calc'::text, progress);

ALTER TABLE item_counter 
ADD COLUMN budget_vpy NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN budget_vppy NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN total_budget NUMERIC(20, 4) DEFAULT 0,
-- ADD COLUMN relative_budget_percent NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "LYvsACT_FCT_percent" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "SALES_ACT_FCT_per_sku_ly" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "LLYvsaCT_FCT_percent" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "SALES_ACT_FCT_per_sku_lly" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "FirstMargin_percent" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "PurchasedRetailValueGrossSale" NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN ly_margin_percent NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN ly_margin  NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN margin_act_forecast_vs_ly_percent  NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN margin_act_or_forecast_per_sku  NUMERIC(20, 4) DEFAULT 0,
-- ADD COLUMN budget_gross_margin  NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN budget_vs_py_margin_percent  NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "StockatRetailPrice"   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "StockatCostPrice"   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "TYForecast"   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "PurchaseRetailValueatGrossSale"    NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "OTBquantity"   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "OTBorPurchaseCost"   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN "GrossSales"   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN lly_margin_percent   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN lly_margin   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN margin_act_forecast_vs_ppy_percent   NUMERIC(20, 4) DEFAULT 0,
ADD COLUMN budget_vs_ppy_margin_percent   NUMERIC(20, 4) DEFAULT 0;

	progress := 21;
	INSERT INTO public.progress_status(status, progress) VALUES ('claculations get_sp_otb_output_data_calc'::text, progress);

UPDATE item_counter
SET  budget_vpy = 
     case 
	     when net_sales_ly = 0 then 0
	     else (budget_amount / net_sales_ly) * 100
	 end,
	 
     budget_vppy = 
	 case 
	     when net_sales_lly = 0 then 0
	     else (budget_amount / net_sales_lly)* 100
	 end,

	 "FirstMargin_percent" = 
	 case
	    when budget_amount = 0 then 0
	    else ((budget_amount - "SupplyCost") / budget_amount )* 100
	 end,
-- data = data.with_columns(PurchasedRetailValueGrossSale = ((pl.col('budget_amount')/(100-pl.col('markdown_percent')))/(pl.col('proposed_sellthru_percent')/100)).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

	 "PurchasedRetailValueGrossSale" = 
	 case 
	     when proposed_sellthru_percent / 100 = 0 then 0
		 WHEN (100 - markdown_percent) = 0 THEN 0
		 else (budget_amount /(100 - markdown_percent)/(proposed_sellthru_percent / 100))
	 end,
			   
		 ly_margin_percent = 
	 case 
	     when net_sales_ly = 0 then 0
	     else((net_sales_ly -  cost_of_goods_ly) / net_sales_ly )*100
	 end,
	 ly_margin = net_sales_ly -  cost_of_goods_ly,
	 
	 margin_act_forecast_vs_ly_percent = 
	 case
	     when  (net_sales_ly -  cost_of_goods_ly) = 0 then 0
	     else (margin_act_forecast / (net_sales_ly -  cost_of_goods_ly) * 100)
	 end,
     
	 margin_act_or_forecast_per_sku = 
	 case
		   when total_sku_count = 0 then 0
		   else(margin_act_forecast / total_sku_count)
	end,
-- 	 budget_gross_margin = budget_gross_margin_percent / 100,
     budget_gross_margin = budget_amount - budget_cost,
	 budget_vs_py_margin_percent = 
	 case 
	    when  (ly_margin_percent * 100) = 0 then 0
	    else ((budget_amount - budget_cost) / (ly_margin_percent * 100))*100
	 end,
	 "StockatRetailPrice"  = initial_average_retail_price * stock_on_hand_qty,
	 "TYForecast" =  (initial_average_retail_price * stock_on_hand_qty) - "DisplayItemValue" - "COR_EOLStock_value",
	 "PurchaseRetailValueatGrossSale" = "PurchasedRetailValueGrossSale" - ((initial_average_retail_price * stock_on_hand_qty) - "DisplayItemValue" - "COR_EOLStock_value"),
	 "OTBquantity" = 
	 case
	    when initial_average_retail_price = 0 then 0
	    else ("PurchasedRetailValueGrossSale" - ((initial_average_retail_price * stock_on_hand_qty) - "DisplayItemValue" - "COR_EOLStock_value")) / initial_average_retail_price
	 end,
	 
	 "OTBorPurchaseCost" = 
	 case 
	     when budget_amount = 0 then 0
	     else "PurchasedRetailValueGrossSale" - ((initial_average_retail_price * stock_on_hand_qty) - "DisplayItemValue" - "COR_EOLStock_value") * (100 - ((budget_amount - "SupplyCost") / budget_amount) * 100)
	 end,
	 -- 	  -- "GrossSales" = (budget_amount / COALESCE(NULLIF((100 - markdown_percent),0),1))*100,
		"GrossSales" = 
			CASE
				WHEN (100 - markdown_percent) = 0 THEN 0
				ELSE "budget_amount" / COALESCE(NULLIF((100-markdown_percent), 0), 1) * 100
			END,
	-- lly_margin_percent = ((net_sales_lly - cost_of_goods_lly) /  COALESCE(NULLIF(net_sales_lly,0),1) * 100,
		"lly_margin_percent" =
			CASE
				WHEN net_sales_lly = 0 THEN 0
				ELSE (net_sales_lly - cost_of_goods_lly) / COALESCE(NULLIF(net_sales_lly,0),1) * 100
			END,
-- 	-- lly_margin = (((net_sales_lly - cost_of_goods_lly) /  COALESCE(NULLIF(net_sales_lly,0),1)) * 100) / 100,
		 "lly_margin" =
			CASE
				WHEN net_sales_lly = 0 THEN 0
				ELSE (net_sales_lly - cost_of_goods_lly) / COALESCE(NULLIF(net_sales_lly,0),1)
			END,
	
	-- margin_act_forecast_vs_ppy_percent =  (margin_act_forecast / COALESCE(NULLIF((lly_margin_percent * 100),0),1))* 100,
		"margin_act_forecast_vs_ppy_percent" =
			CASE
				WHEN lly_margin_percent = 0 THEN 0
				ELSE margin_act_forecast / COALESCE(NULLIF((lly_margin_percent),0),1)
			END,
	
	-- budget_vs_ppy_margin_percent = ((budget_amount - budget_cost) / COALESCE(NULLIF(((((net_sales_lly - cost_of_goods_lly) /  COALESCE(NULLIF(net_sales_lly,0),1)) * 100) * 100),0),1)) * 100,
	 	"budget_vs_ppy_margin_percent" =
			CASE
				WHEN lly_margin_percent = 0 THEN 0
				ELSE budget_gross_margin / COALESCE(NULLIF((lly_margin_percent),0),1)
			END,
 
 
 
 
	"StockatCostPrice" = 
    CASE
        WHEN net_sales_ly > 0 THEN
            CASE
                WHEN net_sales_ly * 100 = 0 THEN 0
                ELSE stock_on_hand_qty * initial_average_retail_price * (1 - (((net_sales_ly - cost_of_goods_ly) / COALESCE(NULLIF(net_sales_ly * 100, 0), 1)) / 100))
            END
        WHEN net_sales_ly <= 0 THEN
            CASE
                WHEN COALESCE(NULLIF(quantity_act_forecast, 0), 0) = 0 THEN 0
                ELSE (COALESCE(cost_act_forecast, 0) / COALESCE(NULLIF(quantity_act_forecast, 0), 1)) * COALESCE(stock_on_hand_qty, 0)
            END
        WHEN COALESCE("ACT_FCT", 0) <= 0 THEN
            CASE
                WHEN COALESCE(budget_qty, 0) * COALESCE(stock_on_hand_qty, 0) = 0 THEN 0
                ELSE COALESCE(budget_cost, 0) / COALESCE(NULLIF(budget_qty, 0), 1) * COALESCE(stock_on_hand_qty, 0)
            END
        ELSE 0
    END;
	 
	 
	 
	 
	progress := 22;
	INSERT INTO public.progress_status(status, progress) VALUES ('agg claculations get_sp_otb_output_data_calc'::text, progress);

WITH totals AS (
    SELECT 
        SUM(budget_amount) AS total_budget_amount,
        SUM(net_sales_ly) AS total_net_sales_ly,
	    SUM(net_sales_lly) AS total_net_sales_lly,
		SUM(total_sku_count_ly) AS sum_of_total_sku_count_ly,
		SUM(total_sku_count_lly) AS sum_of_total_sku_count_lly
		
    FROM 
        item_counter
)
UPDATE item_counter
SET 
--  relative_budget_percent = (budget_amount / COALESCE(NULLIF(totals.total_budget_amount, 0), 1)) * 100,
	--'LYvsACT_FCT_percent'=  ('net_sales_ly')/('net_sales_ly').sum()*100)

    "LYvsACT_FCT_percent" = 
	case
	   when totals.total_net_sales_ly = 0 then 0
	   else (net_sales_ly / totals.total_net_sales_ly) * 100
	end,
	
	-- 'SALES_ACT_FCT_per_sku_ly' = 'LYvsACT_FCT_percent'/'total_sku_count_ly'

	"SALES_ACT_FCT_per_sku_ly" = 
	case 
	   when totals.total_net_sales_ly != 0 THEN
			CASE 
				WHEN totals.sum_of_total_sku_count_ly != 0
				THEN 
				((net_sales_ly / totals.total_net_sales_ly) * 100) / totals.sum_of_total_sku_count_ly
				ELSE 0
			END
		ELSE 0
	END,

	-- 'LLYvsACT_FCT_percent' = 'net_sales_lly'/'net_sales_lly'.sum()*100)
	
	"LLYvsaCT_FCT_percent" = 
	case
	    when totals.total_net_sales_lly = 0 then 0
		else (net_sales_lly / totals.total_net_sales_lly) * 100
	end,
	
	"SALES_ACT_FCT_per_sku_lly" = 
	CASE WHEN totals.total_net_sales_lly != 0 
		THEN
		CASE WHEN totals.sum_of_total_sku_count_lly != 0 
			THEN 	
				((net_sales_lly / totals.total_net_sales_lly ) * 100) / totals.sum_of_total_sku_count_lly
			ELSE 0
		END
		ELSE 0
 	END
	
FROM 
    totals;

	progress := 23;
	INSERT INTO public.progress_status(status, progress) VALUES ('completed get_sp_otb_output_data_calc'::text, progress);

END
$BODY$;
ALTER PROCEDURE public.get_sp_otb_output_data_calc()
    OWNER TO mohit;

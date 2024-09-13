-- PROCEDURE: public.get_sp_otb_itemwise_calculations(date, date)

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_itemwise_calculations(date, date);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_itemwise_calculations(
	IN forecast_date_from date,
	IN forecast_date_to date)
LANGUAGE 'plpgsql'
AS $BODY$
	DECLARE 
		progress INT := 9;

		BEGIN
			progress := 9;
			INSERT INTO public.progress_status(status, progress) VALUES ('invoked procedure get_sp_otb_itemwise_calculations item_counter creation'::text, progress);

			CREATE TABLE item_counter AS 
				SELECT 
					*, 
					ROW_NUMBER() OVER (PARTITION BY "ITEMID") AS total_sku_count,
					ROW_NUMBER() OVER (PARTITION BY "ITEMID_ty") AS total_sku_count_ty,
					ROW_NUMBER() OVER (PARTITION BY "ITEMID_ly") AS total_sku_count_ly,
					ROW_NUMBER() OVER (PARTITION BY "ITEMID_lly") AS total_sku_count_lly 
				FROM ra_and_stock_temp;

			DROP TABLE IF EXISTS ra_and_stock_temp;

			ALTER TABLE item_counter 
			
				ADD COLUMN "QTY_Buy_By_SKU" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "unit_buy_by_sku_ly" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "unit_buy_by_sku_lly" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "units_per_sku_total" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "units_per_sku_ly" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "units_per_sku_lly" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "unit_buy_by_sku_total" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "quantity_mix_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "quantity_act_forecast" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "quantity_act_forecast_vs_budget_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "quantity_act_forecast_vs_ly_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "quantity_act_or_forecast_per_sku" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "quantity_act_forecast_vs_ppy_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "budget_per_sku_qty_total" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "budget_cost_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "cost_act_forecast" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "budget_vs_act_forecast_cost_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "cost_budget_per_sku" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "budget_vs_py_cost_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "budget_vs_ppy_cost_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "cost_mix_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "cost_act_forecast_vs_budget_perc" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "cost_act_forecast_vs_ly_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "cost_act_forecast_vs_ppy_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "cost_act_or_forecast_per_sku" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "Original_BudgetCostofGoods" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "margin_actuals" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "budget_gross_margin" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "margin_act_forecast" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "ACT_FCT" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "budget_margin_percent" NUMERIC(20, 4) DEFAULT 0,		
				ADD COLUMN "budget_vs_act_forecast_margin_percent" NUMERIC(20, 4) DEFAULT 0,		
				ADD COLUMN "act_forecast_vs_budget_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "coefficient_score" INTEGER DEFAULT 0,
				ADD COLUMN "coefficient_score_mix_percent" INTEGER DEFAULT 0,
				ADD COLUMN "budget_margin_mix_percent" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "margin_budget_per_sku" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "budget_per_sku" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "units_per_sku_total_ly" NUMERIC(20, 4) DEFAULT 0,
				ADD COLUMN "units_per_sku_total_lly" NUMERIC(20, 4) DEFAULT 0;
				
			progress := 10;
			INSERT INTO public.progress_status(status, progress) VALUES ('formulas exec get_sp_otb_itemwise_calculations'::text, progress);
	

	
				UPDATE item_counter
					SET 
					total_sku_count =
						CASE 
					        WHEN total_sku_count != 1 AND "budget_amount" = 0 THEN 0
					        ELSE 1
					    END,

					total_sku_count_ly =
						CASE 
					        WHEN total_sku_count_ly != 1 AND "net_sales_ly" = 0 THEN 0
					        ELSE 1
					    END,

					total_sku_count_lly =
						CASE 
					        WHEN total_sku_count_lly != 1 AND "net_sales_lly" = 0 THEN 0
					        ELSE 1
					    END,
					
					"QTY_Buy_By_SKU" = 
					    CASE 
					        WHEN total_sku_count = 0 THEN 0
					        ELSE COALESCE(NULLIF(total_sku_count, 0), 1) * budget_qty / NULLIF(total_sku_count, 0)
					    END,

-- -- df = df.with_columns(((pl.col('budget_qty_ly')/pl.col('total_sku_count_ly')).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('unit_buy_by_sku_ly'))

-- 					"unit_buy_by_sku_ly" = 
-- 						CASE
-- 							WHEN "total_sku_count_ly" = 0 THEN 0
-- 							ELSE COALESCE(NULLIF("total_sku_count_ly", 0), 1) * "budget_qty_ly"/ NULLIF("total_sku_count_ly", 0)
-- 						END,

-- -- df = df.with_columns(((pl.col('budget_qty_lly')/pl.col('total_sku_count_lly')).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('unit_buy_by_sku_lly'))

	
-- 					"unit_buy_by_sku_lly" = 
-- 						CASE
-- 							WHEN "total_sku_count_lly" = 0 THEN 0
-- 							ELSE COALESCE(NULLIF("total_sku_count_lly", 0), 1) * "budget_qty_lly"/ NULLIF("total_sku_count_lly", 0)
-- 						END,

-- -- df = df.with_columns(((pl.col('net_sales_ly')/pl.col('total_sku_count_ly')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('units_per_sku_ly'))

	
-- 					"units_per_sku_ly" = 
-- 						CASE
-- 							WHEN "total_sku_count_ly" = 0 THEN 0
-- 							ELSE COALESCE(NULLIF("total_sku_count_ly", 0), 1) * "net_sales_ly"/ NULLIF("total_sku_count_ly", 0)
-- 						END,

-- -- df = df.with_columns(((pl.col('net_sales_lly')/pl.col('total_sku_count_lly')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('units_per_sku_lly'))

	
-- 					"units_per_sku_lly" = 
-- 						CASE
-- 							WHEN "total_sku_count_lly" = 0 THEN 0
-- 							ELSE COALESCE(NULLIF("total_sku_count_lly", 0), 1) * "net_sales_lly"/ NULLIF("total_sku_count_lly", 0)
-- 						END,
-- df = df.with_columns(((pl.col('budget_amount')/pl.col('total_sku_count')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('unit_buy_by_sku_total'))

	
					-- "unit_buy_by_sku_total" = 
					-- 	CASE
					-- 		WHEN "total_sku_count" = 0 THEN 0
					-- 		ELSE COALESCE(NULLIF("total_sku_count", 0), 1) * "budget_amount"/ NULLIF("total_sku_count", 0)
					-- 	END,

--       df = df.with_columns((pl.col('budget_qty')+pl.col('quantity_actuals')).alias('quantity_act_forecast'))
        	

	
					"quantity_act_forecast" = "budget_qty" + "quantity_actuals",

  --       df = df.with_columns(((pl.col('budget_qty')/pl.col('quantity_act_forecast')).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias('quantity_act_forecast_vs_budget_percent'))

	
					"quantity_act_forecast_vs_budget_percent" = 
						CASE
							WHEN "quantity_act_forecast" = 0 THEN 0
							ELSE COALESCE(NULLIF("quantity_act_forecast", 0), 1) * "budget_qty" / NULLIF("quantity_act_forecast", 0) * 100
						END,
  --       df = df.with_columns(((pl.col('quantity_act_forecast')/pl.col('sold_qty_ly')).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias('quantity_act_forecast_vs_ly_percent'))
			

	
					"quantity_act_forecast_vs_ly_percent" = 
						CASE
							WHEN "sold_qty_ly" = 0 THEN 0
							ELSE COALESCE(NULLIF("sold_qty_ly", 0), 1) * "budget_qty" / NULLIF("sold_qty_ly", 0) * 100
						END,
  --       df = df.with_columns(((pl.col('quantity_act_forecast')/pl.col('total_sku_count')).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias('quantity_act_or_forecast_per_sku'))

	
					"quantity_act_or_forecast_per_sku" = 
						CASE
							WHEN "total_sku_count" = 0 THEN 0
							ELSE COALESCE(NULLIF("total_sku_count", 0), 1) * "quantity_act_forecast" / NULLIF("total_sku_count", 0) * 100
						END,     
  --       df = df.with_columns(((pl.col('quantity_act_forecast')/pl.col('quantity_ppy')).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias('quantity_act_forecast_vs_ppy_percent'))

	
					"quantity_act_forecast_vs_ppy_percent" = 
						CASE
							WHEN "quantity_ppy" = 0 THEN 0
							ELSE COALESCE(NULLIF("quantity_ppy", 0), 1) * "quantity_act_forecast" / NULLIF("quantity_ppy", 0) * 100
						END,   
  --       df = df.with_columns(((pl.col('budget_amount') /pl.col('total_sku_count')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('budget_per_sku_qty_total'))

	
					"budget_per_sku_qty_total" = 
						CASE
							WHEN "total_sku_count" = 0 THEN 0
							ELSE COALESCE(NULLIF("total_sku_count", 0), 1) * "budget_amount" / NULLIF("total_sku_count", 0) * 100
						END,     

  
   --      df = df.with_columns(((pl.col('budget_cost')+pl.col('cost_actuals')).replace({-np.inf:0, np.inf:0}).fill_nan(0)).alias('cost_act_forecast'))
  			

	
					"cost_act_forecast" = "budget_cost" + "cost_actuals",
						
   --      df = df.with_columns(((pl.col('budget_cost')/pl.col('cost_act_forecast'))*100).alias('budget_vs_act_forecast_cost_percent'))

	
					"budget_vs_act_forecast_cost_percent" = 
						CASE
							WHEN "cost_act_forecast" = 0 THEN 0
							ELSE COALESCE(NULLIF("cost_act_forecast", 0), 1) * "budget_cost" / NULLIF("cost_act_forecast", 0) * 100
						END,
   --      df = df.with_columns(((pl.col('budget_cost')/pl.col('total_sku_count')).replace({-np.inf:0, np.inf:0}).fill_nan(0)).alias('cost_budget_per_sku'))

	
					"cost_budget_per_sku" = 
						CASE
							WHEN "cost_act_forecast" = 0 THEN 0
							ELSE COALESCE(NULLIF("total_sku_count", 0), 1) * "budget_cost" / NULLIF("total_sku_count", 0)
						END,
   --      df = df.with_columns(((pl.col('budget_cost')/pl.col('cost_of_goods_ly')).replace({-np.nan:0, np.nan:0}).fill_nan(0)*100).alias('budget_vs_py_cost_percent'))

	
					"budget_vs_py_cost_percent" = 
						CASE
							WHEN "cost_of_goods_ly" = 0 THEN 0
							ELSE COALESCE(NULLIF("cost_of_goods_ly", 0), 1) * "budget_cost" / NULLIF("cost_of_goods_ly", 0) * 100
						END,
   --      df = df.with_columns(((pl.col('budget_cost')/pl.col('cost_of_goods_lly')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('budget_vs_ppy_cost_percent'))

	
					"budget_vs_ppy_cost_percent" = 
						CASE
							WHEN "cost_of_goods_lly" = 0 THEN 0
							ELSE COALESCE(NULLIF("cost_of_goods_lly", 0), 1) * "budget_cost" / NULLIF("cost_of_goods_lly", 0) * 100
						END,
  
   --      df = df.with_columns(((pl.col('cost_act_forecast')/pl.col('budget_cost'))*100).alias('cost_act_forecast_vs_budget_perc'))

	
					"cost_act_forecast_vs_budget_perc" = 
						CASE
							WHEN "budget_cost" = 0 THEN 0
							ELSE COALESCE(NULLIF("cost_of_goods_lly", 0), 1) * "cost_act_forecast" / NULLIF("budget_cost", 0) * 100
					END,
   --      df = df.with_columns(((pl.col('cost_act_forecast')/pl.col('cost_of_goods_ly'))*100).alias('cost_act_forecast_vs_ly_percent'))

	
					"cost_act_forecast_vs_ly_percent" = 
						CASE
							WHEN "cost_of_goods_ly" = 0 THEN 0
							ELSE COALESCE(NULLIF("cost_of_goods_ly", 0), 1) * "cost_act_forecast" / NULLIF("cost_of_goods_ly", 0) * 100
					END,
   --      df = df.with_columns((((pl.col('cost_act_forecast').replace({-np.inf:0, np.inf:0}).fill_nan(0)/pl.col('cost_of_goods_lly').replace({-np.inf:0, np.inf:0}).fill_nan(0)).replace({-np.inf:0, np.inf:0}).fill_nan(0))*100).alias('cost_act_forecast_vs_ppy_percent'))

	
					"cost_act_forecast_vs_ppy_percent" = 
						CASE
							WHEN "cost_of_goods_lly" = 0 THEN 0
							ELSE COALESCE(NULLIF("cost_of_goods_lly", 0), 1) * "cost_act_forecast" / NULLIF("cost_of_goods_lly", 0) * 100
					END,
   --      df = df.with_columns(((pl.col('cost_act_forecast') /pl.col('total_sku_count')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('cost_act_or_forecast_per_sku'))

	
					"cost_act_or_forecast_per_sku" = 
						CASE
							WHEN "total_sku_count" = 0 THEN 0
							ELSE COALESCE(NULLIF("total_sku_count", 0), 1) * "cost_act_forecast" / NULLIF("total_sku_count", 0) * 100
					END,

  -- df = df.with_columns(Original_BudgetCostofGoods = pl.col('BudgetCostofGoods'))

					"Original_BudgetCostofGoods" = "BudgetCostofGoods",

  --	   df = df.with_columns(sales_actual = pl.col('sales_actual').cast(pl.Int64))

  --       df = df.with_columns((((pl.col('sales_actual') - pl.col('cost_actuals'))/pl.col('sales_actual')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('margin_actuals'))
					margin_actuals =
						CASE
							WHEN "sales_actual" = 0 THEN 0
							ELSE COALESCE(NULLIF("sales_actual", 0), 1) * ("sales_actual"-"cost_actuals") / NULLIF("sales_actual", 0)
						END,
 
  --       df = df.with_columns(((pl.col('budget_gross_margin')/pl.col('budget_amount'))*100).alias('budget_margin_percent'))
					budget_margin_percent = 
						CASE
							WHEN "budget_amount" = 0 THEN 0
							ELSE COALESCE(NULLIF("budget_amount", 0), 1) * ("budget_amount"-"budget_cost")/NULLIF("budget_amount", 0) * 100
						END,
  --       df = df.with_columns(((pl.col('budget_gross_margin')+pl.col('margin_actuals')).replace({-np.inf:0, np.inf:0}).fill_nan(0)).alias('margin_act_forecast'))
					margin_act_forecast = "budget_gross_margin" + "margin_actuals",
  
--********************************************************************************************					
  --       df = df.with_columns(((pl.col('budget_gross_margin')*100)/(pl.col('budget_gross_margin').sum())).alias('budget_margin_mix_percent'))
					-- "budget_margin_mix_percent" =
--********************************************************************************************
  --       df = df.with_columns((((pl.col('budget_gross_margin')/pl.col('margin_act_forecast'))*100)).alias('budget_vs_act_forecast_margin_percent'))
					"budget_vs_act_forecast_margin_percent" = 
						CASE 
							WHEN "margin_act_forecast" = 0  THEN 0
							ELSE COALESCE(NULLIF("margin_act_forecast", 0), 1) * ("budget_gross_margin")/NULLIF("margin_act_forecast", 0) * 100
						END,

 --***************************************************************************************************************************************************************************** 
 --       df = df.with_columns(((pl.col('budget_gross_margin')/pl.col('total_sku_count')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('margin_budget_per_sku'))

 --***********************************************************************************************************************************************************************

-- ??????????????????????????????????????				
  --       df = df.with_columns((pl.lit(0.0)).alias("coefficient_score"))
        
  --       df = df.with_columns(coefficient_score_mix_percent = (pl.col('coefficient_score')/pl.col('coefficient_score').sum()).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100)

  --       df = df.with_columns(((pl.col('sales_actual').cast(pl.Int64))))

--??????????????????????????????????????
  --       df = df.with_columns((((pl.col('budget_amount').cast(pl.Float64)+pl.col('sales_actual')).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0).cast(pl.Float64))).alias('ACT_FCT'))
-- 					

-- "ACT_FCT" = budget_amount + sales_actual,

					"ACT_FCT" = 
						CASE 
							WHEN "Budget_date"::date BETWEEN FORECAST_DATE_FROM AND FORECAST_DATE_TO 
							THEN budget_amount + sales_actual 
							ELSE sales_actual 
						END,

					
  --       df = df.with_columns(((((pl.col('budget_amount'))/(pl.col('ACT_FCT'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)) *100).alias('act_forecast_vs_budget_percent'))
					act_forecast_vs_budget_percent = 
						CASE
							WHEN "ACT_FCT" = 0 THEN 0
							ELSE COALESCE(NULLIF("ACT_FCT", 0), 1) * "budget_amount" / "ACT_FCT" * 100
						END;

  --******************************AGGS_---------------8************************************
  -- 		df = df.with_columns((((pl.col('sold_qty_ly')*100)/(pl.col('sold_qty_ly').sum())).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('quantity_mix_percent'))
		
 -- 	   df = df.with_columns(((pl.col('budget_cost')*100)/(pl.col('budget_cost').sum())).alias('budget_cost_percent'))

--      df = df.with_columns(((pl.col('cost_of_goods_ly')*100)/(pl.col('cost_of_goods_ly').sum()).replace({-np.inf:0, np.inf:0}).fill_nan(0)).alias('cost_mix_percent'))
-- ********************************************AGGS---------------------

	

	progress := 11;
	INSERT INTO public.progress_status(status, progress) VALUES ('get_sp_otb_itemwise_calculations agg div formulas'::text, progress);

	
	WITH aggregate_values AS (
    	SELECT
        	SUM(sold_qty_ly) AS total_sold_qty_ly,
			SUM("budget_cost") AS total_budget_cost,
			SUM("cost_of_goods_ly") AS total_cost_of_goods_ly,
			SUM(total_sku_count) AS sum_of_unique_sku_count,
			SUM("budget_gross_margin") AS total_budget_gross_margin,
			SUM(total_sku_count_ly) AS sum_of_unique_sku_count_ly,
			SUM(total_sku_count_lly) AS sum_of_unique_sku_count_lly
		

    	FROM item_counter
		)
		UPDATE item_counter AS i
			SET 
			"unit_buy_by_sku_total" = 
				CASE
					WHEN s."sum_of_unique_sku_count" = 0 THEN 0
					ELSE "budget_amount"/ COALESCE(NULLIF(s."sum_of_unique_sku_count", 0), 1)
				END,
        -- df = df.with_columns(((pl.col('budget_qty_lly')/pl.col('total_sku_count_lly')).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('unit_buy_by_sku_lly'))
			"unit_buy_by_sku_ly" = 
				CASE
					WHEN s."sum_of_unique_sku_count_ly" = 0 THEN 0
					ELSE "budget_qty_ly"/ NULLIF(s."sum_of_unique_sku_count_ly", 0)
				END,
			
			"unit_buy_by_sku_lly" = 
				CASE
					WHEN s."sum_of_unique_sku_count_lly" = 0 THEN 0
					ELSE "budget_qty_lly"/ NULLIF(s."sum_of_unique_sku_count_lly", 0)
				END,
			"units_per_sku_total" = 
				CASE
					WHEN s."sum_of_unique_sku_count" = 0 THEN 0
					ELSE "budget_qty"/ NULLIF(s."sum_of_unique_sku_count", 0)
				END,
		
			"units_per_sku_total_ly" = 
				CASE
					WHEN s."sum_of_unique_sku_count_ly" = 0 THEN 0
					ELSE "net_sales_ly"/ NULLIF(s."sum_of_unique_sku_count_ly", 0)
				END,
			
			"units_per_sku_total_lly" = 
				CASE
					WHEN s."sum_of_unique_sku_count_lly" = 0 THEN 0
					ELSE "net_sales_lly"/ NULLIF(s."sum_of_unique_sku_count_lly", 0)
				END,
				
		  --       df = df.with_columns(((pl.col('budget_gross_margin')*100)/(pl.col('budget_gross_margin').sum())).alias('budget_margin_mix_percent'))
			
		-- df = df.with_columns(((pl.col('budget_amount') /pl.col('total_sku_count')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('budget_per_sku'))
			budget_per_sku = 
				CASE 
					WHEN s.sum_of_unique_sku_count = 0 THEN 0
					ELSE budget_amount/COALESCE(NULLIF(s.sum_of_unique_sku_count, 0), 1)
				END,
		
			budget_margin_mix_percent = 
				CASE
					WHEN s.total_budget_gross_margin = 0 THEN 0
					ELSE i."sold_qty_ly" * 100/ NULLIF(s."total_budget_gross_margin", 0)
				END,
--       df = df.with_columns(((pl.col('budget_gross_margin')/pl.col('total_sku_count')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('margin_budget_per_sku'))
			margin_budget_per_sku =
				CASE
					WHEN s.sum_of_unique_sku_count = 0 THEN 0
					ELSE i."budget_gross_margin" * 100/ NULLIF(s."sum_of_unique_sku_count", 0)
				END,

			"quantity_mix_percent" = 
				CASE
					WHEN s.total_sold_qty_ly = 0 THEN 0
					ELSE i."sold_qty_ly" * 100/ NULLIF(s."total_sold_qty_ly", 0)
				END,
			budget_cost_percent = 
						CASE
							WHEN s.total_budget_cost = 0 THEN 0
							ELSE i."budget_cost" / NULLIF(s.total_budget_cost, 0)
						END,
			"cost_mix_percent" = 
				CASE
					WHEN "cost_of_goods_lly" = 0 THEN 0
						ELSE i."cost_of_goods_ly" * 100 / NULLIF(s.total_cost_of_goods_ly, 0) 
					END
		-- df = df.with_columns((((pl.col('budget_qty'))/pl.col('total_sku_count')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('units_per_sku_total'))

	
			
			FROM aggregate_values as s;
  
	
		END
		
$BODY$;
ALTER PROCEDURE public.get_sp_otb_itemwise_calculations(date, date)
    OWNER TO mohit;

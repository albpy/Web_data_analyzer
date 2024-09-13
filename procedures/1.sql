-- PROCEDURE: public.get_sp_otb_1(date, date, date, date, text[], text[], text[], text[], text[], text[], text[], integer, text[])

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_1(date, date, date, date, text[], text[], text[], text[], text[], text[], text[], integer, text[]);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_1(
	IN history_date_from date,
	IN history_date_to date,
	IN forecast_date_from date,
	IN forecast_date_to date,
	IN sales_channels text[],
	IN product_families text[],
	IN sub_families text[],
	IN suppliers text[],
	IN categories text[],
	IN sub_categories text[],
	IN skus text[],
	IN top_items integer,
	IN store_classes text[])
LANGUAGE 'plpgsql'
AS $BODY$
	DECLARE 
		progress INT := 0;
BEGIN
	TRUNCATE TABLE public.progress_status;
	-- DROP TABLE IF EXISTS ra_item_temp;
	-- DROP TABLE IF EXISTS ra_item_stock_temp;
	-- DROP TABLE IF EXISTS ra_item_stock_kpi_temp;
	-- DROP TABLE IF EXISTS sales_table_temp_;
	-- DROP TABLE IF EXISTS ra_and_stock;
	-- DROP TABLE IF EXISTS ra_and_stock_temp;
	DROP TABLE IF EXISTS item_counter;
	DROP TABLE IF EXISTS temp_param_values;
	DROP TABLE IF EXISTS sales_for_budget;
	DROP TABLE IF EXISTS ra_and_sales_temp;
	DROP TABLE IF EXISTS ra_sales_item_joined;
	DROP TABLE IF EXISTS ra_sales_item_stock_joined;
	DROP TABLE IF EXISTS ra_sales_item_stock_kpi_joined;

	progress := 1 ;
	INSERT INTO public.progress_status(status, progress) VALUES ('Stored procedure get_sp_otb was invoked'::text, progress);
  	RAISE NOTICE 'Stored procedure my_procedure was invoked';
	
	-- Print data types and values of input parameters
	-- Create a temporary table to store parameter values and data types
    CREATE TEMPORARY TABLE temp_param_values (
        param_name TEXT,
        param_value TEXT,
        param_type TEXT
    );

    -- Insert parameter values and data types into the temporary table
    INSERT INTO temp_param_values (param_name, param_value, param_type)
    VALUES
        ('history_date_from', CAST(history_date_from AS TEXT), pg_typeof(history_date_from)),
        ('history_date_to', CAST(history_date_to AS TEXT), pg_typeof(history_date_to)),
        ('forecast_date_from', CAST(forecast_date_from AS TEXT), pg_typeof(forecast_date_from)),
        ('forecast_date_to', CAST(forecast_date_to AS TEXT), pg_typeof(forecast_date_to)),
        ('sales_channels', ARRAY_TO_STRING(sales_channels, ','), pg_typeof(sales_channels)),
        ('product_families', ARRAY_TO_STRING(product_families, ','), pg_typeof(product_families)),
        ('sub_families', ARRAY_TO_STRING(sub_families, ','), pg_typeof(sub_families)),
        ('suppliers', ARRAY_TO_STRING(suppliers, ','), pg_typeof(suppliers)),
        ('categories', ARRAY_TO_STRING(categories, ','), pg_typeof(categories)),
        ('sub_categories', ARRAY_TO_STRING(sub_categories, ','), pg_typeof(sub_categories)),
        ('skus', ARRAY_TO_STRING(skus, ','), pg_typeof(skus)),
        ('top_items', (top_items, ','), pg_typeof(top_items)),
        ('store_classes', ARRAY_TO_STRING(store_classes, ','), pg_typeof(store_classes));
	progress := 2;
	INSERT INTO public.progress_status(status, progress) VALUES ('ra_item_temp temporary table creation'::text, progress);
    -- Print data types and values of input parameters
--     RAISE NOTICE 'history_date_from: %, DataType: %', history_date_from, pg_typeof(history_date_from);
--     RAISE NOTICE 'history_date_to: %, DataType: %', history_date_to, pg_typeof(history_date_to);
--     RAISE NOTICE 'forecast_date_from: %, DataType: %', forecast_date_from, pg_typeof(forecast_date_from);
--     RAISE NOTICE 'forecast_date_to: %, DataType: %', forecast_date_to, pg_typeof(forecast_date_to);
--     RAISE NOTICE 'sales_channels: %, DataType: %', sales_channels, pg_typeof(sales_channels);
--     RAISE NOTICE 'product_families: %, DataType: %', product_families, pg_typeof(product_families);
--     RAISE NOTICE 'sub_families: %, DataType: %', sub_families, pg_typeof(sub_families);
--     RAISE NOTICE 'suppliers: %, DataType: %', suppliers, pg_typeof(suppliers);
--     RAISE NOTICE 'categories: %, DataType: %', categories, pg_typeof(categories);
--     RAISE NOTICE 'sub_categories: %, DataType: %', sub_categories, pg_typeof(sub_categories);
--     RAISE NOTICE 'skus: %, DataType: %', skus, pg_typeof(skus);
--     RAISE NOTICE 'top_items: %, DataType: %', top_items, pg_typeof(top_items);
--     RAISE NOTICE 'store_classes: %, DataType: %', store_classes, pg_typeof(store_classes);
		-- CREATE INDEX idx_ra_table_itemid_invntlocid_channel ON "ra_table" USING btree("ITEMID", "INVENTLOCATIONID", "Channel");

		progress := 6;
		INSERT INTO public.progress_status(status, progress) VALUES ('Create temp table ra_and_stock_temp joins sales table'::text, progress);

--***********************		
		CREATE TABLE "sales_for_budget" AS
			SELECT iq.channel, iq.store_code, iq.item_code, 
						SUM("LINEAMOUNT") AS "LINEAMOUNT", 
						SUM("LINEDISC") AS "LINEDISC",
						SUM("SALESQTY") AS "SALESQTY",
						SUM("COSTPRICE") AS "COSTPRICE",
						SUM(gross_sales) AS gross_sales,
						MAX("INVOICEDATE") AS "INVOICEDATE",
						MAX(historical_year) AS historical_year,
						MAX(history_quarter) AS history_quarter,
						MAX(history_month) AS history_month,
						MAX(history_week) AS history_week,
						MAX(history_day) AS history_day
						
	FROM
	(
		SELECT stt.channel AS channel, stt."INVENTLOCATIONID" AS store_code, 
	           stt."ITEMID" AS item_code, stt."LINEAMOUNT" AS "LINEAMOUNT", 
				stt."LINEDISC" AS "LINEDISC", stt."SALESQTY" AS "SALESQTY", 
				stt."COSTPRICE" AS "COSTPRICE", 
				stt."LINEAMOUNT" + stt."LINEDISC" as gross_sales,
				date_part('Year'::text, stt."INVOICEDATE"::date)::integer AS historical_year,
				stt."INVOICEDATE" AS "INVOICEDATE",
			EXTRACT(quarter FROM stt."INVOICEDATE"::date)::character varying AS history_quarter,
			to_char(stt."INVOICEDATE"::date::timestamp with time zone, 'Month'::text)::character varying AS history_month,
			EXTRACT(week FROM stt."INVOICEDATE"::date)::character varying AS history_week,
			to_char(stt."INVOICEDATE"::date::timestamp with time zone, 'Day'::text)::character varying AS history_day
				
		  FROM salesdata_trnx_cleaned stt 
		 WHERE stt.channel || stt."INVENTLOCATIONID" || stt."ITEMID" IN (
				SELECT o."Channel" || o."INVENTLOCATIONID" || o."ITEMID" 
				  FROM ra_table o 
						where "Budget_date"::date >=  '2024-07-01' AND  
							"Budget_date"::date <= '2024-12-31'
		)
			   		   AND date("INVOICEDATE") >= history_date_from and date("INVOICEDATE") <= history_date_to
		) AS iq 
		GROUP BY iq.channel, iq.store_code, iq.item_code;

	CREATE INDEX idx_itemid_invntlocid_ch ON "sales_for_budget" USING btree("item_code", "store_code", channel);
	
	-- CREATE INDEX idx_ra_table_budget_date ON "ra_table" USING btree("Budget_date");
--****************				
	    CREATE  TABLE ra_and_sales_temp
		AS
		SELECT 
			   ra_it_st_kpi."Store",
			   ra_it_st_kpi."area",
			   ra_it_st_kpi."Region",
			   ra_it_st_kpi."adjusted_budget_gross_margin_percent",
			   ra_it_st_kpi."budget_amount",
			   ra_it_st_kpi."budget_cost",
			   ra_it_st_kpi."budget_qty",
			   ra_it_st_kpi."Channel",
			   ra_it_st_kpi."INVENTLOCATIONID",
			   ra_it_st_kpi."Budget_date",
			   stt."item_code" AS "ITEMID",
			   stt."LINEAMOUNT",
			   stt."LINEDISC",
			   stt."SALESQTY",
			   stt."COSTPRICE",
			   stt."gross_sales",
			   stt."historical_year",
			   stt."INVOICEDATE",
			   stt."history_quarter",
			   stt."history_month",
			   stt."history_week",
			   stt."history_day"
			   
					FROM "sales_for_budget" as "stt" LEFT JOIN (SELECT 
							"Channel", "ITEMID", 
							"INVENTLOCATIONID",		
						
							MAX("Store") AS "Store",
							MAX("area") AS "area",
							MAX("Region") AS "Region",
							
							SUM("adjusted_budget_gross_margin_percent") AS "adjusted_budget_gross_margin_percent",
							SUM("budget_amount") AS "budget_amount",
							SUM("budget_cost") AS "budget_cost",
							SUM("budget_qty") AS "budget_qty",
							-- MAX("Channel") AS "Channel",
							-- MAX("INVENTLOCATIONID") AS "INVENTLOCATIONID",
							MAX("Budget_date") AS "Budget_date"
						FROM ra_table where "Budget_date"::date between forecast_date_from and forecast_date_to
						group by "Channel", "ITEMID", "INVENTLOCATIONID") as ra_it_st_kpi 
						
								
						ON stt."item_code" = ra_it_st_kpi."ITEMID" 
						AND  stt."channel" = ra_it_st_kpi."Channel" 
						AND  ra_it_st_kpi."INVENTLOCATIONID"  = stt."store_code";--WHERE;

		
	progress := 7;
	INSERT INTO public.progress_status(status, progress) VALUES ('starting apply formulas'::text, progress);

--*************************************
	CREATE TABLE ra_sales_item_joined
		AS SELECT * FROM ra_and_sales_temp rs
			JOIN item_master im
				ON rs."ITEMID" = im."item_lookup_code";

--*************************************
	CREATE TABLE ra_sales_item_stock_joined
		AS SELECT rsi.*,  -- All columns from ra_sales_item_joined, you can also explicitly list them to avoid conflicts
			    st."ITEMID" AS st_item_id,  -- Alias to avoid conflicts
			    st."INVENTLOCATIONID" AS st_invent_id,
			    st."stock_on_hand_qty",
			    st."opening_stock",
			    st."closing_stock",
			    st."current_stock_cost_at_retail",
			    st."stock_received_qty",
			    st."opening_stock_at_cost",
			    st."adjustment_at_cost",
			    st."adjustment_at_retail",
			    st."stock_date" FROM ra_sales_item_joined rsi
			LEFT JOIN (
			SELECT 
				"ItemLookupCode" AS "ITEMID" ,
				"StoreID" AS "INVENTLOCATIONID",
				SUM("Quantity") AS "stock_on_hand_qty",
				SUM("AvailableQTY") AS "opening_stock",
				SUM("AvailableQTY") AS "closing_stock",
				AVG("Price") AS "current_stock_cost_at_retail",
				0 AS "stock_received_qty",
				SUM("Cost") AS "opening_stock_at_cost",
				0 AS "adjustment_at_cost",
				0 AS "adjustment_at_retail",
				MAX("SnapShotTime") AS "stock_date"
			 FROM stock 
			  GROUP BY "ItemLookupCode","StoreID") AS st
				ON rsi."ITEMID" = st."ITEMID" AND rsi."INVENTLOCATIONID" = st."INVENTLOCATIONID"; 
	
--*********************************
CREATE TABLE ra_sales_item_stock_kpi_joined
		AS SELECT rsis.*,
				   kpi."article_score_sale",
				   kpi."article_score_abc",
				   kpi."article_score_ae",
				   kpi."article_score_speed",
				   kpi."article_score_terminal",
				   kpi."article_score_margin",
				   kpi."article_score_sell",
				   kpi."article_score_markdown",
				   kpi."article_score_core",
				   kpi."article_score_quartile",
				   kpi."article_score_sortimeter",
				   kpi."btech_vs_sortimeter",
				   kpi."store_sku_count",
				   kpi."price"
				FROM ra_sales_item_stock_joined rsis
	  		LEFT JOIN kpi_table kpi
				 ON  rsis."ITEMID" = kpi."ITEMID" ;
-----------------------------Drop NULLS-----------------------
CREATE TABLE temp_for_otb AS
SELECT * FROM ra_sales_item_stock_kpi_joined WHERE "Channel" IS NOT NULL;

DROP TABLE ra_sales_item_stock_kpi_joined;

ALTER TABLE temp_for_otb RENAME TO ra_sales_item_stock_kpi_joined;
---------------------------------------------------------------------------
	-- Add a new column named quantity_actuals to the ra_and_stock_temp table
	ALTER TABLE ra_sales_item_stock_kpi_joined 
		ADD COLUMN quantity_actuals NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN sold_qty_ly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN quantity_ppy NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN sales_actual NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN net_sales_ly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN net_sales_lly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN cost_actuals NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN cost_of_goods_ly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN cost_of_goods_lly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN gross_sales_ty NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN gross_sales_ly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN gross_sales_lly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN "ITEMID_ty" TEXT,
		ADD COLUMN "ITEMID_ly" TEXT,
		ADD COLUMN "ITEMID_lly" TEXT,
		ADD COLUMN budget_qty_ty NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN budget_qty_ly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN budget_qty_lly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN current_stock_cost NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN stock_cost_ly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN net_sales_mix_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN final_price NUMERIC(20, 4) DEFAULT 0,
 		ADD COLUMN initial_average_retail_price NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN initial_average_retail_price_ty NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN initial_average_retail_price_ly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN initial_average_retail_price_lly NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN purchase_value NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN  sales_act_forecast NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN purchase_value_mix_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN budget_percent NUMERIC(20, 8) DEFAULT 0,
		ADD COLUMN relative_budget_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN budget_gross_margin_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN ly_gross_margin NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN ly_customer_disc NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN markdown_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN supply_retail_value NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN retail_value_including_markdown NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN "MarkdownValue" NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN "StockatCostPrice" NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN "StockatRetailPrice" NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN "PurchaseRetailValueatGrossSale" NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN adjusted_markdown_percent NUMERIC(20, 4) DEFAULT 0,
		ALTER COLUMN budget_amount TYPE NUMERIC(20, 8) USING budget_amount::NUMERIC(20, 8);
	-- Update the quantity_actuals column based on the conditions
	UPDATE ra_sales_item_stock_kpi_joined

-- df = df.with_columns(PurchaseRetailValueatGrossSale = pl.col('retail_value_including_markdown')/(pl.col('proposed_sellthru_percent')/100))

		SET quantity_actuals = 
			(CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE) THEN "SALESQTY"
				ELSE 0
			END),
		sold_qty_ly = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 1 THEN "SALESQTY"
				ELSE 0
			END,
		 quantity_ppy = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 2 THEN "SALESQTY"
				ELSE 0
			END,

			
			sales_actual =   
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)
				THEN "LINEAMOUNT"
				ELSE 0
			END,
			
			
		 net_sales_ly = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 1 THEN "LINEAMOUNT"
				ELSE 0
			END,

		 net_sales_lly = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 2 THEN "LINEAMOUNT"
				ELSE 0
			END,

		 cost_actuals = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE) THEN "COSTPRICE"
				ELSE 0
			END,

		 cost_of_goods_ly = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 1 THEN "COSTPRICE"
				ELSE 0
			END,

		 cost_of_goods_lly = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 2 THEN "COSTPRICE"
				ELSE 0
			END,

		 gross_sales_ty = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE) THEN "gross_sales"
				ELSE 0
			END,

		 gross_sales_ly = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 1 THEN "gross_sales"
				ELSE 0
			END,

		 gross_sales_lly = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 2 THEN "gross_sales"
				ELSE 0
			END,

		 "ITEMID_ty" = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE) THEN "ITEMID"
				ELSE '0'
			END,

		 "ITEMID_ly" = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 1 THEN "ITEMID"
				ELSE '0'
			END,

		 "ITEMID_lly" = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE)- 2 THEN "ITEMID"
				ELSE '0'
			END,

	      stock_cost_ly = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE) -1 THEN "current_stock_cost"
				ELSE 0
			END,
		 ly_customer_disc = 
			CASE
				WHEN historical_year = EXTRACT(YEAR FROM CURRENT_DATE) -1 THEN "LINEDISC"
				ELSE 0
			END,

		
		markdown_percent = 
			CASE 
				WHEN (ly_customer_disc + net_sales_ly) = 0 THEN 0
				ELSE ly_customer_disc/(ly_customer_disc + net_sales_ly) * 100
			END,
		

		-- data = data.with_columns(supply_retail_value = pl.col('SupplyCost')/(1-pl.col('FirstMargin_percent')/100))
		-- consider 5 as Logistics%
		"supply_retail_value" = (budget_cost - (COALESCE(budget_cost,0) * (5 / 100))) / (1 - ((budget_amount -  (budget_cost -(COALESCE(budget_cost,0) * (5 / 100))))/ COALESCE(NULLIF(budget_amount,0),1))),
		
		 "MarkdownValue" = (supply_retail_value) * markdown_percent,
		
		 ly_gross_margin = ((net_sales_ly - cost_of_goods_ly)/COALESCE(NULLIF(net_sales_ly, 0),1)) * 100,
		
		 final_price = net_sales_ly/COALESCE(NULLIF(sold_qty_ly, 0), 1),

		 initial_average_retail_price = case when budget_qty =0 then 0 else budget_amount / budget_qty end,
			
		 initial_average_retail_price_ty = gross_sales_ty / COALESCE(NULLIF(quantity_actuals, 0), 1),
	
		 initial_average_retail_price_ly = gross_sales_ly / COALESCE(NULLIF(sold_qty_ly, 0), 1),
	
		 initial_average_retail_price_lly = gross_sales_lly / COALESCE(NULLIF(quantity_ppy, 0), 1),
	
		 purchase_value = ((opening_stock + stock_received_qty) * initial_average_retail_price),
	
		 sales_act_forecast = sales_actual,
	
		 relative_budget_percent = budget_percent,

		 

		 -- budget_gross_margin_percent  = ((budget_amount-budget_cost)/COALESCE(NULLIF(budget_amount,0),1)) * 100;
		budget_gross_margin_percent = 
			CASE 
				 WHEN budget_amount = 0 THEN 0 
    			ELSE ((budget_amount - budget_cost) / budget_amount) * 100 
			END;

 -- 	progress := 8;
	-- INSERT INTO public.progress_status(status, progress) VALUES ('main procedure aggregate division'::text, progress);
	 
	 

	WITH aggregate_values AS (
    SELECT
        SUM(net_sales_ly) AS total_net_sales_ly,
        SUM(purchase_value) AS total_purchase_value,
        SUM(budget_amount) AS total_budget_amount_value
    FROM ra_sales_item_stock_kpi_joined
)

	UPDATE ra_sales_item_stock_kpi_joined AS r
		SET net_sales_mix_percent = (r.net_sales_ly / COALESCE(NULLIF(s.total_net_sales_ly, 0), 1)) * 100,
    		purchase_value_mix_percent = (r.purchase_value / COALESCE(NULLIF(s.total_purchase_value, 0), 1)) * 100,
    		budget_percent = (r.budget_amount / COALESCE(NULLIF(s.total_budget_amount_value, 0), 1)) * 100
	FROM aggregate_values AS s;

		
	CALL get_sp_otb_root_frame_calc_1();

	CALL get_sp_otb_itemwise_calculations_1(forecast_date_from, forecast_date_to);
END;
$BODY$;
ALTER PROCEDURE public.get_sp_otb_1(date, date, date, date, text[], text[], text[], text[], text[], text[], text[], integer, text[])
    OWNER TO postgres;

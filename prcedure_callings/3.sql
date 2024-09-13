-- PROCEDURE: public.get_sp_otb(date, date, date, date, text[], text[], text[], text[], text[], text[], text[], integer, text[])

-- DROP PROCEDURE IF EXISTS public.get_sp_otb(date, date, date, date, text[], text[], text[], text[], text[], text[], text[], integer, text[]);

CREATE OR REPLACE PROCEDURE public.get_sp_otb(
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
	DROP TABLE IF EXISTS ra_item_temp;
	DROP TABLE IF EXISTS ra_item_stock_temp;
	DROP TABLE IF EXISTS ra_item_stock_kpi_temp;
	DROP TABLE IF EXISTS sales_table_temp_;
	DROP TABLE IF EXISTS ra_and_stock;
	DROP TABLE IF EXISTS ra_and_stock_temp;
	DROP TABLE IF EXISTS item_counter;
	DROP TABLE IF EXISTS temp_param_values;

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

	CREATE TEMP TABLE  ra_item_temp -- ra_trnx_temp
		AS 
		SELECT it."ITEMID",
			   it."Description",
			   it."Department",
			   it."Category",
			   it."Family",
			   it."SubFamily",
			   it."SubCategory",
			   it."Company_ABC",
			   it."Family_ABC",
			   it."ExtendedSubCategory",
			   it."SubCategorySupplier",
			   it."AssemblyCodeNickName",
			   -- it."ENDOFLife",
			   it."DOM_COMM",
			   it."Status",
			   it."Supplier",
			   rt."Store",
			   rt."area",
			   rt."Region",
			   rt."adjusted_budget_gross_margin_percent",
			   rt."budget_amount",
			   rt."budget_cost",
			   rt."budget_qty",
			   rt."Channel",
			   rt."INVENTLOCATIONID",
			   rt."Budget_date"

		FROM (
			SELECT 
				"item_lookup_code" AS "ITEMID",
				 MAX("description") AS "Description",
				 MAX("department") AS "Department",
				 MAX("category_name") AS "Category",
				 MAX("family") AS "Family",
				 MAX("sub_family") AS "SubFamily",
				 MAX("sub_category") AS "SubCategory",
				 MAX("companyabc") AS "Company_ABC",
				 MAX("family_abc") AS "Family_ABC",
				 MAX("extended_sub_category") AS "ExtendedSubCategory",
				 MAX("sub_category_supplier") AS "SubCategorySupplier",
				 MAX("assembly_code_nickname") AS "AssemblyCodeNickName",
				 -- MAX("ENDOFLife") AS "ENDOFLife",
				 MAX("dom_comm") AS "DOM_COMM",
				 -- MAX("status") AS "Status",
				 MAX(CASE WHEN "status" THEN 1 ELSE 0 END) AS "Status",
				 MAX("supplier") AS "Supplier"
				 FROM 
					item_master GROUP BY  "item_lookup_code") AS it
	 	LEFT JOIN
	 	ra_table rt ON it."ITEMID" = rt."ITEMID"
		-- WHERE "Budget_date"::date between '2024-07-01' and '2024-07-02';
		WHERE ("Budget_date"::date BETWEEN forecast_date_from AND forecast_date_to);

		
		
	CREATE INDEX idx_itemid_invntlocid ON ra_item_temp USING btree("ITEMID", "INVENTLOCATIONID");

	progress := 3;
	INSERT INTO public.progress_status(status, progress) VALUES ('Create temp table ra_item_stock_temp joinin stock'::text, progress);
	
	
-- -----------------------------------------------------------------------------------------	
	
	CREATE TABLE ra_item_stock_temp
		AS
		SELECT ra_it."ITEMID",
			   ra_it."Description",
			   ra_it."Department",
			   ra_it."Category",
			   ra_it."Family",
			   ra_it."SubFamily",
			   ra_it."SubCategory",
			   ra_it."ExtendedSubCategory",
			   ra_it."SubCategorySupplier",
			   ra_it."AssemblyCodeNickName",
			   ra_it."Company_ABC",
			   ra_it."Family_ABC",
			   -- ra_it."ENDOFLife",
			   ra_it."DOM_COMM",
			   ra_it."Status",
			   ra_it."Supplier",
			   ra_it."Store",
			   ra_it."area",
			   ra_it."Region",
			   ra_it."adjusted_budget_gross_margin_percent",
			   ra_it."budget_amount",
			   ra_it."budget_cost",
			   ra_it."budget_qty",
			   ra_it."Channel",
			   ra_it."INVENTLOCATIONID",
			   ra_it."Budget_date",
			   st."Quantity" as "stock_on_hand_qty",
			   st."AvailableQTY" as "opening_stock",
			   st."AvailableQTY" as "closing_stock",
			   "Price" AS "current_stock_cost_at_retail",
				0 AS "stock_received_qty",
				"Cost" AS "opening_stock_at_cost",
				0 AS "adjustment_at_cost",
				0 AS "adjustment_at_retail",
				"SnapShotTime" AS "stock_date"
		
		from ra_item_temp ra_it left join stock st on (ra_it."ITEMID" = st."ItemLookupCode") and (ra_it."INVENTLOCATIONID" = st."StoreID");
      

	
	
	progress := 4;
	INSERT INTO public.progress_status(status, progress) VALUES ('Create temp table ra_item_stock_kpi_temp joins kpi'::text, progress);
-- ....................................................................
-- .....................................................................

	
       CREATE TABLE ra_item_stock_kpi_temp
		AS
		SELECT 
			  ra_it_st."ITEMID",
			   ra_it_st."Description",
			   ra_it_st."Department",
			   ra_it_st."Category",
			   ra_it_st."Family",
			   ra_it_st."SubFamily",
			   ra_it_st."SubCategory",
			   ra_it_st."ExtendedSubCategory",
			   ra_it_st."SubCategorySupplier",
			   ra_it_st."AssemblyCodeNickName",
			   -- ra_it_st."ENDOFLife",
			   ra_it_st."DOM_COMM",
			   ra_it_st."Status",
			   ra_it_st."Supplier",
			   ra_it_st."Company_ABC",
			   ra_it_st."Family_ABC",
			   ra_it_st."Store",
			   ra_it_st."area",
			   ra_it_st."Region",
			   ra_it_st."adjusted_budget_gross_margin_percent",
			   ra_it_st."budget_amount",
			   ra_it_st."budget_cost",
			   ra_it_st."budget_qty",
			   ra_it_st."Channel",
			   ra_it_st."INVENTLOCATIONID",
			   ra_it_st."Budget_date",
			   ra_it_st."stock_on_hand_qty",
			   ra_it_st."opening_stock",
			   ra_it_st."closing_stock",
			   ra_it_st."current_stock_cost_at_retail",
			   ra_it_st."stock_received_qty",
			   ra_it_st."opening_stock_at_cost",
			   ra_it_st."adjustment_at_cost",
			   ra_it_st."adjustment_at_retail",
			   ra_it_st."stock_date",
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
			  

		FROM ra_item_stock_temp  ra_it_st
		LEFT JOIN kpi_table kpi on ra_it_st."ITEMID" = kpi."ITEMID" ;
	
	
	
	
	
	progress := 5;
	INSERT INTO public.progress_status(status, progress) VALUES ('Create temp table sales_table_temp_'::text, progress);
	
	CREATE TABLE sales_table_temp_
		TABLESPACE pg_default
		AS
		 SELECT "ITEMID",
			channel,
			"family",
			"INVENTLOCATIONID",
			sum("LINEAMOUNT") AS "LINEAMOUNT",
			sum("LINEDISC") AS "LINEDISC",
			sum("SALESQTY") AS "SALESQTY",
			sum("COSTPRICE") AS "COSTPRICE",
			sum("LINEAMOUNT") + sum("LINEDISC") AS gross_sales,
			-- (sum("LINEDISC") / (sum("LINEAMOUNT") + sum("LINEDISC"))) * 100 AS markdown_percent,
			date_part('Year'::text, max("INVOICEDATE"::date))::integer AS historical_year,
			max("INVOICEDATE") AS "INVOICEDATE",
			EXTRACT(quarter FROM "INVOICEDATE"::date)::character varying AS history_quarter,
			to_char("INVOICEDATE"::date::timestamp with time zone, 'Month'::text)::character varying AS history_month,
			EXTRACT(week FROM "INVOICEDATE"::date)::character varying AS history_week,
			to_char("INVOICEDATE"::date::timestamp with time zone, 'Day'::text)::character varying AS history_day
		    FROM salesdata_trnx
			WHERE "channel" IS NOT NULL AND "family" IS NOT NULL AND "channel" != 'NON'
			-- AND "INVOICEDATE"::date between history_date_from and history_date_to
			AND "INVOICEDATE"::date BETWEEN TO_DATE((EXTRACT(YEAR FROM CURRENT_DATE))::text || '-' || TO_CHAR(history_date_from, 'MM-DD'), 'YYYY-MM-DD')
			AND TO_DATE((EXTRACT(YEAR FROM CURRENT_DATE))::text || '-' || TO_CHAR(history_date_to, 'MM-DD'), 'YYYY-MM-DD') OR
			"INVOICEDATE"::date BETWEEN TO_DATE((EXTRACT(YEAR FROM CURRENT_DATE)-1)::text || '-' || TO_CHAR(history_date_from, 'MM-DD'), 'YYYY-MM-DD')
			AND TO_DATE((EXTRACT(YEAR FROM CURRENT_DATE)-1)::text || '-' || TO_CHAR(history_date_to, 'MM-DD'), 'YYYY-MM-DD') OR
			"INVOICEDATE"::date BETWEEN TO_DATE((EXTRACT(YEAR FROM CURRENT_DATE)-2)::text || '-' || TO_CHAR(history_date_from, 'MM-DD'), 'YYYY-MM-DD')
			AND TO_DATE((EXTRACT(YEAR FROM CURRENT_DATE)-2)::text || '-' || TO_CHAR(history_date_to, 'MM-DD'), 'YYYY-MM-DD')
			AND "channel" IS NOT NULL AND "family" IS NOT NULL
		    GROUP BY "ITEMID", channel, "INVENTLOCATIONID", "INVOICEDATE", "family"

		WITH DATA;

		ALTER TABLE IF EXISTS public.current_sales_trnx
			OWNER TO postgres;
		
		progress := 6;
		INSERT INTO public.progress_status(status, progress) VALUES ('Create temp table ra_and_stock_temp joins sales table'::text, progress);

		CREATE TABLE ra_budget_temp as (SELECT 
								"Channel", "ITEMID", 
								"INVENTLOCATIONID",
							    MAX("Description") AS "Description",
							    MAX("Department") AS "Department",
							    MAX("Category") AS "Category",
							    MAX("Family") AS "Family",
							    MAX("SubFamily") AS "SubFamily",
							    MAX("SubCategory") AS "SubCategory",
							    MAX("ExtendedSubCategory") AS "ExtendedSubCategory",
							    MAX("SubCategorySupplier") AS "SubCategorySupplier",
							    MAX("AssemblyCodeNickName") AS "AssemblyCodeNickName",
							    MAX("DOM_COMM") AS "DOM_COMM",
							    SUM("Status") AS "Status",
							    MAX("Supplier") AS "Supplier",
							    MAX("Company_ABC") AS "Company_ABC",
							    MAX("Family_ABC") AS "Family_ABC",
							    MAX("Store") AS "Store",
							    MAX("area") AS "area",
							    MAX("Region") AS "Region",
							    SUM("adjusted_budget_gross_margin_percent") AS "adjusted_budget_gross_margin_percent",
							    SUM("budget_amount") AS "budget_amount",
							    SUM("budget_cost") AS "budget_cost",
							    SUM("budget_qty") AS "budget_qty",
							    -- MAX("Channel") AS "Channel",
							    -- MAX("INVENTLOCATIONID") AS "INVENTLOCATIONID",
							    MAX("Budget_date") AS "Budget_date",
							    SUM("stock_on_hand_qty") AS "stock_on_hand_qty",
							    SUM("opening_stock") AS "opening_stock",
							    SUM("closing_stock") AS "closing_stock",
							    SUM("current_stock_cost_at_retail") AS "current_stock_cost_at_retail",
							    SUM("stock_received_qty") AS "stock_received_qty",
							    SUM("opening_stock_at_cost") AS "opening_stock_at_cost",
							    SUM("adjustment_at_cost") AS "adjustment_at_cost",
							    SUM("adjustment_at_retail") AS "adjustment_at_retail",
							    MAX("stock_date") AS "stock_date",
							    SUM("article_score_sale") AS "article_score_sale",
							    SUM("article_score_abc") AS "article_score_abc",
							    SUM("article_score_ae") AS "article_score_ae",
							    SUM("article_score_speed") AS "article_score_speed",
							    SUM("article_score_terminal") AS "article_score_terminal",
							    SUM("article_score_margin") AS "article_score_margin",
							    SUM("article_score_sell") AS "article_score_sell",
							    SUM("article_score_markdown") AS "article_score_markdown",
							    SUM("article_score_core") AS "article_score_core",
							    SUM("article_score_quartile") AS "article_score_quartile",
							    SUM("article_score_sortimeter") AS "article_score_sortimeter",
							    SUM("btech_vs_sortimeter") AS "btech_vs_sortimeter",
							    SUM("store_sku_count") AS "store_sku_count",
							    SUM("price") AS "price" 
									from ra_item_stock_kpi_temp group by 
										"Channel", "ITEMID", "INVENTLOCATIONID");

	
        -- IF  array_length(store_classes, 1) IS NOT NULL  THEN
	    CREATE  TABLE ra_and_stock_temp
		AS
	SELECT public.ra_budget_temp."ITEMID",
			   public.ra_budget_temp."Description",
			   public.ra_budget_temp."Department",
			   public.ra_budget_temp."Category",
			   public.ra_budget_temp."Family",
			   public.ra_budget_temp."SubFamily",
			   public.ra_budget_temp."SubCategory",
			   public.ra_budget_temp."ExtendedSubCategory",
			   public.ra_budget_temp."SubCategorySupplier",
			   public.ra_budget_temp."AssemblyCodeNickName",
			   -- o."ENDOFLife",
			   public.ra_budget_temp."DOM_COMM",
			   public.ra_budget_temp."Status",
			   public.ra_budget_temp."Supplier",
			   public.ra_budget_temp."Store",
			   public.ra_budget_temp."area",
			   public.ra_budget_temp."Region",
			   public.ra_budget_temp."Company_ABC",
			   public.ra_budget_temp."Family_ABC",
			   public.ra_budget_temp."adjusted_budget_gross_margin_percent",
			   public.ra_budget_temp."budget_amount",
			   public.ra_budget_temp."budget_cost",
			   public.ra_budget_temp."budget_qty",
			   public.ra_budget_temp."Channel",
			   public.ra_budget_temp."INVENTLOCATIONID",
			   public.ra_budget_temp."Budget_date",
			   public.ra_budget_temp."stock_on_hand_qty",
			   public.ra_budget_temp."opening_stock",
			   public.ra_budget_temp."closing_stock",
			   public.ra_budget_temp."current_stock_cost_at_retail",
			   public.ra_budget_temp."stock_received_qty",
			   public.ra_budget_temp."opening_stock_at_cost",
			   public.ra_budget_temp."adjustment_at_cost",
			   public.ra_budget_temp."adjustment_at_retail",
			   public.ra_budget_temp."stock_date",
			   public.ra_budget_temp."article_score_sale",
			   public.ra_budget_temp."article_score_abc",
			   public.ra_budget_temp."article_score_ae",
			   public.ra_budget_temp."article_score_speed",
			   public.ra_budget_temp."article_score_terminal",
			   public.ra_budget_temp."article_score_margin",
			   public.ra_budget_temp."article_score_sell",
			   public.ra_budget_temp."article_score_markdown",
			   public.ra_budget_temp."article_score_core",
			   public.ra_budget_temp."article_score_quartile",
			   public.ra_budget_temp."article_score_sortimeter",
			   public.ra_budget_temp."btech_vs_sortimeter",
			   public.ra_budget_temp."store_sku_count",
			   public.ra_budget_temp."price",
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
			   
		FROM temp1 stt  JOIN public.ra_budget_temp 
			
		ON stt."item_code" = public.ra_budget_temp."ITEMID" 
			AND  stt."channel" = public.ra_budget_temp."Channel" 
		AND stt."store_code" = public.ra_budget_temp."INVENTLOCATIONID" --AND stt."family" = ra_it_st_kpi."Family" WHERE
		-- WHERE ("Budget_date"::date BETWEEN '2024-07-01' AND '2024-07-02') 
			-- ra_it_st_kpi."budget_amount" IS NOT NULL AND
			(CASE WHEN array_length(sales_channels, 1) IS NOT NULL THEN "Channel" IN (SELECT unnest(sales_channels)) ELSE true END)
            AND (CASE WHEN array_length(product_families, 1) IS NOT NULL THEN "Family" IN (SELECT unnest(product_families)) ELSE true END)
            AND (CASE WHEN array_length(sub_families, 1) IS NOT NULL THEN "SubFamily" IN (SELECT unnest(sub_families)) ELSE true END)
            AND (CASE WHEN array_length(suppliers, 1) IS NOT NULL THEN "Supplier" IN (SELECT unnest(suppliers)) ELSE true END)
            AND (CASE WHEN array_length(categories, 1) IS NOT NULL THEN "ExtendedSubCategory" IN (SELECT unnest(categories)) ELSE true END)
            AND (CASE WHEN array_length(sub_categories, 1) IS NOT NULL THEN "SubCategory" IN (SELECT unnest(sub_categories)) ELSE true END)
            AND (CASE WHEN array_length(skus, 1) IS NOT NULL THEN ra_it_st_kpi."ITEMID" IN (SELECT unnest(skus)) ELSE true END);
			
		-- 	AND (CASE WHEN array_length(product_families, 1) IS NOT NULL OR array_length(sub_families, 1) IS NOT NULL 
		-- 		 OR array_length(suppliers, 1) IS NOT NULL OR array_length(categories, 1) IS NOT NULL OR 
		-- 		 array_length(sub_categories, 1) IS NOT NULL OR array_length(skus, 1) IS NOT NULL
		-- 		 THEN "Family_ABC" IN (SELECT unnest(store_classes)) ELSE "Company_ABC" IN (SELECT unnest(store_classes) )END)
		-- 	 ORDER BY 
		-- 		  CASE 
		-- 			  WHEN top_items IS NOT NULL THEN stt."LINEAMOUNT" 
		-- 		  ELSE 1 
		--   END DESC
		-- LIMIT 
		--   CASE 
		-- 	WHEN top_items IS NOT NULL THEN top_items 
		-- 	ELSE (SELECT COUNT(*) FROM ra_item_stock_kpi_temp) 
		  -- END;

	
	

	
	-- ELSE
	-- CREATE  TABLE ra_and_stock_temp
	-- 	AS
	-- 	SELECT ra_it_st_kpi."ITEMID",
	-- 		   ra_it_st_kpi."Description",
	-- 		   ra_it_st_kpi."Department",
	-- 		   ra_it_st_kpi."Category",
	-- 		   ra_it_st_kpi."Family",
	-- 		   ra_it_st_kpi."SubFamily",
	-- 		   ra_it_st_kpi."SubCategory",
	-- 		   ra_it_st_kpi."ExtendedSubCategory",
	-- 		   ra_it_st_kpi."SubCategorySupplier",
	-- 		   ra_it_st_kpi."AssemblyCodeNickName",
	-- 		   -- ra_it_st_kpi."ENDOFLife",
	-- 		   ra_it_st_kpi."DOM_COMM",
	-- 		   ra_it_st_kpi."Status",
	-- 		   ra_it_st_kpi."Supplier",
	-- 		   ra_it_st_kpi."Store",
	-- 		   ra_it_st_kpi."area",
	-- 		   ra_it_st_kpi."Region",
	-- 		   ra_it_st_kpi."Company_ABC",
	-- 		   ra_it_st_kpi."Family_ABC",
	-- 		   ra_it_st_kpi."adjusted_budget_gross_margin_percent",
	-- 		   ra_it_st_kpi."budget_amount",
	-- 		   ra_it_st_kpi."budget_cost",
	-- 		   ra_it_st_kpi."budget_qty",
	-- 		   ra_it_st_kpi."Channel",
	-- 		   ra_it_st_kpi."INVENTLOCATIONID",
	-- 		   ra_it_st_kpi."Budget_date",
	-- 		   ra_it_st_kpi."stock_on_hand_qty",
	-- 		   ra_it_st_kpi."opening_stock",
	-- 		   ra_it_st_kpi."closing_stock",
	-- 		   ra_it_st_kpi."current_stock_cost_at_retail",
	-- 		   ra_it_st_kpi."stock_received_qty",
	-- 		   ra_it_st_kpi."opening_stock_at_cost",
	-- 		   ra_it_st_kpi."adjustment_at_cost",
	-- 		   ra_it_st_kpi."adjustment_at_retail",
	-- 		   ra_it_st_kpi."stock_date",
	-- 		   ra_it_st_kpi."article_score_sale",
	-- 		   ra_it_st_kpi."article_score_abc",
	-- 		   ra_it_st_kpi."article_score_ae",
	-- 		   ra_it_st_kpi."article_score_speed",
	-- 		   ra_it_st_kpi."article_score_terminal",
	-- 		   ra_it_st_kpi."article_score_margin",
	-- 		   ra_it_st_kpi."article_score_sell",
	-- 		   ra_it_st_kpi."article_score_markdown",
	-- 		   ra_it_st_kpi."article_score_core",
	-- 		   ra_it_st_kpi."article_score_quartile",
	-- 		   ra_it_st_kpi."article_score_sortimeter",
	-- 		   ra_it_st_kpi."btech_vs_sortimeter",
	-- 		   ra_it_st_kpi."store_sku_count",
	-- 		   ra_it_st_kpi."price",
	-- 		   stt."LINEAMOUNT",
	-- 		   stt."LINEDISC",
	-- 		   stt."SALESQTY",
	-- 		   stt."COSTPRICE",
	-- 		   stt."gross_sales",
	-- 		   stt."historical_year",
	-- 		   stt."INVOICEDATE",
	-- 		   stt."history_quarter",
	-- 		   stt."history_month",
	-- 		   stt."history_week",
	-- 		   stt."history_day"
			   
	-- 	FROM ra_item_stock_kpi_temp ra_it_st_kpi LEFT JOIN sales_table_temp_ stt
	-- 	ON stt."ITEMID" = ra_it_st_kpi."ITEMID" 
	-- 	AND  stt."channel" = ra_it_st_kpi."Channel" 
	-- 	AND stt."INVENTLOCATIONID" = ra_it_st_kpi."INVENTLOCATIONID"
	-- 		WHERE ("Budget_date"::date BETWEEN forecast_date_from AND forecast_date_to) 
	-- 			AND (CASE WHEN array_length(sales_channels, 1) IS NOT NULL THEN "Channel" IN (SELECT unnest(sales_channels)) ELSE true END)
	--             AND (CASE WHEN array_length(product_families, 1) IS NOT NULL THEN "Family" IN (SELECT unnest(product_families)) ELSE true END)
	--             AND (CASE WHEN array_length(sub_families, 1) IS NOT NULL THEN "SubFamily" IN (SELECT unnest(sub_families)) ELSE true END)
	--             AND (CASE WHEN array_length(suppliers, 1) IS NOT NULL THEN "Supplier" IN (SELECT unnest(suppliers)) ELSE true END)
	--             AND (CASE WHEN array_length(categories, 1) IS NOT NULL THEN "ExtendedSubCategory" IN (SELECT unnest(categories)) ELSE true END)
	--             AND (CASE WHEN array_length(sub_categories, 1) IS NOT NULL THEN "SubCategory" IN (SELECT unnest(sub_categories)) ELSE true END)
	--             AND (CASE WHEN array_length(skus, 1) IS NOT NULL THEN ra_it_st_kpi."ITEMID" IN (SELECT unnest(skus)) ELSE true END)
	-- 		-- 	ORDER BY 
	-- 		-- 		  CASE 
	-- 		-- 			  WHEN top_items IS NOT NULL THEN stt."LINEAMOUNT" 
	-- 		-- 		  ELSE 1 
	-- 		--   END DESC
	-- 		-- LIMIT 
	-- 		--   CASE 
	-- 		-- 	WHEN top_items IS NOT NULL THEN top_items 
	-- 		-- 	ELSE (SELECT COUNT(*) FROM ra_item_stock_kpi_temp) 
	-- 		  -- END;
	-- 		 END IF;
	 
	 progress := 7;
	INSERT INTO public.progress_status(status, progress) VALUES ('starting apply formulas'::text, progress);
	 
	 
-- 	UPDATE ra_and_stock_temp SET "LINEAMOUNT" =0 WHERE "LINEAMOUNT" IS NULL; 
	 
	 
-- IF top_items IS NOT NULL THEN
--     CREATE  TABLE ra_and_stock AS
--     SELECT * FROM ra_and_stock_temp
--     LIMIT top_items;

-- 	ELSE
-- 	    CREATE  TABLE ra_and_stock AS
--         SELECT * FROM ra_and_stock_temp; 	
-- END IF;
---------------------------------------------------------------------------
	-- Add a new column named quantity_actuals to the ra_and_stock_temp table
	ALTER TABLE ra_and_stock_temp 
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
		ADD COLUMN budget_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN relative_budget_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN budget_gross_margin_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN ly_gross_margin NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN ly_customer_disc NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN markdown_percent NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN supply_retail_value NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN "MarkdownValue" NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN "StockatCostPrice" NUMERIC(20, 4) DEFAULT 0,
		ADD COLUMN "StockatRetailPrice" NUMERIC(20, 4) DEFAULT 0;

	-- Update the quantity_actuals column based on the conditions
	UPDATE ra_and_stock_temp
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
		
		 -- "stock_cost_ly" = 
		
		 "MarkdownValue" = (supply_retail_value) * markdown_percent,
		
		 ly_gross_margin = ((net_sales_ly - cost_of_goods_ly)/COALESCE(NULLIF(net_sales_ly, 0),1)) * 100,
		
		 final_price = net_sales_ly/COALESCE(NULLIF(sold_qty_ly, 0), 1),

		 initial_average_retail_price = gross_sales / COALESCE(NULLIF(budget_qty, 0), 1),
			
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

 	progress := 8;
	INSERT INTO public.progress_status(status, progress) VALUES ('main procedure aggregate division'::text, progress);
	 
	 

	WITH aggregate_values AS (
    SELECT
        SUM(net_sales_ly) AS total_net_sales_ly,
        SUM(purchase_value) AS total_purchase_value,
        SUM(budget_amount) AS total_budget_amount_value
    FROM ra_and_stock_temp
)

	UPDATE ra_and_stock_temp AS r
		SET net_sales_mix_percent = (r.net_sales_ly / COALESCE(NULLIF(s.total_net_sales_ly, 0), 1)) * 100,
    		purchase_value_mix_percent = (r.purchase_value / COALESCE(NULLIF(s.total_purchase_value, 0), 1)) * 100,
    		budget_percent = (r.budget_amount / COALESCE(NULLIF(s.total_budget_amount_value, 0), 1)) * 100
	FROM aggregate_values AS s;

		
	CALL get_sp_otb_root_frame_calc();
END;
$BODY$;
ALTER PROCEDURE public.get_sp_otb(date, date, date, date, text[], text[], text[], text[], text[], text[], text[], integer, text[])
    OWNER TO mohit;

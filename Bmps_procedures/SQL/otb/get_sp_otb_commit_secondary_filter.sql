-- PROCEDURE: public.get_sp_otb_commit_secondary_filter(boolean, json)

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_commit_secondary_filter(boolean, json);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_commit_secondary_filter(
	IN s_f_s boolean,
	IN s_f_d json)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    product_family TEXT[];
    sub_family TEXT[];
    supplier TEXT[];
    category TEXT[];
    dom_comm TEXT[];
    sub_category TEXT[];
    extended_sub_category TEXT[];
    sub_category_supplier TEXT[];
    sales_year INT[];
    sales_day TEXT[];
    sales_quarter TEXT[];
    sales_date TEXT[];
    sales_month TEXT[];
    sales_week TEXT[];
    budget_year TEXT[];
    -- season TEXT[];
    -- country TEXT[];
    region TEXT[];
    Territory TEXT[];
    -- city TEXT[];
    store_name TEXT[];
    forecast_month TEXT[];
    forecast_weekday INT[];
    forecast_date TEXT[];
    forecast_quarter TEXT[];
    budget_day TEXT[];
    channel TEXT[];
    article_score TEXT[];
	progress INT := 0;
BEGIN
	progress := 14;
	INSERT INTO public.progress_status(status, progress) VALUES ('Invoked get_sp_otb_commit_secondary_filter sorting variables'::text, progress);

	DROP TABLE IF EXISTS otb_min_filter;
    -- Extract values from the JSON input as arrays
    product_family := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'family')::jsonb));
    RAISE NOTICE 'family: %', product_family;
    
    sub_family := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'sub_family')::jsonb));
    RAISE NOTICE 'sub_family: %', sub_family;
    
    supplier := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'supplier')::jsonb));
    RAISE NOTICE 'supplier: %', supplier;
    
    category := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'category')::jsonb));
    RAISE NOTICE 'category: %', category;
    
    dom_comm := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'dom_comm')::jsonb));
    RAISE NOTICE 'dom_comm: %', dom_comm;
    
    sub_category := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'sub_category')::jsonb));
    RAISE NOTICE 'sub_category: %', sub_category;
    
    extended_sub_category := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'extended_sub_category')::jsonb));
    RAISE NOTICE 'extended_sub_category: %', extended_sub_category;
    
    sub_category_supplier := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'sub_category_supplier')::jsonb));
    RAISE NOTICE 'sub_category_supplier: %', sub_category_supplier;
    
    sales_year := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'HistoricalYear')::jsonb));
    RAISE NOTICE 'historical_year: %', sales_year;
    
    sales_day := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_Day')::jsonb));
    RAISE NOTICE 'history_day: %', sales_day;
    
    sales_quarter := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_Quarter')::jsonb));
    RAISE NOTICE 'history_quarter: %', sales_quarter;
    
    sales_date := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_dates')::jsonb));
    RAISE NOTICE 'history_dates: %', sales_date;
    
    sales_month := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_month')::jsonb));
    RAISE NOTICE 'history_month: %', sales_month;
    
    sales_week := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_week')::jsonb));
    RAISE NOTICE 'history_week: %', sales_week;
    
    budget_year := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'BudgetYear')::jsonb));
    RAISE NOTICE 'budget_year: %', budget_year;
    
    -- season := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'season')::jsonb));
    -- RAISE NOTICE 'season: %', season;
    
    -- country := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'country')::jsonb));
    -- RAISE NOTICE 'country: %', country;
    
    region := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'region')::jsonb));
    RAISE NOTICE 'region: %', region;
    
    Territory := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'area')::jsonb));
    RAISE NOTICE 'area: %', Territory;
    
    -- city := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'city')::jsonb));
    -- RAISE NOTICE 'city: %', city;
    
    store_name := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'Store_Name')::jsonb));
    RAISE NOTICE 'store_name: %', store_name;
    
    forecast_month := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'month')::jsonb));
    RAISE NOTICE 'budget_month: %', forecast_month;
    
    forecast_weekday := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'week')::jsonb));
    RAISE NOTICE 'budget_weekday: %', forecast_weekday;
    
    forecast_date := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'BudgetDate')::jsonb));
    RAISE NOTICE 'budget_date: %', forecast_date;
    
    forecast_quarter := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'Quarter')::jsonb));
    RAISE NOTICE 'budget_quarter: %', forecast_quarter;
    
    budget_day := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'Day')::jsonb));
    RAISE NOTICE 'budget_day: %', budget_day;
    
    channel := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'Channel')::jsonb));
    RAISE NOTICE 'channel: %', channel;
    
    article_score := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'article_score')::jsonb));
    RAISE NOTICE 'article_score: %', article_score;

    -- Example SELECT statement for debugging purposes
    -- This should be replaced with actual processing logic
	
	progress := 15;
	INSERT INTO public.progress_status(status, progress) VALUES ('creating filtered otb_min_filter table'::text, progress);

	CREATE TABLE otb_min_filter AS
    	SELECT * FROM item_counter 
	    WHERE (
				COALESCE(array_length(product_family, 1), 0) = 0  OR "Family" = ANY(product_family)
		)
		AND (
				COALESCE(array_length(sub_family, 1), 0) = 0  OR "SubFamily" = ANY(sub_family)
		)
		AND (
				COALESCE(array_length(channel, 1), 0) = 0  OR "Channel" = ANY(channel)
		)
		AND (
	        COALESCE(array_length(supplier, 1), 0) = 0 OR "Supplier" = ANY(supplier)
	    )
	    AND (
	        COALESCE(array_length(category, 1), 0) = 0 OR "Category" = ANY(category)
	    )
	    AND (
	        COALESCE(array_length(dom_comm, 1), 0) = 0 OR "DOM_COMM" = ANY(dom_comm)
	    )
	    AND (
	        COALESCE(array_length(sub_category, 1), 0) = 0 OR "SubCategory" = ANY(sub_category)
	    )
	    AND (
	        COALESCE(array_length(extended_sub_category, 1), 0) = 0 OR "ExtendedSubCategory" = ANY(extended_sub_category)
	    )
	    AND (
	        COALESCE(array_length(sub_category_supplier, 1), 0) = 0 OR "SubCategorySupplier" = ANY(sub_category_supplier)
	    )
	    AND (
	        COALESCE(array_length(sales_year, 1), 0) = 0 OR "historical_year" = ANY(sales_year)
	    )
	    AND (
	        COALESCE(array_length(sales_day, 1), 0) = 0 OR "history_day" = ANY(sales_day)
	    )
	    AND (
	        COALESCE(array_length(sales_quarter, 1), 0) = 0 OR "history_quarter" = ANY(sales_quarter)
	    )
	    AND (
	        COALESCE(array_length(sales_date, 1), 0) = 0 OR "INVOICEDATE" = ANY(sales_date)
	    )
	    AND (
	        COALESCE(array_length(sales_month, 1), 0) = 0 OR "history_month" = ANY(sales_month)
	    )
	    AND (
	        COALESCE(array_length(sales_week, 1), 0) = 0 OR "history_week" = ANY(sales_week)
	    )
	    AND (
	        COALESCE(array_length(budget_year, 1), 0) = 0 OR "Budget_year" = ANY(budget_year)
	    )
	    -- AND (
	    --     COALESCE(array_length(season, 1), 0) = 0 OR "Season" = ANY(season)
	    -- )
	    -- AND (
	    --     COALESCE(array_length(country, 1), 0) = 0 OR "Country" = ANY(country)
	    -- )
	    AND (
	        COALESCE(array_length(region, 1), 0) = 0 OR "Region" = ANY(region)
	    )
	    AND (
	        COALESCE(array_length(territory, 1), 0) = 0 OR "area" = ANY(territory)
	    )
	    -- AND (
	    --     COALESCE(array_length(city, 1), 0) = 0 OR "City" = ANY(city)
	    -- )
	    AND (
	        COALESCE(array_length(store_name, 1), 0) = 0 OR "Store" = ANY(store_name)
	    )
	    AND (
	        COALESCE(array_length(forecast_month, 1), 0) = 0 OR "budget_month" = ANY(forecast_month)
	    )
	    AND (
	        COALESCE(array_length(forecast_weekday, 1), 0) = 0 OR "budget_weekday" = ANY(forecast_weekday)
	    )
	    AND (
	        COALESCE(array_length(forecast_date, 1), 0) = 0 OR "Budget_date" = ANY(forecast_date)
	    )
	    AND (
	        COALESCE(array_length(forecast_quarter, 1), 0) = 0 OR "budget_quarter" = ANY(forecast_quarter)
	    );
	    -- AND (
	    --     COALESCE(array_length(budget_day, 1), 0) = 0 OR "Budget_Day" = ANY(budget_day)
	    -- )
	    -- AND (
	    --     COALESCE(array_length(channel, 1), 0) = 0 OR "Channel" = ANY(channel)
	    -- )
	    -- AND (
	    --     COALESCE(array_length(article_score, 1), 0) = 0 OR "Article_Score" = ANY(article_score)
	    -- );
	progress := 16;
	INSERT INTO public.progress_status(status, progress) VALUES ('finished get_sp_otb_commit_secondary_filter'::text, progress);

END;
$BODY$;
ALTER PROCEDURE public.get_sp_otb_commit_secondary_filter(boolean, json)
    OWNER TO mohit;

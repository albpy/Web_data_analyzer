-- PROCEDURE: public.get_sp_otb_commit_secondary_filter(boolean, json)

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_commit_secondary_filter(boolean, json);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_commit_secondary_filter(
	IN s_f_s boolean,
	IN s_f_d json)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_product_family TEXT[];
    v_sub_family TEXT[];
    v_supplier TEXT[];
    v_category TEXT[];
    v_dom_comm TEXT[];
    v_sub_category TEXT[];
    v_extended_sub_category TEXT[];
    v_sub_category_supplier TEXT[];
    v_sales_year INT[];
    v_sales_day TEXT[];
    v_sales_quarter TEXT[];
    v_sales_date TEXT[];
    v_sales_month TEXT[];
    v_sales_week TEXT[];
    v_budget_year TEXT[];
    v_region TEXT[];
    v_territory TEXT[];
    v_store_name TEXT[];
    v_forecast_month TEXT[];
    v_forecast_weekday INT[];
    v_forecast_date TEXT[];
    v_forecast_quarter TEXT[];
    v_budget_day TEXT[];
    v_channel TEXT[];
    v_article_score TEXT[];
    v_progress INT := 0;
BEGIN
	DROP TABLE IF EXISTS otb_min_filter;
    v_progress := 14;
    INSERT INTO public.progress_status(status, progress) VALUES ('Invoked get_sp_otb_commit_secondary_filter sorting variables', v_progress);

    -- Extract values from the JSON input as arrays
    v_product_family := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'family')::jsonb));
    v_sub_family := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'sub_family')::jsonb));
    v_supplier := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'supplier')::jsonb));
    v_category := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'category')::jsonb));
    v_dom_comm := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'dom_comm')::jsonb));
    v_sub_category := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'sub_category')::jsonb));
    v_extended_sub_category := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'extended_sub_category')::jsonb));
    v_sub_category_supplier := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'sub_category_supplier')::jsonb));
    v_sales_year := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'HistoricalYear')::jsonb));
    v_sales_day := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_Day')::jsonb));
    v_sales_quarter := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_Quarter')::jsonb));
    v_sales_date := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_dates')::jsonb));
    v_sales_month := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_month')::jsonb));
    v_sales_week := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'history_week')::jsonb));
    v_budget_year := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'BudgetYear')::jsonb));
    v_region := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'region')::jsonb));
    v_territory := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'area')::jsonb));
    v_store_name := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'Store_Name')::jsonb));
    v_forecast_month := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'month')::jsonb));
    v_forecast_weekday := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'week')::jsonb));
    v_forecast_date := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'BudgetDate')::jsonb));
    v_forecast_quarter := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'Quarter')::jsonb));
    v_budget_day := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'Day')::jsonb));
    v_channel := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'Channel')::jsonb));
    v_article_score := ARRAY(SELECT jsonb_array_elements_text((s_f_d->'secondary_filter'->'article_score')::jsonb));

    -- Update progress after operations
    v_progress := 15;
    INSERT INTO public.progress_status(status, progress) VALUES ('Finished get_sp_otb_commit_secondary_filter', v_progress);

    -- Creating filtered table
    CREATE TABLE otb_min_filter AS
    SELECT * FROM item_counter
    WHERE 
        (COALESCE(array_length(v_product_family, 1), 0) = 0 OR "family" = ANY(v_product_family))
    AND
        (COALESCE(array_length(v_sub_family, 1), 0) = 0 OR "sub_family" = ANY(v_sub_family))
    AND
        (COALESCE(array_length(v_supplier, 1), 0) = 0 OR "supplier" = ANY(v_supplier))
    AND
        (COALESCE(array_length(v_category, 1), 0) = 0 OR "category_name" = ANY(v_category))
    AND
        (COALESCE(array_length(v_dom_comm, 1), 0) = 0 OR "dom_comm" = ANY(v_dom_comm))
    AND
        (COALESCE(array_length(v_sub_category, 1), 0) = 0 OR "sub_category" = ANY(v_sub_category))
    AND
        (COALESCE(array_length(v_extended_sub_category, 1), 0) = 0 OR "extended_sub_category" = ANY(v_extended_sub_category))
    AND
        (COALESCE(array_length(v_sub_category_supplier, 1), 0) = 0 OR "sub_category_supplier" = ANY(v_sub_category_supplier))
    AND
        (COALESCE(array_length(v_sales_year, 1), 0) = 0 OR "historical_year" = ANY(v_sales_year))
    AND
        (COALESCE(array_length(v_sales_day, 1), 0) = 0 OR "history_day" = ANY(v_sales_day))
    AND
        (COALESCE(array_length(v_sales_quarter, 1), 0) = 0 OR "history_quarter" = ANY(v_sales_quarter))
    AND
        (COALESCE(array_length(v_sales_date, 1), 0) = 0 OR "INVOICEDATE" = ANY(v_sales_date))
    AND
        (COALESCE(array_length(v_sales_month, 1), 0) = 0 OR "history_month" = ANY(v_sales_month))
    AND
        (COALESCE(array_length(v_sales_week, 1), 0) = 0 OR "history_week" = ANY(v_sales_week))
    AND
        (COALESCE(array_length(v_budget_year, 1), 0) = 0 OR "Budget_year" = ANY(v_budget_year))
    AND
        (COALESCE(array_length(v_region, 1), 0) = 0 OR "Region" = ANY(v_region))
    AND
        (COALESCE(array_length(v_territory, 1), 0) = 0 OR "area" = ANY(v_territory))
    AND
        (COALESCE(array_length(v_store_name, 1), 0) = 0 OR "Store" = ANY(v_store_name))
    AND
        (COALESCE(array_length(v_forecast_month, 1), 0) = 0 OR "budget_month" = ANY(v_forecast_month))
    AND
        (COALESCE(array_length(v_forecast_weekday, 1), 0) = 0 OR "budget_weekday" = ANY(v_forecast_weekday))
    AND
        (COALESCE(array_length(v_forecast_date, 1), 0) = 0 OR "Budget_date" = ANY(v_forecast_date))
    AND
        (COALESCE(array_length(v_forecast_quarter, 1), 0) = 0 OR "budget_quarter" = ANY(v_forecast_quarter))
    AND
        (COALESCE(array_length(v_channel, 1), 0) = 0 OR "Channel" = ANY(v_channel));
    -- AND
    --     (COALESCE(array_length(v_article_score, 1), 0) = 0 OR "article_score" = ANY(v_article_score));
END;
$BODY$;
ALTER PROCEDURE public.get_sp_otb_commit_secondary_filter(boolean, json)
    OWNER TO postgres;

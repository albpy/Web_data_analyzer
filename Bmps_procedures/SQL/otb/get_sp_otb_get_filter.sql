-- PROCEDURE: public.get_sp_otb_get_filter()

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_get_filter();

CREATE OR REPLACE PROCEDURE public.get_sp_otb_get_filter(
	)
LANGUAGE 'plpgsql'
AS $BODY$
	DECLARE 
		progress INT := 12;
BEGIN
	progress := 12;
	INSERT INTO public.progress_status(status, progress) VALUES ('Invoked get_sp_otb_get_filter creating otb_sub_filter table'::text, progress);

    TRUNCATE TABLE otb_sub_filter;

	-- CREATE TABLE otb_get_filter 
    -- IF array_length(channels, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("Channel")
        	SELECT DISTINCT "Channel" FROM item_counter WHERE "Channel" IS NOT NULL;--"Channel" = ANY(channels);
	-- END IF;
		
	-- IF array_length(region, 1) IS NOT NULL THEN
		INSERT INTO otb_sub_filter (region)
        	SELECT DISTINCT "Region" FROM item_counter WHERE "Region" IS NOT NULL; --WHERE "Region" = ANY(region);
	-- END IF;
		
	-- IF array_length(store, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (store)
        	SELECT DISTINCT "Store" FROM item_counter WHERE "Store" IS NOT NULL; --WHERE "Store" = ANY(store);
	-- END IF;

	-- IF array_length(bud_quarter, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (budget_quarter)
        SELECT DISTINCT "budget_quarter" FROM item_counter WHERE "budget_quarter" IS NOT NULL; --WHERE "budget_quarter" = ANY(bud_quarter);
    -- END IF;

    -- IF array_length(bud_month, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (budget_month)
        SELECT DISTINCT "budget_month" FROM item_counter WHERE "budget_month" IS NOT NULL; --WHERE "budget_month" = ANY(bud_month);
    -- END IF;

    -- IF array_length(bud_weekday, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (budget_weekday)
        SELECT DISTINCT "budget_weekday" FROM item_counter WHERE "budget_weekday" IS NOT NULL; --WHERE "budget_weekday" = ANY(bud_weekday::integer[]);
    -- END IF;

    -- IF array_length(bud_week, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (budget_day)
        SELECT DISTINCT "budget_week" FROM item_counter WHERE "budget_week" IS NOT NULL; --WHERE "budget_week" = ANY(bud_week);
    -- END IF;

    -- IF array_length(bud_date, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (budget_dates)
        SELECT DISTINCT "Budget_date"::date FROM item_counter  WHERE "Budget_date"::date IS NOT NULL; --WHERE "Budget_date" = ANY(bud_date);
    -- END IF;

    -- IF array_length(hist_year, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (historical_year)
        SELECT DISTINCT "historical_year" FROM item_counter WHERE "historical_year" IS NOT NULL;-- WHERE "historical_year" = ANY(hist_year);
    -- END IF;

    -- IF array_length(hist_quarter, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("history_Quarter")
        SELECT DISTINCT "history_quarter" FROM item_counter WHERE "history_quarter" IS NOT NULL;-- WHERE "history_quarter" = ANY(hist_quarter);
    -- END IF;

    -- IF array_length(hist_month, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (history_month)
        SELECT DISTINCT "history_month" FROM item_counter WHERE "history_month" IS NOT NULL; --WHERE "history_month" = ANY(hist_month);
    -- END IF;

    -- IF array_length(hist_week, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (history_week)
        SELECT DISTINCT "history_week" FROM item_counter WHERE "history_week" IS NOT NULL; --WHERE "history_week" = ANY(hist_week);
    -- END IF;

    -- IF array_length(hist_day, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("history_Day")
        SELECT DISTINCT "history_day" FROM item_counter WHERE "history_day" IS NOT NULL; --WHERE "history_day" = ANY(hist_day);
    -- END IF;

    -- IF array_length(hist_dates, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter (history_datess)
        SELECT DISTINCT "INVOICEDATE"::date FROM item_counter  WHERE "INVOICEDATE"::date IS NOT NULL; --WHERE "INVOICEDATE" = ANY(hist_dates);
    -- END IF;

    -- IF array_length(fam, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("family")
        SELECT DISTINCT "Family" FROM item_counter  WHERE "Family" IS NOT NULL;--WHERE "Family" = ANY(fam);
    -- END IF;

    -- IF array_length(sub_fam, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("sub_Family")
        SELECT DISTINCT "SubFamily" FROM item_counter  WHERE "SubFamily" IS NOT NULL; --WHERE "SubFamily" = ANY(sub_fam);
    -- END IF;

    -- IF array_length(supp, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("Supplier")
        SELECT DISTINCT "Supplier" FROM item_counter WHERE "Supplier" IS NOT NULL; --WHERE "Supplier" = ANY(supp);
    -- END IF;

    -- IF array_length(categ, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("Category")
        SELECT DISTINCT "Category" FROM item_counter WHERE "Category" IS NOT NULL; --WHERE "Category" = ANY(categ);
    -- END IF;

    -- IF array_length(dom_com, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("Dom_Comm")
        SELECT DISTINCT "DOM_COMM" FROM item_counter WHERE "DOM_COMM" IS NOT NULL; --WHERE "DOM_COMM" = ANY(dom_com);
    -- END IF;

    -- IF array_length(sub_categ, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("Sub_Category")
        SELECT DISTINCT "SubCategory" FROM item_counter WHERE "SubCategory" IS NOT NULL; --WHERE "SubCategory" = ANY(sub_categ);
    -- END IF;

    -- IF array_length(extended_sub_categ, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("Extended_sub_category")
        SELECT DISTINCT "ExtendedSubCategory" FROM item_counter WHERE "ExtendedSubCategory" IS NOT NULL; --WHERE "ExtendedSubCategory" = ANY(extended_sub_categ);
    -- END IF;

    -- IF array_length(sub_category_supp, 1) IS NOT NULL THEN
        INSERT INTO otb_sub_filter ("Sub_Category_supplier")
        SELECT DISTINCT "SubCategorySupplier" FROM item_counter WHERE "SubCategorySupplier" IS NOT NULL; --WHERE "SubCategorySupplier" = ANY(sub_category_supp);
    -- END IF;
	progress := 13;
	INSERT INTO public.progress_status(status, progress) VALUES ('ended get_sp_otb_get_filter'::text, progress);

END;
$BODY$;
ALTER PROCEDURE public.get_sp_otb_get_filter()
    OWNER TO mohit;

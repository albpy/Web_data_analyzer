SELECT * FROM pg_stat_activity where state = 'active';

SELECT pg_terminate_backend(110507)

SELECT column_name
	FROM information_schema.columns
	WHERE table_name = 'item_counter';
 SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND table_name = 'ra_sales_item_stock_kpi_joined'
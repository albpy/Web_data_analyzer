-- PROCEDURE: public.get_sp_otb_drill_down_filter(text[], text[])

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_drill_down_filter(text[], text[]);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_drill_down_filter(
	IN columnsnames text[],
	IN columnvalues text[])
LANGUAGE 'plpgsql'
AS $BODY$
	DECLARE
    sql_query TEXT;
    i INT;
	BEGIN
		DROP TABLE IF EXISTS otb_table_drill_in;
	    -- Initialize the dynamic SQL query
	    sql_query := 'CREATE TABLE otb_table_drill_in AS SELECT * FROM item_counter ';

		-- Loop through the column names and values
 	IF columnsnames IS NOT NULL AND array_length(columnsnames, 1) IS NOT NULL THEN
	    FOR i IN 1..array_length(columnsnames, 1) LOOP
	        sql_query := sql_query || 'WHERE ' || '"'|| columnsnames[i] || '"' || ' = ' || quote_literal(columnvalues[i]);
	        IF i < array_length(columnsnames, 1) THEN
	            sql_query := sql_query || ' AND ';
	        END IF;
	    END LOOP;
	END IF;

		RAISE NOTICE 'Expand Filter Query : %', sql_query;
		EXECUTE sql_query;
	
END;
$BODY$;
ALTER PROCEDURE public.get_sp_otb_drill_down_filter(text[], text[])
    OWNER TO mohit;

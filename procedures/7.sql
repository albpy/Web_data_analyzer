-- PROCEDURE: public.get_sp_otb_main_aggregation(text, text[], boolean, text[], text[], text[], text[], text[], text, text)

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_main_aggregation(text, text[], boolean, text[], text[], text[], text[], text[], text, text);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_main_aggregation(
	IN table_title text,
	IN group_ text[],
	IN sfs boolean,
	IN average_columns text[],
	IN sum_columns text[],
	IN kpi_check_box text[],
	IN kpi_revised_cols text[],
	IN scores_matrix text[],
	IN coefficient_score text,
	IN coefficient_score_mix_percent text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    group_by_columns text;
    query_ text;
    select_columns text;
    column_revised text;
	progress INT := 0;
BEGIN
	progress := 17;
	INSERT INTO public.progress_status(status, progress) VALUES ('invoked get_sp_otb_main_aggregation create query'::text, progress);

	DROP TABLE IF EXISTS otb_main_data;
    group_by_columns := array_to_string(group_, ', ');

    -- Build the SELECT clause dynamically
    select_columns := '';

    IF array_length(group_, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(group_, 1) LOOP
            select_columns := select_columns || '"' || group_[i] || '", ';
        END LOOP;
    END IF;

    -- Add the average columns
    IF array_length(average_columns, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(average_columns, 1) LOOP
            select_columns := select_columns || 'AVG("' || average_columns[i] || '") AS "' || average_columns[i] || '", ';
        END LOOP;
    END IF;

    -- Add the sum columns
    IF array_length(sum_columns, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(sum_columns, 1) LOOP
            select_columns := select_columns || 'SUM("' || sum_columns[i] || '") AS "' || sum_columns[i] || '", ';
        END LOOP;
    END IF;

    -- Add the max columns
    IF array_length(kpi_check_box, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(kpi_check_box, 1) LOOP
            select_columns := select_columns || 'MAX("' || kpi_check_box[i] || '") AS "' || kpi_check_box[i] || '", ';
        END LOOP;
    END IF;

    -- Add the revised kpi columns if they exist
    IF array_length(kpi_revised_cols, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(kpi_revised_cols, 1) LOOP
            column_revised := kpi_revised_cols[i];
            IF otb_kpi_column_exists('item_counter', column_revised) THEN
                select_columns := select_columns || 'SUM("' || kpi_revised_cols[i] || '") AS "' || kpi_revised_cols[i] || '", ';
            END IF;
        END LOOP;
    END IF;

    -- Add the scores matrix columns
    IF array_length(scores_matrix, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(scores_matrix, 1) LOOP
            select_columns := select_columns || 'AVG("' || scores_matrix[i] || '") AS "' || scores_matrix[i] || '", ';
        END LOOP;
    END IF;

    -- Add coefficient score columns if not empty
    IF coefficient_score != '' THEN
        select_columns := select_columns || 'AVG("' || coefficient_score || '") AS "' || coefficient_score || '", ';
    END IF;

    IF coefficient_score_mix_percent != '' THEN
        select_columns := select_columns || 'SUM("' || coefficient_score_mix_percent || '") AS "' || coefficient_score_mix_percent || '", ';
    END IF;

    -- Remove the trailing comma and space
    select_columns := rtrim(select_columns, ', ');

    -- Construct the base query
    query_ := 'SELECT ' || select_columns || ' FROM ' || table_title;

    -- Append GROUP BY clause if there are any group columns
    IF array_length(group_, 1) IS NOT NULL THEN
    	query_ := query_ || ' GROUP BY ' || '"' || array_to_string(group_, '", "') || '"';
	END IF;

    -- Construct the dynamic SQL query to create a temp table
    query_ := 'CREATE TABLE otb_main_data AS ' || query_;

    -- Print the dynamic query
    RAISE NOTICE 'Dynamic Query: %', query_;
	progress := 18;
	INSERT INTO public.progress_status(status, progress) VALUES ('get_sp_otb_main_aggregation query exec create table otb_main_data'::text, progress);

    -- Execute the dynamic query
    EXECUTE query_;
	progress := 19;
	INSERT INTO public.progress_status(status, progress) VALUES ('completed get_sp_otb_main_aggregation'::text, progress);

END
$BODY$;
ALTER PROCEDURE public.get_sp_otb_main_aggregation(text, text[], boolean, text[], text[], text[], text[], text[], text, text)
    OWNER TO postgres;

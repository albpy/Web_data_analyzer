-- PROCEDURE: public.get_sp_otb_commit_drilldown(text[], text[], text[], text[], text[], text[], text[], text, text)

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_commit_drilldown(text[], text[], text[], text[], text[], text[], text[], text, text);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_commit_drilldown(
	IN group_ text[],
	IN mean_cols text[],
	IN sum_cols text[],
	IN distribution_cols text[],
	IN kpi_rank_cols text[],
	IN article_score_cols text[],
	IN max_cols text[],
	IN coefficient_score text,
	IN coefficient_score_mix_percent text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    sql_query TEXT;
    i INT;
    group_clause TEXT := '';
    mean_clause TEXT := '';
    sum_clause TEXT := '';
	max_clause TEXT := '';
    distribution_clause TEXT := '';
    kpi_rank_clause TEXT := '';
    article_score_clause TEXT := '';
    column_revised text;
    coefficient_clause  text := '';
BEGIN
	DROP TABLE IF EXISTS otb_drilled_out;
    -- Construct the GROUP BY clause
    IF array_length(group_, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(group_, 1) LOOP
            group_clause := group_clause || '"' ||group_[i] || '"';
            IF i < array_length(group_, 1) THEN
                group_clause := group_clause || ', ';
            END IF;
        END LOOP;
    END IF;

	 -- RAISE NOTICE 'group_clause: %', group_clause;

    -- Construct the mean (AVG) clause
    IF array_length(mean_cols, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(mean_cols, 1) LOOP
            mean_clause := mean_clause || 'AVG("' || mean_cols[i] || '") AS ' || '"'|| mean_cols[i] || '"';
            IF i < array_length(mean_cols, 1) THEN
                mean_clause := mean_clause || ', ';
            END IF;
        END LOOP;
    END IF;

	 -- RAISE NOTICE 'mean_clause: %', mean_clause;

    -- Construct the sum clause
    IF array_length(sum_cols, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(sum_cols, 1) LOOP
            sum_clause := sum_clause || 'SUM("' || sum_cols[i] || '") AS ' || '"' || sum_cols[i] || '"';
            IF i < array_length(sum_cols, 1) THEN
                sum_clause := sum_clause || ', ';
            END IF;
        END LOOP;
    END IF;

	 -- RAISE NOTICE 'sum_clause: %', sum_clause;

    -- Construct the max clause
    IF array_length(max_cols, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(max_cols, 1) LOOP
            max_clause := max_clause || 'MAX("' || max_cols[i] || '") AS ' || '"' || max_cols[i] || '"';
            IF i < array_length(max_cols, 1) THEN
                max_clause := max_clause || ', ';
            END IF;
        END LOOP;
    END IF;

	 -- RAISE NOTICE 'max_clause: %', max_clause;

    -- Construct the distribution (SUM) clause
    IF array_length(distribution_cols, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(distribution_cols, 1) LOOP
			column_revised := distribution_cols[i];
            IF otb_kpi_column_exists('otb_table_drill_in', column_revised) THEN
            	distribution_clause := distribution_clause || 'SUM("' || distribution_cols[i] || '") AS ' || '"' || distribution_cols[i] || '"';
	            IF i < array_length(distribution_cols, 1) THEN
	                distribution_clause := distribution_clause || ', ';
	            END IF;
			END IF;
        END LOOP;
    END IF;

	 -- RAISE NOTICE 'distribution_clause: %', distribution_clause;

    -- Construct the KPI rank (MAX) clause
    IF array_length(kpi_rank_cols, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(kpi_rank_cols, 1) LOOP
            kpi_rank_clause := kpi_rank_clause || 'MAX("' || kpi_rank_cols[i] || '") AS ' || '"' || kpi_rank_cols[i] || '"';
            IF i < array_length(kpi_rank_cols, 1) THEN
                kpi_rank_clause := kpi_rank_clause || ', ';
            END IF;
        END LOOP;
    END IF;

	 -- RAISE NOTICE 'kpi_rank_clause: %', kpi_rank_clause;

    -- Construct the article score (AVG) clause
    IF array_length(article_score_cols, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(article_score_cols, 1) LOOP
            article_score_clause := article_score_clause || 'AVG("' || article_score_cols[i] || '") AS ' || '"' || article_score_cols[i] || '"';
            IF i < array_length(article_score_cols, 1) THEN
                article_score_clause := article_score_clause || ', ';
            END IF;
        END LOOP;
    END IF;
	 -- RAISE NOTICE 'article_score_clause: %', article_score_clause;

	-- Conditionally include coefficient score and coefficient score mix percent
    -- Conditionally include coefficient score and coefficient score mix percent
	RAISE NOTICE 'Type of coefficient_score: %', pg_typeof(coefficient_score);
	RAISE NOTICE 'coefficient_score: %', coefficient_score;

    -- Add coefficient score columns if not empty
    IF coefficient_score IS NOT NULL AND coefficient_score != '' THEN  --!= '' THEN
        coefficient_clause := coefficient_clause || 'AVG("' || coefficient_score || '") AS "' || coefficient_score || '", ';
    END IF;
	 -- RAISE NOTICE 'coefficient_clause: %', coefficient_clause;

    IF coefficient_score_mix_percent IS NOT NULL AND coefficient_score_mix_percent != '' THEN--!= '' THEN
        coefficient_clause := coefficient_clause || 'SUM("' || coefficient_score_mix_percent || '") AS "' || coefficient_score_mix_percent || '", ';
    END IF;
	
	-- RAISE NOTICE 'coefficient_clause_1: %', coefficient_clause;

    -- Construct the final SQL query
  sql_query := 'CREATE TABLE otb_drilled_out AS SELECT ' ||
             CASE WHEN group_clause <> '' THEN group_clause || ', ' ELSE '' END ||
             mean_clause ||
             CASE WHEN mean_clause <> '' THEN ', ' ELSE '' END ||
             sum_clause ||
             CASE WHEN sum_clause <> '' THEN ', ' ELSE '' END ||
             max_clause ||
             CASE WHEN max_clause <> '' THEN ', ' ELSE '' END ||
             distribution_clause ||
             CASE WHEN distribution_clause <> '' THEN ', ' ELSE '' END ||
             kpi_rank_clause ||
             CASE WHEN kpi_rank_clause <> '' THEN ', ' ELSE '' END ||
             article_score_clause ||
             CASE WHEN article_score_clause <> '' THEN ', ' ELSE '' END ||
             coefficient_clause;
             -- ' FROM otb_table_drill_in ' ||
             -- CASE WHEN group_clause <> '' THEN 'GROUP BY ' || group_clause ELSE '' END;
 			
		-- Remove the trailing comma and space
    	sql_query := rtrim(sql_query, ', ');
 		RAISE NOTICE 'befor from: %', sql_query;
        sql_query :=    sql_query || ' FROM otb_table_drill_in ' || 
                 		CASE WHEN group_clause <> '' THEN 'GROUP BY ' || group_clause ELSE '' END;

    RAISE NOTICE 'Query executed: %', sql_query;

    -- Execute the dynamic SQL query
    EXECUTE sql_query;
END
$BODY$;
ALTER PROCEDURE public.get_sp_otb_commit_drilldown(text[], text[], text[], text[], text[], text[], text[], text, text)
    OWNER TO mohit;

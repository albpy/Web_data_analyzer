SELECT "Channel", "ITEMID", "INVENTLOCATIONID"--, TO_CHAR(TO_DATE("Budget_date", 'DD/MM/YYYY'),  'YYYY-MM-DD'))::DATE AS budget_date, 
	,COUNT(*) FROM public.ra_table
	WHERE TO_DATE("Budget_date", 'DD/MM/YYYY') >= TO_DATE('01/07/2024', 'DD/MM/YYYY') AND 
		TO_DATE("Budget_date", 'DD/MM/YYYY') <= TO_DATE('31/12/2024', 'DD/MM/YYYY')
GROUP BY "Channel", "INVENTLOCATIONID", "ITEMID"  order by 4 DESC;

select "Budget_date" from ra_table limit 12;

CREATE TABLE ra_table_without_date AS SELECT * FROM ra_table
select "Budget_date" from ra_table where "Channel" = 'WH' and "ITEMID" = '1MWWREFSR3375HSSL009'
	and "INVENTLOCATIONID" = 'Re-9014'
	
	WHERE "budget_date" >= '2024-07-01' and "Budget_date" <= '2024/12/31'
	GROUP BY "Channel", "INVENTLOCATIONID", "ITEMID" order by 4 DESC

select "Budget_date" from ra_table limit 12
	
DROP TABLE temp1
CREATE TABLE temp1 AS
	SELECT iq.channel, iq.store_code, iq.item_code, SUM(line_amount) AS line_amount
	FROM
	(
		SELECT stt.channel AS channel, stt."INVENTLOCATIONID" AS store_code, 
	           stt."ITEMID" AS item_code, stt."LINEAMOUNT" AS line_amount
		  FROM salesdata_trnx_cleaned stt 
		 WHERE stt.channel || stt."INVENTLOCATIONID" || stt."ITEMID" IN (
				SELECT o."Channel" || o."INVENTLOCATIONID" || o."ITEMID" 
				  FROM ra_table o 
						where "Budget_date"::date >= '2024-07-01'AND 
							"Budget_date"::date <= '2024-12-01')
		   		   AND date("INVOICEDATE") >= '2023-01-01' and date("INVOICEDATE") <= '2023-12-31'
	) AS iq 
	GROUP BY iq.channel, iq.store_code, iq.item_code

	
select sum(line_amount) from temp1
select sum(budget_amount) from temp1


---------
SELECT SUM(line_amount), SUM(budget_amount) FROM (
SELECT t1.channel, t1.store_code, t1.item_code, t1.line_amount, o.budget_amount 
 FROM temp1 t1 JOIN (SELECT "Channel", "ITEMID", "INVENTLOCATIONID", sum(budget_amount) as budget_amount from ra_table group by "Channel", "ITEMID", "INVENTLOCATIONID") o ON (t1.channel = o."Channel" and 
	t1.store_code = o."INVENTLOCATIONID" and t1.item_code = o."ITEMID") ORDER BY
	channel, store_code, item_code
)
--------
SELECT 
    SUM(stt."LINEAMOUNT") AS line_amount, 
    subquery.bamnt,
    subquery.bqty
FROM 
    salesdata_trnx_cleaned stt 
JOIN 
    (SELECT SUM(budget_amount) AS bamnt, SUM(budget_qty) AS bqty FROM ra_table) AS subquery ON TRUE
WHERE 
    EXISTS (
        SELECT 1 
        FROM ra_table o 
        WHERE 
            stt.channel = o."Channel" 
            AND stt."INVENTLOCATIONID" = o."INVENTLOCATIONID" 
            AND stt."ITEMID" = o."ITEMID"
    )
    AND date("INVOICEDATE") >= '2023-01-01' 
    AND date("INVOICEDATE") <= '2023-12-31';

-----
-- select sum("LINEAMOUNT") FROM sales_table_temp_
-- select sum(line_amount) from temp1


select "Budget_date" from ra_table


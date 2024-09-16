DROP TABLE temp1
CREATE TABLE temp1 AS
	SELECT iq.channel, iq.store_code, iq.item_code, SUM("LINEAMOUNT") AS "LINEAMOUNT", 
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
						-- where "Budget_date"::date >= '2024-07-01'AND 
						-- 	"Budget_date"::date <= '2024-12-01'
	)
		   	-- 	   AND date("INVOICEDATE") >= '2023-01-01' and date("INVOICEDATE") <= '2023-12-31'
	) AS iq 
	GROUP BY iq.channel, iq.store_code, iq.item_code

select * from temp1
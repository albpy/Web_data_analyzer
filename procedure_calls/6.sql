SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'ra_item_stock_kpi_temp'
ORDER BY ordinal_position;

CALL get_sp_otb('2023-01-01', '2023-12-31', '2024-07-01', '2024-12-31', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], Null, ARRAY[]::text[])

UPDATE salesdata_trnx_cleaned
SET "INVOICEDATE" = TO_CHAR(TO_DATE("INVOICEDATE", 'DD/MM/YYYY'), 'YYYY-MM-DD');

select sum("LINEAMOUNT") from ra_and_stock_temp where "INVOICEDATE"::date BETWEEN '2023-01-01' and '2023-12-31';
select sum("budget_amount") from ra_and_stock_temp
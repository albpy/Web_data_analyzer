-- PROCEDURE: public.otb_purchase_retail_value_at_gross_sale(text)

-- DROP PROCEDURE IF EXISTS public.otb_purchase_retail_value_at_gross_sale(text);

CREATE OR REPLACE PROCEDURE public.otb_purchase_retail_value_at_gross_sale(
	IN table_name_ text)
LANGUAGE 'plpgsql'
AS $BODY$
	BEGIN
		EXECUTE FORMAT('UPDATE %I
			SET "PurchaseRetailValueatGrossSale" = 
				CASE
					WHEN proposed_sellthru_percent = 0 THEN 0
					WHEN "FirstMargin_percent" = 100 THEN 0
					WHEN markdown_percent = 100 THEN 0
					ELSE
						(
							(
								(budget_cost - (COALESCE(budget_cost, 0) * (NULLIF("Logistic%%", 0) / 100))) 
								/ 
								NULLIF((1 - (NULLIF("FirstMargin_percent", 100) / 100)), 0)
							)
							/
							NULLIF((1 - (NULLIF(markdown_percent, 100) / 100)), 0)
						) 
						/ 
						(NULLIF(proposed_sellthru_percent, 0) / 100)
				END ', table_name_);

END;
$BODY$;
ALTER PROCEDURE public.otb_purchase_retail_value_at_gross_sale(text)
    OWNER TO postgres;

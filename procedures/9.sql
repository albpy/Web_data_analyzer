-- PROCEDURE: public.get_sp_otb_editable_columns_operations(json, text, numeric, text[], text[], numeric, numeric, boolean)

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_editable_columns_operations(json, text, numeric, text[], text[], numeric, numeric, boolean);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_editable_columns_operations(
	IN edited_row json,
	IN column_id text,
	IN new_value numeric,
	IN columnnames text[],
	IN columnvalues text[],
	IN original numeric,
	IN increase numeric,
	IN child boolean)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    -- budget_percent TEXT;
    -- budget_qty TEXT;
    -- adjusted_budget_gross_margin_percent TEXT;
    -- markdown_percent TEXT;
    -- "Logistic%" TEXT;
    -- proposed_sellthru_percent TEXT;
    -- "DisplayItemQty" TEXT;
    -- "COR_EOLStock_value" TEXT;
    
	query_ TEXT;
	condition_ TEXT;
	condition_not TEXT;
	
	i INT;

	initial_avg_price_sum DECIMAL;
	stock_at_retail_price_sum DECIMAL;

	budget_cost_of_goods_on_edited_row DECIMAL;
	revised_budget_amount_on_margin DECIMAL;
	budget_amount_sum DECIMAL;

	edited_column_id TEXT;

	relative_budget_perc_sum DECIMAL;

	increase_bud_perc DECIMAL;
	row_col_value DECIMAL;

	column_id_new TEXT;

	columns_to_filter_length INT;
	budget_perc_selected_sum NUMERIC(20, 8);
	budget_perc_unselected_sum NUMERIC(20, 8);

	budget_perc_excluded_selected_sum DECIMAL;
	budget_perc_excluded_unselected_sum DECIMAL;

	total_budget_amount NUMERIC(20, 8);

	displyqty_query TEXT;

	COR_query TEXT;

	get_selected_budget_per_sum TEXT;
	get_unselected_budget_per_sum TEXT;

	bud_quer TEXT;
BEGIN
    -- DROP TABLE IF EXISTS otb_editable_cols;
    query_ := ' FROM item_counter ';
	condition_ := ' WHERE ';
	condition_not := '';
	
		IF array_length(columnnames, 1) IS NOT NULL THEN
			FOR i IN 1..array_length(columnnames, 1) LOOP
		        condition_ :=  ' AND ' || '"' || columnnames[i] || '"' || ' = ''' || columnvalues[i] || '''';
		    END LOOP;
			-- Remove the leading ' AND ' from the condition_ string
        	condition_ := substr(condition_, 5);
			-- Add the WHERE clause at the beginning
        	condition_ := ' WHERE ' || condition_;
		ELSE
			condition_ := ' WHERE true ';
		END IF;
		RAISE NOTICE 'generate true condition: %', condition_;

-- Not condition
	IF array_length(columnnames, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(columnnames, 1) LOOP
            condition_not := condition_not || ' AND ' || '"' || columnnames[i] || '"' || ' != ''' || columnvalues[i] || '''';
        END LOOP;
        -- Remove the leading ' AND ' from the condition_ string
        condition_not := substr(condition_not, 5);
        -- Add the WHERE clause at the beginning
        condition_not := ' WHERE ' || condition_not;
    ELSE
        condition_not := ' WHERE true ';
    END IF;
    RAISE NOTICE 'Generated NOT Condition: %', condition_not;
-- colid, newvalue	 
	RAISE NOTICE 'Columnid: %', column_id;
	RAISE NOTICE 'newValue: %', new_value;
	RAISE NOTICE 'original: %', original;
	RAISE NOTICE 'increase: %', increase;
	

    -- Assuming there's only one row that matches the conditions in the temporary table
    IF column_id = 'Logistic%' THEN
        RAISE NOTICE 'child status logistic: %', child;
        IF child = FALSE THEN
            UPDATE item_counter SET
                "Logistic%" = new_value,
-- 				"PurchaseRetailValueatGrossSale" = "initial_avera"
                "SupplyCost" = "budget_cost" - (budget_cost* (new_value/100)),
			            -- df = df.with_columns(((((pl.col('budget_amount')-pl.col('SupplyCost')))/(pl.col('budget_amount'))).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100).alias('FirstMargin_percent'))        
				"FirstMargin_percent" = CASE WHEN budget_amount = 0 THEN 0 ELSE
											(budget_amount - ("BudgetCostofGoods" - ("BudgetCostofGoods" * new_value))) / budget_amount * 100
										END,
				"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * ((100 - "FirstMargin_percent"));
             
        ELSE
            EXECUTE FORMAT('UPDATE item_counter SET
                "Logistic%%" = %L,
                "SupplyCost" = budget_cost - (budget_cost * (%L / 100)),
                "FirstMargin_percent" = CASE WHEN budget_amount = 0 THEN 0 ELSE (("budget_amount" - (budget_cost - (budget_cost * (%L / 100)) ) ) * 100) / "budget_amount" END,
                "OTBorPurchaseCost" = CASE WHEN budget_amount = 0 THEN 0 ELSE "otb_retail_value_at_gross_sale" * (100 - ((("budget_amount" - (budget_cost - (budget_cost * (%L / 100)) ) ) * 100) / "budget_amount")) END %s', new_value, new_value, new_value, new_value, condition_);
        END IF;
    END IF;

	

	IF column_id = 'DisplayItemQty' THEN
        RAISE NOTICE 'child status DisplayItemQty: %', child;
        IF child = FALSE THEN
			UPDATE item_counter SET
				"DisplayItemQty" = new_value,
				"DisplayItemValue" = "initial_average_retail_price" * new_value,
				"TYForecast" = "StockatRetailPrice" - "DisplayItemValue" - "COR_EOLStock_value",
				"PurchaseRetailValueatGrossSale" = 
					CASE
						WHEN adjusted_sellthru_percent = 0 THEN 0
						WHEN "FirstMargin_percent" = 100 THEN 0
						WHEN markdown_percent = 100 THEN 0
						ELSE
							(
								(
									(budget_cost - (COALESCE(budget_cost, 0) * (NULLIF("Logistic%", 0) / 100))) 
									/ 
									NULLIF((1 - (NULLIF("FirstMargin_percent", 100) / 100)), 0)
								)
								/
								NULLIF((1 - (NULLIF(markdown_percent, 100) / 100)), 0)
							) 
							/ 
							(NULLIF(adjusted_sellthru_percent, 0) / 100)
					END,
                "OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (100 - "FirstMargin_percent"),
				"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END;
				-- "DisplayItemValue" = 0;
				-- drill down display
        ELSE
			
			RAISE NOTICE 'query_ %', query_;
			RAISE NOTICE 'condition %', condition_;
			displyqty_query := 'SELECT SUM(initial_average_retail_price) ' || query_ || ' ' || condition_;
			RAISE NOTICE 'condition %', displyqty_query;
			EXECUTE displyqty_query INTO initial_avg_price_sum;
			RAISE NOTICE 'SUM initial_avg_price_sum %', initial_avg_price_sum;
            
			IF initial_avg_price_sum = 0 THEN
				UPDATE item_counter
					set DisplayItemValue = 0 || condition_;
			
			ELSE
				RAISE NOTICE 'Type of initial_average_retail_price: %', pg_typeof(initial_avg_price_sum);
				EXECUTE FORMAT('UPDATE item_counter
					SET "DisplayItemValue" = ("initial_average_retail_price"/ %L) * %L ', initial_avg_price_sum, new_value) || condition_;
			END IF;
				UPDATE item_counter SET
					"TYForecast" = "StockatRetailPrice" - "DisplayItemValue" - "COR_EOLStock_value",
					"PurchaseRetailValueatGrossSale" = 
						CASE
							WHEN adjusted_sellthru_percent = 0 THEN 0
							WHEN "FirstMargin_percent" = 100 THEN 0
							WHEN markdown_percent = 100 THEN 0
							ELSE
								(
									(
										(budget_cost - (COALESCE(budget_cost, 0) * (NULLIF("Logistic%", 0) / 100))) 
										/ 
										NULLIF((1 - (NULLIF("FirstMargin_percent", 100) / 100)), 0)
									)
									/
									NULLIF((1 - (NULLIF(markdown_percent, 100) / 100)), 0)
								) 
								/ 
								(NULLIF(adjusted_sellthru_percent, 0) / 100)
						END,
	              	"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (100 - "FirstMargin_percent"),
					"OTBquantity" = CASE
			                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
			                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
			                		END;
		END IF;
    END IF;

	IF column_id = 'COR_EOLStock_value' THEN 
		RAISE NOTICE 'child status COR_EOLStock_value: %', child;
		IF child = FALSE THEN
			COR_query := 'SELECT SUM(initial_average_retail_price) ' || query_  ;
			RAISE NOTICE 'COR_query: %', COR_query;

			EXECUTE COR_query INTO stock_at_retail_price_sum;

			RAISE NOTICE 'stock_at_retail_price_sum: %', stock_at_retail_price_sum;
			
			IF stock_at_retail_price_sum = 0 THEN 
				UPDATE item_counter
					SET "COR_EOLStock_value" = 0; 
			ELSE
				UPDATE item_counter SET 
					"COR_EOLStock_value" = new_value,
					"TYForecast" = "StockatRetailPrice" - "DisplayItemValue" - new_value;
			END IF;
		ELSE
			-- EXECUTE 'SELECT SUM(initial_average_retail_price) ' || query_ || condition_ || ' INTO ' || stock_at_retail_price_sum;
			COR_query := 'SELECT SUM(initial_average_retail_price) ' || query_ || condition_ ;
			RAISE NOTICE 'COR_query: %', COR_query;

			EXECUTE COR_query INTO stock_at_retail_price_sum;
			RAISE NOTICE 'COR_query: %', 'a';
			EXECUTE FORMAT('UPDATE item_counter SET 
				"COR_EOLStock_value" = ("StockatRetailPrice" / %L) * %L,
				"TYForecast" = "StockatRetailPrice" - "DisplayItemValue" - (
				("StockatRetailPrice" / stock_at_retail_price_sum) * new_value) %s', 
				stock_at_retail_price_sum, new_value, condition_);
		END IF;
	END IF;

	IF column_id = 'adjusted_markdown_percent' THEN 
		RAISE NOTICE 'child status adjusted_markdown_percent: %', child;
		IF child = FALSE THEN
			UPDATE item_counter SET
				adjusted_markdown_percent = new_value,
				-- retail_value_including_markdown = ((budget_cost - (COALESCE(budget_cost,0) * ("Logistic%" / 100))) / (1 - ("FirstMargin_percent"/100)))/(1-(new_value/100)),
				retail_value_including_markdown = supply_retail_value/(1-new_value/100),
                "PurchaseRetailValueatGrossSale" = (retail_value_including_markdown/(new_value/100)),
				otb_retail_value_at_gross_sale = "PurchaseRetailValueatGrossSale" -("DisplayItemQty" - "DisplayItemValue" - "COR_EOLStock_value"),
				"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (1-("FirstMargin_percent"/100)),
				"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END,
			-- data = data.with_columns(MarkdownValue=pl.col("retail_value_including_markdown")-pl.col("supply_retail_value"))
				"MarkdownValue" = CASE WHEN supply_retail_value=0 THEN 0 ELSE (supply_retail_value/(1-new_value/100))/supply_retail_value END;

			
				
		ELSE
			EXECUTE FORMAT('
			    UPDATE item_counter 
			    SET 
			        adjusted_markdown_percent = %L,
			        retail_value_including_markdown = supply_retail_value / (1 - %L / 100)
			    %s;', new_value, new_value, condition_);
				call otb_purchase_retail_value_at_gross_sale('item_counter', 'adjusted_markdown_percent');
			EXECUTE FORMAT('
			    UPDATE item_counter 
			    SET 
			        otb_retail_value_at_gross_sale = "PurchaseRetailValueatGrossSale" -("DisplayItemQty" - "DisplayItemValue" - "COR_EOLStock_value"),
			    	"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (1-("FirstMargin_percent"/100)),
					"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END,
			-- data = data.with_columns(MarkdownValue=pl.col("retail_value_including_markdown")-pl.col("supply_retail_value"))
				"MarkdownValue" = (supply_retail_value/(1-%L/100))/supply_retail_value

				%s;', new_value, condition_);				

		END IF;
	END IF;

	IF column_id = 'adjusted_sellthru_percent' THEN
		RAISE NOTICE 'child status adjusted_sellthru_percent: %', child;
		IF child = FALSE THEN
			UPDATE item_counter SET
				adjusted_sellthru_percent = new_value,
				retail_value_including_markdown = supply_retail_value/(1-new_value),
				"PurchaseRetailValueatGrossSale" = (supply_retail_value/(1-new_value))/new_value;
			-- call otb_purchase_retail_value_at_gross_sale('item_counter', 'adjusted_sellthru_percent');				
			UPDATE item_counter SET
				otb_retail_value_at_gross_sale = "PurchaseRetailValueatGrossSale" -("DisplayItemQty" - "DisplayItemValue" - "COR_EOLStock_value"),
				"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (1-("FirstMargin_percent"/100)),
				"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END;
		ELSE 
			EXECUTE FORMAT('
			    UPDATE item_counter 
			    SET
					adjusted_sellthru_percent = %L ,
					
				"PurchaseRetailValueatGrossSale" = budget_amount / (1-((100-%L))/100), 
				otb_retail_value_at_gross_sale = "PurchaseRetailValueatGrossSale" -("DisplayItemQty" - "DisplayItemValue" - "COR_EOLStock_value"),				
				"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (1-("FirstMargin_percent"/100)),
				"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END
				%s', new_value, new_value, condition_);
		END IF;
	END IF;

	IF column_id = 'adjusted_budget_gross_margin_percent' THEN
		RAISE NOTICE 'child status adjusted_budget_gross_margin_percent: %', child;
		budget_cost_of_goods_on_edited_row := (edited_row->>'BudgetCostofGoods')::numeric;
		revised_budget_amount_on_margin := budget_cost_of_goods_on_edited_row/(1-0.01*new_value);
		EXECUTE 'SELECT SUM(budget_amount) FROM item_counter ' || condition_ || ' INSERT INTO budget_amount_sum';
		
		IF child = FALSE THEN
			UPDATE item_counter SET
				budget_amount = revised_budget_amount_on_margin;
		ELSE 
			UPDATE item_counter SET
				 new_value = (100*budget_amount)/budget_amount_sum || condition_;
		END IF;
		column_id_new := 'budget_percent';
	END IF;

--*************************
	-- SELECT COUNT(*) INTO columns_to_filter_length FROM (SELECT unnest(string_to_array(p_child_condition, ' AND '))) AS temp;

--*************************
	IF column_id = 'budget_percent' THEN
		RAISE NOTICE 'column is: budget_percent ';
		-- ALTER TABLE item_counter 

	-- 	budget_cost_of_goods_on_edited_row := (edited_row->>'BudgetCostofGoods')::numeric;
	-- 	revised_budget_amount_on_margin := budget_cost_of_goods_on_edited_row/(1-0.01*new_value);
	-- 	EXECUTE 'SELECT SUM(budget_amount) FROM item_counter ' || condition_ || ' INSERT INTO budget_amount_sum';
		
	-- 	IF child = FALSE THEN
	-- 		UPDATE item_counter SET
	-- 			budget_amount = revised_budget_amount_on_margin;
	-- 	ELSE 
	-- 		UPDATE item_counter SET
	-- 			 new_value = (100*budget_amount)/budget_amount_sum || condition_;
	-- 	END IF;
	-- 	edited_column_id := 'budget_percent';
	-- END IF;

		-- IF array_length(columnnames, 1) = 1 THEN
			get_selected_budget_per_sum := 'SELECT SUM(budget_percent) FROM item_counter ' || condition_;
			get_unselected_budget_per_sum := 'SELECT SUM(budget_percent) FROM item_counter ' || ' '|| condition_not ; 
			EXECUTE get_selected_budget_per_sum INTO budget_perc_selected_sum;
			EXECUTE get_unselected_budget_per_sum INTO budget_perc_unselected_sum;
			
			RAISE NOTICE 'budget sums: selected: %,unselected: %', budget_perc_selected_sum, budget_perc_unselected_sum;
			RAISE NOTICE 'budget condition: %', condition_;
				
			bud_quer := FORMAT('UPDATE item_counter SET
				budget_percent = budget_percent+(%L * (budget_percent / %L)) %s
				',increase, budget_perc_selected_sum, condition_);
			EXECUTE bud_quer;
			RAISE NOTICE 'budget quer: %', bud_quer;

			EXECUTE FORMAT('UPDATE item_counter SET
				budget_percent = budget_percent-(%L * (budget_percent / %L)) %s
				',increase, budget_perc_unselected_sum, condition_not);
		-- END IF;
			column_id_new := 'budget_percent';
	ELSIF 
		column_id NOT IN ('Logistic%', 'DisplayItemQty', 'adjusted_budget_gross_margin_percent', 'COR_EOLStock_value', 'adjusted_markdown_percent', 'adjusted_sellthru_percent', 'Check_box') THEN
			EXECUTE format('SELECT SUM(%I) FROM item_counter ', column_id) || condition_ || ' INSERT INTO budget_perc_excluded_selected_sum';
			EXECUTE format('SELECT SUM(%I) FROM item_counter ', column_id) || condition_not || ' INSERT INTO budget_perc_excluded_unselected_sum';
		

		-- IF array_length(columnnames, 1) = 1 THEN
			EXECUTE format('UPDATE item_counter SET %I = %I +((%I * %L) / %L) ', 
					column_id, column_id, column_id, increase, budget_perc_excluded_selected_sum) || condition_;

			EXECUTE format('UPDATE item_counter SET %I = %I -((%I * %L) / %L) ', 
					column_id, column_id, column_id, increase, budget_perc_excluded_selected_sum) || condition_not;
			column_id_new := 'budget_percent';
	END IF;
	-- IF column_id = 'budget_percent' THEN
	-- 	EXECUTE 'SELECT sum(relative_budget_percent) FROM item_counter INSERT INTO relative_budget_perc_sum';
	-- 	row_col_value := edited_row->>column_id
	-- 	increase_bud_perc := (relative_budget_perc_sum*new_value/100) - (relative_budget_perc_sum*row_col_value/100)
	RAISE NOTICE 'column_id_new: %', column_id_new;

	IF column_id_new = 'budget_percent' THEN
		EXECUTE FORMAT('SELECT SUM(budget_amount) FROM item_counter ') INTO total_budget_amount;
			UPDATE item_counter SET
				budget_amount = budget_percent * (total_budget_amount/100),
				"ACT_FCT" = budget_amount+sales_actual,
				-- budget_qty = ROUND(CASE WHEN initial_average_retail_price = 0 THEN 0 ELSE budget_amount / initial_average_retail_price END),
				budget_margin_percent = CASE WHEN budget_amount = 0 THEN 0 ELSE (budget_gross_margin / budget_amount) * 100 END,
				"SalesActualsByForecast" = CASE WHEN budget_amount = 0 THEN 0 ELSE (sales_actual::FLOAT / budget_amount) * 100 END,
				budget_per_sku_qty_total = CASE WHEN total_sku_count = 0 THEN 0 ELSE (budget_amount / total_sku_count) * 100 END,
				"Deficit" = budget_amount - (initial_average_retail_price * budget_qty),
				act_forecast_vs_budget_percent = CASE WHEN "ACT_FCT" = 0 THEN 0 ELSE (budget_amount / "ACT_FCT") * 100 END,
				adjusted_budget_gross_margin_percent = CASE WHEN budget_amount = 0 THEN 0 ELSE ((budget_amount - "BudgetCostofGoods") / budget_amount) * 100 END,
				"BudgetCostofGoods" = budget_amount - ((budget_amount * adjusted_budget_gross_margin_percent) / 100),
				"FirstMargin_percent" = CASE WHEN budget_amount = 0 THEN 0 ELSE (100 * (budget_amount - "SupplyCost") / budget_amount) END,
				-- markdown_percent = CASE WHEN "GrossSales" = 0 THEN 0 ELSE (("GrossSales" - budget_amount) / "GrossSales") END,
				-- "GrossSales" = CASE WHEN 100 - markdown_percent = 0 THEN 0 ELSE (budget_amount / (100 - markdown_percent)) END,
				retail_value_including_markdown = 
				    CASE 
				        WHEN 1 - (markdown_percent / 100) = 0 THEN 0 
				        ELSE supply_retail_value / (1 - (markdown_percent / 100)) 
				    END,
				"PurchaseRetailValueatGrossSale" = 
				    CASE 
				        WHEN adjusted_sellthru_percent = 0 THEN 0 
				        ELSE retail_value_including_markdown / (adjusted_sellthru_percent / 100) 
				    END,
				"OTBorPurchaseCost" = 
				    CASE 
				        WHEN (1 - ("FirstMargin_percent" / 100)) = 0 THEN 0 
				        ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) 
				    END;
	END IF;

END;
$BODY$;
ALTER PROCEDURE public.get_sp_otb_editable_columns_operations(json, text, numeric, text[], text[], numeric, numeric, boolean)
    OWNER TO mohit;
-- PROCEDURE: public.get_sp_otb_editable_columns_operations(json, text, numeric, text[], text[], numeric, numeric, boolean)

-- DROP PROCEDURE IF EXISTS public.get_sp_otb_editable_columns_operations(json, text, numeric, text[], text[], numeric, numeric, boolean);

CREATE OR REPLACE PROCEDURE public.get_sp_otb_editable_columns_operations(
	IN edited_row json,
	IN column_id text,
	IN new_value numeric,
	IN columnnames text[],
	IN columnvalues text[],
	IN original numeric,
	IN increase numeric,
	IN child boolean)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    -- budget_percent TEXT;
    -- budget_qty TEXT;
    -- adjusted_budget_gross_margin_percent TEXT;
    -- markdown_percent TEXT;
    -- "Logistic%" TEXT;
    -- proposed_sellthru_percent TEXT;
    -- "DisplayItemQty" TEXT;
    -- "COR_EOLStock_value" TEXT;
    
	query_ TEXT;
	condition_ TEXT;
	condition_not TEXT;
	
	i INT;

	initial_avg_price_sum DECIMAL;
	stock_at_retail_price_sum DECIMAL;

	budget_cost_of_goods_on_edited_row DECIMAL;
	revised_budget_amount_on_margin DECIMAL;
	budget_amount_sum DECIMAL;

	edited_column_id TEXT;

	relative_budget_perc_sum DECIMAL;

	increase_bud_perc DECIMAL;
	row_col_value DECIMAL;

	column_id_new TEXT;

	columns_to_filter_length INT;
	budget_perc_selected_sum NUMERIC(20, 8);
	budget_perc_unselected_sum NUMERIC(20, 8);

	budget_perc_excluded_selected_sum DECIMAL;
	budget_perc_excluded_unselected_sum DECIMAL;

	total_budget_amount NUMERIC(20, 8);

	displyqty_query TEXT;

	COR_query TEXT;

	get_selected_budget_per_sum TEXT;
	get_unselected_budget_per_sum TEXT;

	bud_quer TEXT;
BEGIN
    -- DROP TABLE IF EXISTS otb_editable_cols;
    query_ := ' FROM item_counter ';
	condition_ := ' WHERE ';
	condition_not := '';
	
		IF array_length(columnnames, 1) IS NOT NULL THEN
			FOR i IN 1..array_length(columnnames, 1) LOOP
		        condition_ :=  ' AND ' || '"' || columnnames[i] || '"' || ' = ''' || columnvalues[i] || '''';
		    END LOOP;
			-- Remove the leading ' AND ' from the condition_ string
        	condition_ := substr(condition_, 5);
			-- Add the WHERE clause at the beginning
        	condition_ := ' WHERE ' || condition_;
		ELSE
			condition_ := ' WHERE true ';
		END IF;
		RAISE NOTICE 'generate true condition: %', condition_;

-- Not condition
	IF array_length(columnnames, 1) IS NOT NULL THEN
        FOR i IN 1..array_length(columnnames, 1) LOOP
            condition_not := condition_not || ' AND ' || '"' || columnnames[i] || '"' || ' != ''' || columnvalues[i] || '''';
        END LOOP;
        -- Remove the leading ' AND ' from the condition_ string
        condition_not := substr(condition_not, 5);
        -- Add the WHERE clause at the beginning
        condition_not := ' WHERE ' || condition_not;
    ELSE
        condition_not := ' WHERE true ';
    END IF;
    RAISE NOTICE 'Generated NOT Condition: %', condition_not;
-- colid, newvalue	 
	RAISE NOTICE 'Columnid: %', column_id;
	RAISE NOTICE 'newValue: %', new_value;
	RAISE NOTICE 'original: %', original;
	RAISE NOTICE 'increase: %', increase;
	

    -- Assuming there's only one row that matches the conditions in the temporary table
    IF column_id = 'Logistic%' THEN
        RAISE NOTICE 'child status logistic: %', child;
        IF child = FALSE THEN
            UPDATE item_counter SET
                "Logistic%" = new_value,
-- 				"PurchaseRetailValueatGrossSale" = "initial_avera"
                "SupplyCost" = "budget_cost" - (budget_cost* (new_value/100)),
			            -- df = df.with_columns(((((pl.col('budget_amount')-pl.col('SupplyCost')))/(pl.col('budget_amount'))).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100).alias('FirstMargin_percent'))        
				"FirstMargin_percent" = CASE WHEN budget_amount = 0 THEN 0 ELSE
											(budget_amount - ("BudgetCostofGoods" - ("BudgetCostofGoods" * new_value))) / budget_amount * 100
										END,
				"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * ((100 - "FirstMargin_percent"));
             
        ELSE
            EXECUTE FORMAT('UPDATE item_counter SET
                "Logistic%%" = %L,
                "SupplyCost" = budget_cost - (budget_cost * (%L / 100)),
                "FirstMargin_percent" = CASE WHEN budget_amount = 0 THEN 0 ELSE (("budget_amount" - (budget_cost - (budget_cost * (%L / 100)) ) ) * 100) / "budget_amount" END,
                "OTBorPurchaseCost" = CASE WHEN budget_amount = 0 THEN 0 ELSE "otb_retail_value_at_gross_sale" * (100 - ((("budget_amount" - (budget_cost - (budget_cost * (%L / 100)) ) ) * 100) / "budget_amount")) END %s', new_value, new_value, new_value, new_value, condition_);
        END IF;
    END IF;

	

	IF column_id = 'DisplayItemQty' THEN
        RAISE NOTICE 'child status DisplayItemQty: %', child;
        IF child = FALSE THEN
			UPDATE item_counter SET
				"DisplayItemQty" = new_value,
				"DisplayItemValue" = "initial_average_retail_price" * new_value,
				"TYForecast" = "StockatRetailPrice" - "DisplayItemValue" - "COR_EOLStock_value",
				"PurchaseRetailValueatGrossSale" = 
					CASE
						WHEN adjusted_sellthru_percent = 0 THEN 0
						WHEN "FirstMargin_percent" = 100 THEN 0
						WHEN markdown_percent = 100 THEN 0
						ELSE
							(
								(
									(budget_cost - (COALESCE(budget_cost, 0) * (NULLIF("Logistic%", 0) / 100))) 
									/ 
									NULLIF((1 - (NULLIF("FirstMargin_percent", 100) / 100)), 0)
								)
								/
								NULLIF((1 - (NULLIF(markdown_percent, 100) / 100)), 0)
							) 
							/ 
							(NULLIF(adjusted_sellthru_percent, 0) / 100)
					END,
                "OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (100 - "FirstMargin_percent"),
				"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END;
				-- "DisplayItemValue" = 0;
				-- drill down display
        ELSE
			
			RAISE NOTICE 'query_ %', query_;
			RAISE NOTICE 'condition %', condition_;
			displyqty_query := 'SELECT SUM(initial_average_retail_price) ' || query_ || ' ' || condition_;
			RAISE NOTICE 'condition %', displyqty_query;
			EXECUTE displyqty_query INTO initial_avg_price_sum;
			RAISE NOTICE 'SUM initial_avg_price_sum %', initial_avg_price_sum;
            
			IF initial_avg_price_sum = 0 THEN
				UPDATE item_counter
					set DisplayItemValue = 0 || condition_;
			
			ELSE
				RAISE NOTICE 'Type of initial_average_retail_price: %', pg_typeof(initial_avg_price_sum);
				EXECUTE FORMAT('UPDATE item_counter
					SET "DisplayItemValue" = ("initial_average_retail_price"/ %L) * %L ', initial_avg_price_sum, new_value) || condition_;
			END IF;
				UPDATE item_counter SET
					"TYForecast" = "StockatRetailPrice" - "DisplayItemValue" - "COR_EOLStock_value",
					"PurchaseRetailValueatGrossSale" = 
						CASE
							WHEN adjusted_sellthru_percent = 0 THEN 0
							WHEN "FirstMargin_percent" = 100 THEN 0
							WHEN markdown_percent = 100 THEN 0
							ELSE
								(
									(
										(budget_cost - (COALESCE(budget_cost, 0) * (NULLIF("Logistic%", 0) / 100))) 
										/ 
										NULLIF((1 - (NULLIF("FirstMargin_percent", 100) / 100)), 0)
									)
									/
									NULLIF((1 - (NULLIF(markdown_percent, 100) / 100)), 0)
								) 
								/ 
								(NULLIF(adjusted_sellthru_percent, 0) / 100)
						END,
	              	"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (100 - "FirstMargin_percent"),
					"OTBquantity" = CASE
			                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
			                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
			                		END;
		END IF;
    END IF;

	IF column_id = 'COR_EOLStock_value' THEN 
		RAISE NOTICE 'child status COR_EOLStock_value: %', child;
		IF child = FALSE THEN
			COR_query := 'SELECT SUM(initial_average_retail_price) ' || query_  ;
			RAISE NOTICE 'COR_query: %', COR_query;

			EXECUTE COR_query INTO stock_at_retail_price_sum;

			RAISE NOTICE 'stock_at_retail_price_sum: %', stock_at_retail_price_sum;
			
			IF stock_at_retail_price_sum = 0 THEN 
				UPDATE item_counter
					SET "COR_EOLStock_value" = 0; 
			ELSE
				UPDATE item_counter SET 
					"COR_EOLStock_value" = new_value,
					"TYForecast" = "StockatRetailPrice" - "DisplayItemValue" - new_value;
			END IF;
		ELSE
			-- EXECUTE 'SELECT SUM(initial_average_retail_price) ' || query_ || condition_ || ' INTO ' || stock_at_retail_price_sum;
			COR_query := 'SELECT SUM(initial_average_retail_price) ' || query_ || condition_ ;
			RAISE NOTICE 'COR_query: %', COR_query;

			EXECUTE COR_query INTO stock_at_retail_price_sum;
			RAISE NOTICE 'COR_query: %', 'a';
			EXECUTE FORMAT('UPDATE item_counter SET 
				"COR_EOLStock_value" = ("StockatRetailPrice" / %L) * %L,
				"TYForecast" = "StockatRetailPrice" - "DisplayItemValue" - (
				("StockatRetailPrice" / stock_at_retail_price_sum) * new_value) %s', 
				stock_at_retail_price_sum, new_value, condition_);
		END IF;
	END IF;

	IF column_id = 'adjusted_markdown_percent' THEN 
		RAISE NOTICE 'child status adjusted_markdown_percent: %', child;
		IF child = FALSE THEN
			UPDATE item_counter SET
				adjusted_markdown_percent = new_value,
				-- retail_value_including_markdown = ((budget_cost - (COALESCE(budget_cost,0) * ("Logistic%" / 100))) / (1 - ("FirstMargin_percent"/100)))/(1-(new_value/100)),
				retail_value_including_markdown = supply_retail_value/(1-new_value/100),
                "PurchaseRetailValueatGrossSale" = (retail_value_including_markdown/(new_value/100)),
				otb_retail_value_at_gross_sale = "PurchaseRetailValueatGrossSale" -("DisplayItemQty" - "DisplayItemValue" - "COR_EOLStock_value"),
				"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (1-("FirstMargin_percent"/100)),
				"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END,
			-- data = data.with_columns(MarkdownValue=pl.col("retail_value_including_markdown")-pl.col("supply_retail_value"))
				"MarkdownValue" = CASE WHEN supply_retail_value=0 THEN 0 ELSE (supply_retail_value/(1-new_value/100))/supply_retail_value END;

			
				
		ELSE
			EXECUTE FORMAT('
			    UPDATE item_counter 
			    SET 
			        adjusted_markdown_percent = %L,
			        retail_value_including_markdown = supply_retail_value / (1 - %L / 100)
			    %s;', new_value, new_value, condition_);
				call otb_purchase_retail_value_at_gross_sale('item_counter', 'adjusted_markdown_percent');
			EXECUTE FORMAT('
			    UPDATE item_counter 
			    SET 
			        otb_retail_value_at_gross_sale = "PurchaseRetailValueatGrossSale" -("DisplayItemQty" - "DisplayItemValue" - "COR_EOLStock_value"),
			    	"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (1-("FirstMargin_percent"/100)),
					"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END,
			-- data = data.with_columns(MarkdownValue=pl.col("retail_value_including_markdown")-pl.col("supply_retail_value"))
				"MarkdownValue" = (supply_retail_value/(1-%L/100))/supply_retail_value

				%s;', new_value, condition_);				

		END IF;
	END IF;

	IF column_id = 'adjusted_sellthru_percent' THEN
		RAISE NOTICE 'child status adjusted_sellthru_percent: %', child;
		IF child = FALSE THEN
			UPDATE item_counter SET
				adjusted_sellthru_percent = new_value,
				retail_value_including_markdown = supply_retail_value/(1-new_value),
				"PurchaseRetailValueatGrossSale" = (supply_retail_value/(1-new_value))/new_value;
			-- call otb_purchase_retail_value_at_gross_sale('item_counter', 'adjusted_sellthru_percent');				
			UPDATE item_counter SET
				otb_retail_value_at_gross_sale = "PurchaseRetailValueatGrossSale" -("DisplayItemQty" - "DisplayItemValue" - "COR_EOLStock_value"),
				"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (1-("FirstMargin_percent"/100)),
				"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END;
		ELSE 
			EXECUTE FORMAT('
			    UPDATE item_counter 
			    SET
					adjusted_sellthru_percent = %L ,
					
				"PurchaseRetailValueatGrossSale" = budget_amount / (1-((100-%L))/100), 
				otb_retail_value_at_gross_sale = "PurchaseRetailValueatGrossSale" -("DisplayItemQty" - "DisplayItemValue" - "COR_EOLStock_value"),				
				"OTBorPurchaseCost" = "otb_retail_value_at_gross_sale" * (1-("FirstMargin_percent"/100)),
				"OTBquantity" = CASE
		                    		WHEN budget_cost = 0 OR budget_qty = 0 THEN 0
		                    		ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) / budget_cost / budget_qty
		                		END
				%s', new_value, new_value, condition_);
		END IF;
	END IF;

	IF column_id = 'adjusted_budget_gross_margin_percent' THEN
		RAISE NOTICE 'child status adjusted_budget_gross_margin_percent: %', child;
		budget_cost_of_goods_on_edited_row := (edited_row->>'BudgetCostofGoods')::numeric;
		revised_budget_amount_on_margin := budget_cost_of_goods_on_edited_row/(1-0.01*new_value);
		EXECUTE 'SELECT SUM(budget_amount) FROM item_counter ' || condition_ || ' INSERT INTO budget_amount_sum';
		
		IF child = FALSE THEN
			UPDATE item_counter SET
				budget_amount = revised_budget_amount_on_margin;
		ELSE 
			UPDATE item_counter SET
				 new_value = (100*budget_amount)/budget_amount_sum || condition_;
		END IF;
		column_id_new := 'budget_percent';
	END IF;

--*************************
	-- SELECT COUNT(*) INTO columns_to_filter_length FROM (SELECT unnest(string_to_array(p_child_condition, ' AND '))) AS temp;

--*************************
	IF column_id = 'budget_percent' THEN
		RAISE NOTICE 'column is: budget_percent ';
		-- ALTER TABLE item_counter 

	-- 	budget_cost_of_goods_on_edited_row := (edited_row->>'BudgetCostofGoods')::numeric;
	-- 	revised_budget_amount_on_margin := budget_cost_of_goods_on_edited_row/(1-0.01*new_value);
	-- 	EXECUTE 'SELECT SUM(budget_amount) FROM item_counter ' || condition_ || ' INSERT INTO budget_amount_sum';
		
	-- 	IF child = FALSE THEN
	-- 		UPDATE item_counter SET
	-- 			budget_amount = revised_budget_amount_on_margin;
	-- 	ELSE 
	-- 		UPDATE item_counter SET
	-- 			 new_value = (100*budget_amount)/budget_amount_sum || condition_;
	-- 	END IF;
	-- 	edited_column_id := 'budget_percent';
	-- END IF;

		-- IF array_length(columnnames, 1) = 1 THEN
			get_selected_budget_per_sum := 'SELECT SUM(budget_percent) FROM item_counter ' || condition_;
			get_unselected_budget_per_sum := 'SELECT SUM(budget_percent) FROM item_counter ' || ' '|| condition_not ; 
			EXECUTE get_selected_budget_per_sum INTO budget_perc_selected_sum;
			EXECUTE get_unselected_budget_per_sum INTO budget_perc_unselected_sum;
			
			RAISE NOTICE 'budget sums: selected: %,unselected: %', budget_perc_selected_sum, budget_perc_unselected_sum;
			RAISE NOTICE 'budget condition: %', condition_;
				
			bud_quer := FORMAT('UPDATE item_counter SET
				budget_percent = budget_percent+(%L * (budget_percent / %L)) %s
				',increase, budget_perc_selected_sum, condition_);
			EXECUTE bud_quer;
			RAISE NOTICE 'budget quer: %', bud_quer;

			EXECUTE FORMAT('UPDATE item_counter SET
				budget_percent = budget_percent-(%L * (budget_percent / %L)) %s
				',increase, budget_perc_unselected_sum, condition_not);
		-- END IF;
			column_id_new := 'budget_percent';
	ELSIF 
		column_id NOT IN ('Logistic%', 'DisplayItemQty', 'adjusted_budget_gross_margin_percent', 'COR_EOLStock_value', 'adjusted_markdown_percent', 'adjusted_sellthru_percent', 'Check_box') THEN
			EXECUTE format('SELECT SUM(%I) FROM item_counter ', column_id) || condition_ || ' INSERT INTO budget_perc_excluded_selected_sum';
			EXECUTE format('SELECT SUM(%I) FROM item_counter ', column_id) || condition_not || ' INSERT INTO budget_perc_excluded_unselected_sum';
		

		-- IF array_length(columnnames, 1) = 1 THEN
			EXECUTE format('UPDATE item_counter SET %I = %I +((%I * %L) / %L) ', 
					column_id, column_id, column_id, increase, budget_perc_excluded_selected_sum) || condition_;

			EXECUTE format('UPDATE item_counter SET %I = %I -((%I * %L) / %L) ', 
					column_id, column_id, column_id, increase, budget_perc_excluded_selected_sum) || condition_not;
			column_id_new := 'budget_percent';
	END IF;
	-- IF column_id = 'budget_percent' THEN
	-- 	EXECUTE 'SELECT sum(relative_budget_percent) FROM item_counter INSERT INTO relative_budget_perc_sum';
	-- 	row_col_value := edited_row->>column_id
	-- 	increase_bud_perc := (relative_budget_perc_sum*new_value/100) - (relative_budget_perc_sum*row_col_value/100)
	RAISE NOTICE 'column_id_new: %', column_id_new;

	IF column_id_new = 'budget_percent' THEN
		EXECUTE FORMAT('SELECT SUM(budget_amount) FROM item_counter ') INTO total_budget_amount;
			UPDATE item_counter SET
				budget_amount = budget_percent * (total_budget_amount/100),
				"ACT_FCT" = budget_amount+sales_actual,
				-- budget_qty = ROUND(CASE WHEN initial_average_retail_price = 0 THEN 0 ELSE budget_amount / initial_average_retail_price END),
				budget_margin_percent = CASE WHEN budget_amount = 0 THEN 0 ELSE (budget_gross_margin / budget_amount) * 100 END,
				"SalesActualsByForecast" = CASE WHEN budget_amount = 0 THEN 0 ELSE (sales_actual::FLOAT / budget_amount) * 100 END,
				budget_per_sku_qty_total = CASE WHEN total_sku_count = 0 THEN 0 ELSE (budget_amount / total_sku_count) * 100 END,
				"Deficit" = budget_amount - (initial_average_retail_price * budget_qty),
				act_forecast_vs_budget_percent = CASE WHEN "ACT_FCT" = 0 THEN 0 ELSE (budget_amount / "ACT_FCT") * 100 END,
				adjusted_budget_gross_margin_percent = CASE WHEN budget_amount = 0 THEN 0 ELSE ((budget_amount - "BudgetCostofGoods") / budget_amount) * 100 END,
				"BudgetCostofGoods" = budget_amount - ((budget_amount * adjusted_budget_gross_margin_percent) / 100),
				"FirstMargin_percent" = CASE WHEN budget_amount = 0 THEN 0 ELSE (100 * (budget_amount - "SupplyCost") / budget_amount) END,
				-- markdown_percent = CASE WHEN "GrossSales" = 0 THEN 0 ELSE (("GrossSales" - budget_amount) / "GrossSales") END,
				-- "GrossSales" = CASE WHEN 100 - markdown_percent = 0 THEN 0 ELSE (budget_amount / (100 - markdown_percent)) END,
				retail_value_including_markdown = 
				    CASE 
				        WHEN 1 - (markdown_percent / 100) = 0 THEN 0 
				        ELSE supply_retail_value / (1 - (markdown_percent / 100)) 
				    END,
				"PurchaseRetailValueatGrossSale" = 
				    CASE 
				        WHEN adjusted_sellthru_percent = 0 THEN 0 
				        ELSE retail_value_including_markdown / (adjusted_sellthru_percent / 100) 
				    END,
				"OTBorPurchaseCost" = 
				    CASE 
				        WHEN (1 - ("FirstMargin_percent" / 100)) = 0 THEN 0 
				        ELSE ("PurchaseRetailValueatGrossSale" * (1 - ("FirstMargin_percent" / 100))) 
				    END;
	END IF;

END;
$BODY$;
ALTER PROCEDURE public.get_sp_otb_editable_columns_operations(json, text, numeric, text[], text[], numeric, numeric, boolean)
    OWNER TO mohit;

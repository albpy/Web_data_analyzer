CALL get_sp_otb('2023-03-01', '2023-03-03', '2024-07-01', '2024-07-03', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], Null, ARRAY[]::text[])

CALL get_sp_otb_itemwise_calculations()

CALL get_sp_otb_get_filter()

CALL get_sp_otb_apply_secondary_filter('item_counter', 'secondary_filter': {'family': [], 
                    'sub_family': [], 
                    'supplier': [], 
                    'category': [], 
                    'dom_comm': [], 
                    'sub_category': [], 
                    'extended_sub_category': [], 
                    'sub_category_supplier': [], 
                    'HistoricalYear': [], 
                    'history_Day': [], 
                    'history_Quarter': [], 
                    'history_dates': [], 
                    'history_month': [], 
                    'history_week': [], 
                    'BudgetYear': [], 
                    'season': [], 
                    'country': [], 
                    'region': ['CAIRO'], 
                    'area': [], 
                    'city': [], 
                    'Store_Name': [], 
                    'month': [], 
                    'week': [], 
                    'BudgetDate': [], 
                    'Quarter': ['3'], 
                    'Day': [], 
                    'Channel': ['WAREHOUSE'], 
                    'article_score': ['sale']}, bool, ARRAY[]::text[], '')

CALL get_sp_otb_output_data_calc()
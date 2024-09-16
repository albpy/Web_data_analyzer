call get_sp_otb_commit_secondary_filter(
	TRUE,
	'{
    "secondary_filter": {
        "family": [],
        "sub_family": [],
        "supplier": [],
        "category": [],
        "dom_comm": [],
        "sub_category": [],
        "extended_sub_category": [],
        "sub_category_supplier": [],
        "HistoricalYear": [],
        "history_Day": [],
        "history_Quarter": [],
        "history_dates": [],
        "history_month": [],
        "history_week": [],
        "BudgetYear": [],
        "season": [],
        "country": [],
        "region": [],
        "area": [],
        "city": [],
        "Store_Name": [],
        "month": [],
        "week": [],
        "BudgetDate": [],
        "Quarter": [],
        "Day": [],
        "Channel": ["WH"],
        "article_score": []
    }
}
')

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'item_counter'
ORDER BY ordinal_position;
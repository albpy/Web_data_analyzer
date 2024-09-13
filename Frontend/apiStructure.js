
// Initial API structure
export const apiStructure = {
    fetch_from_db: true,
    company : false,
    page_size: 10,
    page_number: 0,
    history_date_range: { fro: null, to: null },
    forecast_date_range: { fro: '2024-12-13', to: '2023-12-30' },
    sales_channel: [],
    product_family: [],
    sub_families: [],
    category: [],
    sub_category: [],
    suppliers: [],
    sku: [],
    top_items: [],
    store_class: [],
    select_all_kpi: false,
    table_changes: {},
    group_by: { status: false },
    expand: { status: false },
    secondary_filter: {},
    cumulative: false // Add cumulative field
};
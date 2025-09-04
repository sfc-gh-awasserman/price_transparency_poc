// This is how we start the data ingest process, after the setup steps have been completed. 
// This function creates the DAG, and begins the processing. 

USE DATABASE PRICE_TRANSPARENCY_POC;

CALL process_pricing_transparency_file(
    '2025_04_01_priority_health_HMO_in-network-rates.json', 
    1000,    -- segments_per_task
    10,      -- dag_rows - dynamically set 
    8,       -- dag_cols - dynamically set 
    'XSMALL' -- warehouse_size
);


select * from negotiated_prices_v limit 100;



CALL process_pricing_transparency_file(
    '2025-07-18_Blue-Cross-and-Blue-Shield-of-Illinois_Blue-Options-or-Blue-Choice-Options_in-network-rates.json.gz', 
    3000,    -- segments_per_task
    5,      -- dag_rows  
    10,       -- dag_cols
    'XSMALL' -- warehouse_size
);
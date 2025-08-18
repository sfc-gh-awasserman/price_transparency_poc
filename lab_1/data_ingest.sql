// This is how we start the data ingest process, after the setup steps have been completed. 
// This function creates the DAG, and begins the processing. 


CALL process_pricing_transparency_file(
    '2025_04_01_priority_health_HMO_in-network-rates.json', 
    1000,    -- segments_per_task
    10,      -- dag_rows - dynamically set 
    8,       -- dag_cols - dynamically set 
    'MEDIUM' -- warehouse_size
);


select * from negotiated_prices_v limit 100;
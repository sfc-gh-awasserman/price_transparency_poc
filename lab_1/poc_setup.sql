-- ================================================================================================
-- Complete Setup for CMS Pricing Transparency Solution
-- This script sets up everything needed to run the pricing transparency data processing
-- in Snowflake without any external dependencies.
-- ================================================================================================

-- ============================
-- PART 1: Roles and Privileges
-- ============================

-- Create task role (requires SECURITYADMIN privileges)
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE RL_TASK_ADMIN_DEMO 
    COMMENT = 'Task admin role created for pricing transparency demo solution';

-- Grant task execution privileges (requires ACCOUNTADMIN privileges)
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE RL_TASK_ADMIN_DEMO;

-- Grant the task role to PUBLIC for demo purposes
USE ROLE SECURITYADMIN;
GRANT ROLE RL_TASK_ADMIN_DEMO TO ROLE PUBLIC;

-- ============================
-- PART 2: Database and Schema Setup
-- ============================

USE ROLE SYSADMIN;

-- Create main database
CREATE OR REPLACE DATABASE PRICE_TRANSPARENCY_POC
    COMMENT = 'Database for CMS pricing transparency demo';

-- Transfer ownership to PUBLIC role for simplicity
GRANT OWNERSHIP ON DATABASE PRICE_TRANSPARENCY_POC TO ROLE PUBLIC;
GRANT OWNERSHIP ON SCHEMA PRICE_TRANSPARENCY_POC.PUBLIC TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE PRICE_TRANSPARENCY_POC TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON SCHEMA PRICE_TRANSPARENCY_POC.PUBLIC TO ROLE PUBLIC;

-- Switch to working context
USE ROLE PUBLIC;
USE DATABASE PRICE_TRANSPARENCY_POC;
USE SCHEMA PUBLIC;

-- ============================
-- PART 3: Warehouses (Optional - for parallelism)
-- ============================

-- Create main warehouse
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS PRICING_TRANS_DEMO_WH WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 300
    COMMENT = 'Main warehouse for pricing transparency demo processing';

-- Create additional warehouses for parallel processing
CREATE WAREHOUSE IF NOT EXISTS PRICING_TRANS_DEMO_TASK_0_WH WITH WAREHOUSE_SIZE = 'SMALL' AUTO_RESUME = TRUE AUTO_SUSPEND = 300;
CREATE WAREHOUSE IF NOT EXISTS PRICING_TRANS_DEMO_TASK_1_WH WITH WAREHOUSE_SIZE = 'SMALL' AUTO_RESUME = TRUE AUTO_SUSPEND = 300;
CREATE WAREHOUSE IF NOT EXISTS PRICING_TRANS_DEMO_TASK_2_WH WITH WAREHOUSE_SIZE = 'SMALL' AUTO_RESUME = TRUE AUTO_SUSPEND = 300;
CREATE WAREHOUSE IF NOT EXISTS PRICING_TRANS_DEMO_TASK_3_WH WITH WAREHOUSE_SIZE = 'SMALL' AUTO_RESUME = TRUE AUTO_SUSPEND = 300;
CREATE WAREHOUSE IF NOT EXISTS PRICING_TRANS_DEMO_TASK_4_WH WITH WAREHOUSE_SIZE = 'SMALL' AUTO_RESUME = TRUE AUTO_SUSPEND = 300;

-- Grant privileges on warehouses
GRANT ALL PRIVILEGES ON WAREHOUSE PRICING_TRANS_DEMO_WH TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON WAREHOUSE PRICING_TRANS_DEMO_TASK_0_WH TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON WAREHOUSE PRICING_TRANS_DEMO_TASK_1_WH TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON WAREHOUSE PRICING_TRANS_DEMO_TASK_2_WH TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON WAREHOUSE PRICING_TRANS_DEMO_TASK_3_WH TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON WAREHOUSE PRICING_TRANS_DEMO_TASK_4_WH TO ROLE PUBLIC;

USE ROLE PUBLIC;
USE WAREHOUSE PRICING_TRANS_DEMO_WH;

-- ============================
-- PART 4: Stages
-- ============================

-- Internal stage for libraries and scripts
CREATE OR REPLACE STAGE lib_stg
    COMMENT = 'Stage for holding libraries and other core artifacts';

-- Internal stage for data (for sample/demo data)
CREATE OR REPLACE STAGE data_stg
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for holding sample data files';

-- NOTE: External stage must be created manually with your cloud provider credentials
-- This is done during the storage_setup.sql step 

-- CREATE OR REPLACE STAGE ext_data_stg
--     URL = ''
--     STORAGE_INTEGRATION = POC_INTEGRATION
--     DIRECTORY = (ENABLE = TRUE REFRESH_ON_CREATE = FALSE)
--     COMMENT = 'External stage for pricing transparency data files';


-- ============================
-- PART 5: Tables
-- ============================

-- Execution status for the various sub-tasks that get spawned
CREATE OR REPLACE TRANSIENT TABLE segment_task_execution_status (
    data_file VARCHAR,
    task_name VARCHAR,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    end_time TIMESTAMP,
    task_ret_status VARIANT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- Maps tasks to segments they should parse
CREATE OR REPLACE TRANSIENT TABLE task_to_segmentids (
    bucket VARCHAR,
    data_file VARCHAR,
    assigned_task_name VARCHAR,
    from_idx NUMBER,
    to_idx NUMBER,
    segments_record_count NUMBER,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- Provider reference records
CREATE OR REPLACE TRANSIENT TABLE in_network_rates_provider_references (
    seq_no NUMBER,
    data_file VARCHAR,
    segment_id VARCHAR,
    provider_reference VARIANT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- Segment headers
CREATE OR REPLACE TRANSIENT TABLE in_network_rates_segment_header (
    data_file VARCHAR,
    segment_id VARCHAR,
    negotiated_rates_info VARIANT,
    negotiated_rates_count NUMBER,
    bundled_codes_count NUMBER,
    covered_services_count NUMBER,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- File headers
CREATE OR REPLACE TRANSIENT TABLE in_network_rates_file_header (
    data_file VARCHAR,
    data_file_basename VARCHAR,
    cleansed_data_file_basename VARCHAR,
    header VARIANT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- ============================
-- PART 6: Views
-- ============================

-- View to check if all segments were parsed
CREATE OR REPLACE VIEW segments_counts_for_datafile_v 
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
AS
SELECT 
    DISTINCT data_file, task_name,
    JSON_EXTRACT_PATH_TEXT(task_ret_status, 'start_rec_num')::INT AS start_rec_num,
    JSON_EXTRACT_PATH_TEXT(task_ret_status, 'end_rec_num')::INT AS end_rec_num,
    JSON_EXTRACT_PATH_TEXT(task_ret_status, 'last_seg_no')::INT AS total_no_of_segments,
    TIMESTAMPDIFF('minute', start_time, end_time) AS elapsed,
    JSON_EXTRACT_PATH_TEXT(task_ret_status, 'Parsing_error') AS parsing_error,
    JSON_EXTRACT_PATH_TEXT(task_ret_status, 'EOF_Reached') AS eof_reached
FROM segment_task_execution_status
WHERE JSON_EXTRACT_PATH_TEXT(task_ret_status, 'EOF_Reached') = 'True'
ORDER BY start_rec_num;

-- View for currently running parsing tasks
CREATE OR REPLACE VIEW current_segment_parsing_tasks_v
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
AS
SELECT 
    l.* EXCLUDE(data_file, inserted_at, end_time, task_ret_status),
    r.* EXCLUDE(data_file, inserted_at)
FROM segment_task_execution_status AS l
JOIN task_to_segmentids AS r
    ON r.data_file = l.data_file
    AND CONTAINS(LOWER(l.task_name), LOWER(r.assigned_task_name)) = TRUE
WHERE end_time IS NULL;

-- View for file ingestion elapsed time
CREATE OR REPLACE VIEW file_ingestion_elapsed_v
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
AS
SELECT * FROM (
    WITH base AS (
        SELECT 
            data_file,
            MIN(start_time) AS start_time,
            MAX(NVL(end_time, CURRENT_TIMESTAMP())) AS end_time
        FROM segment_task_execution_status
        GROUP BY data_file 
    )
    SELECT *,
        TIMESTAMPDIFF('minutes', l.start_time, l.end_time) AS elapsed_minutes
    FROM base AS l
);

-- View for negotiated arrangements headers
CREATE OR REPLACE VIEW negotiated_arrangements_header_v
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
AS
SELECT 
    SPLIT_PART(segment_id, '::', 1) AS negotiation_arrangement,
    SPLIT_PART(segment_id, '::', 3) AS billing_code,
    SPLIT_PART(segment_id, '::', 2) AS billing_code_type,
    SPLIT_PART(segment_id, '::', 4) AS billing_code_type_version,
    SPLIT_PART(segment_id, '::', 5) AS segment_idx,
    SPLIT_PART(segment_id, '::', 6) AS additional_attr,
    *
FROM in_network_rates_segment_header;

-- ============================
-- PART 7: External Table for Parsed Data
-- ============================

-- External table for staged negotiated arrangements
CREATE OR REPLACE EXTERNAL TABLE ext_negotiated_arrangments_staged(
    p_data_fl VARCHAR AS (SPLIT_PART(metadata$filename, '/', 2)),
    p_billing_code VARCHAR AS (SPLIT_PART(metadata$filename, '/', 3)),
    p_billing_code_type_and_version VARCHAR AS (SPLIT_PART(metadata$filename, '/', 4)),
    p_negotiation_arrangement VARCHAR AS (SPLIT_PART(metadata$filename, '/', 5)),
    p_segment_type VARCHAR AS (SPLIT_PART(metadata$filename, '/', 6))
)
PARTITION BY (p_data_fl, p_negotiation_arrangement, p_billing_code, p_billing_code_type_and_version, p_segment_type)
LOCATION = @ext_data_stg/raw_parsed/
REFRESH_ON_CREATE = FALSE 
AUTO_REFRESH = FALSE
FILE_FORMAT = (TYPE = PARQUET)
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- ============================
-- PART 8: Data Views
-- ============================

-- View of negotiated rates segments in stage
CREATE OR REPLACE VIEW negotiated_rates_segments_v
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
AS
SELECT 
    r.data_file AS data_file,
    p_data_fl AS data_fl_basename,
    value:SEQ_NO::INT AS segment_idx,
    p_negotiation_arrangement AS negotiation_arrangement,
    p_billing_code AS billing_code,
    p_billing_code_type_and_version AS billing_code_type_version_and_version,
    value:name::VARCHAR AS name,
    value:description::VARCHAR AS description,
    value:CHUNK_NO::INT AS chunk_no,
    ARRAY_SIZE(value:NEGOTIATED_RATES) AS chunk_size,
    value:NEGOTIATED_RATES AS negotiated_rates,
    value AS segment_chunk_raw
FROM ext_negotiated_arrangments_staged AS l
JOIN in_network_rates_file_header AS r
    ON l.p_data_fl = r.data_file_basename
WHERE p_segment_type = 'negotiated_rates';


create or replace view NEGOTIATED_RATES_SEGMENT_INFO_V(
	DATA_FILE,
	DATA_FL_BASENAME,
	SEGMENT_IDX,
	NEGOTIATION_ARRANGEMENT,
	BILLING_CODE,
	BILLING_CODE_TYPE_VERSION_AND_VERSION,
	NAME,
	DESCRIPTION,
	CHUNK_NO,
	CHUNK_SIZE,
	SEGMENT_CHUNK_RAW,
	NEGOTIATED_RATES_RECORD_INDEX,
	NEGOTIATED_PRICES,
	PROVIDER_GROUPS
) COMMENT='{\"origin\":\"sf_sit\",\"name\":\"pricing_transparency\",\"version\":{\"major\":1, \"minor\":0},\"attributes\":{\"component\":\"pricing_transparency\"}}'
 as
    select 
        t.* exclude(negotiated_rates)
        ,nr.index as negotiated_rates_record_index
        ,nr.value:negotiated_prices as negotiated_prices
        ,nr.value:provider_groups as provider_groups
    from negotiated_rates_segments_v as t
        , lateral flatten (input => t.negotiated_rates) as nr
    ;


create or replace view NEGOTIATED_PRICES_V(
	DATA_FILE,
	DATA_FL_BASENAME,
	SEGMENT_IDX,
	NEGOTIATION_ARRANGEMENT,
	BILLING_CODE,
	BILLING_CODE_TYPE_VERSION_AND_VERSION,
	NAME,
	DESCRIPTION,
	CHUNK_NO,
	CHUNK_SIZE,
	SEGMENT_CHUNK_RAW,
	NEGOTIATED_RATES_RECORD_INDEX,
	PROVIDER_GROUPS,
	NEGOTIATED_PRICES_RECORD_INDEX,
	BILLING_CLASS,
	EXPIRATION_DATE,
	NEGOTIATED_RATE,
	NEGOTIATED_TYPE,
	SERVICE_CODE
) COMMENT='{\"origin\":\"sf_sit\",\"name\":\"pricing_transparency\",\"version\":{\"major\":1, \"minor\":0},\"attributes\":{\"component\":\"pricing_transparency\"}}'
 as
select 
    b.* exclude(negotiated_prices)
    ,p.index as negotiated_prices_record_index
    ,p.value:billing_class::varchar as billing_class
    ,p.value:expiration_date::date as expiration_date
    ,p.value:negotiated_rate::double as negotiated_rate
    ,p.value:negotiated_type::varchar as negotiated_type
    ,p.value:service_code as service_code
from negotiated_rates_segment_info_v as b
    ,lateral flatten(input => b.negotiated_prices) as p
;

-- ============================
-- PART 9: Git Repository Integration and Upload Python Scripts
-- ============================

-- Create Git repository integration (requires ACCOUNTADMIN privileges)
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE API INTEGRATION GITHUB_PUBLIC
    API_PROVIDER = GIT_HTTPS_API
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED = TRUE
    COMMENT = 'API integration for GitHub public repositories';

-- -- Create Git repository
-- CREATE OR REPLACE GIT REPOSITORY PRICING_TRANSPARENCY_REPO
--     API_INTEGRATION = GITHUB_PUBLIC
--     ORIGIN = 'https://github.com/Snowflake-Labs/CMSgov-pricing-transparency.git';

-- Create Git repository
CREATE OR REPLACE GIT REPOSITORY PRICING_TRANSPARENCY_REPO
    API_INTEGRATION = GITHUB_PUBLIC
    ORIGIN = 'https://github.com/sfc-gh-awasserman/CMSgov-pricing-transparency';



-- Switch back to working role
USE ROLE PUBLIC;

-- Fetch latest changes from Git repository
ALTER GIT REPOSITORY PRICING_TRANSPARENCY_REPO FETCH;

-- Copy Python scripts from Git repository to internal stage
COPY FILES
INTO @lib_stg/scripts/
FROM @PRICING_TRANSPARENCY_REPO/branches/main/src/python/;

-- Create stored procedure for parsing file headers
CREATE OR REPLACE PROCEDURE parse_file_header(
    stage_path VARCHAR, 
    staged_data_flname VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'ijson', 'simplejson')
IMPORTS = ('@lib_stg/scripts/file_header.py', '@lib_stg/scripts/sp_commons.py')
HANDLER = 'file_header.main'
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- Create stored procedure for parsing negotiation arrangement segments
CREATE OR REPLACE PROCEDURE parse_negotiation_arrangement_segments(
    stage_path VARCHAR, 
    staged_data_flname VARCHAR, 
    target_stage_and_path VARCHAR,
    from_idx INTEGER, 
    to_idx INTEGER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'ijson', 'simplejson')
IMPORTS = ('@lib_stg/scripts/negotiation_arrangements.py', '@lib_stg/scripts/sp_commons.py')
HANDLER = 'negotiation_arrangements.main'
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- Create stored procedure for parsing negotiation arrangement headers
CREATE OR REPLACE PROCEDURE negotiation_arrangements_header(
    stage_path VARCHAR, 
    staged_data_flname VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'ijson', 'simplejson')
IMPORTS = ('@lib_stg/scripts/negotiation_arrangements_header.py', '@lib_stg/scripts/sp_commons.py')
HANDLER = 'negotiation_arrangements_header.main'
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- Create stored procedure for parsing provider references
CREATE OR REPLACE PROCEDURE provider_references(
    stage_path VARCHAR, 
    staged_data_flname VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'ijson', 'simplejson')
IMPORTS = ('@lib_stg/scripts/provider_references.py', '@lib_stg/scripts/sp_commons.py')
HANDLER = 'provider_references.main'
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- Create stored procedure for building DAGs
CREATE OR REPLACE PROCEDURE in_network_rates_dagbuilder_matrix(
    stage_path VARCHAR, 
    staged_data_flname VARCHAR, 
    target_stage_and_path VARCHAR,
    segments_per_task INTEGER, 
    warehouse_to_be_used VARCHAR, 
    dag_rows INTEGER, 
    dag_cols INTEGER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'ijson', 'simplejson')
IMPORTS = ('@lib_stg/scripts/in_network_rates_dagbuilder.py', '@lib_stg/scripts/sp_commons.py')
HANDLER = 'in_network_rates_dagbuilder.main_matrix'
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- Create stored procedure for deleting DAGs
CREATE OR REPLACE PROCEDURE delete_dag_for_datafile(
    staged_data_flname VARCHAR, 
    drop_task BOOLEAN
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
IMPORTS = ('@lib_stg/scripts/delete_dag_for_datafile.py', '@lib_stg/scripts/sp_commons.py')
HANDLER = 'delete_dag_for_datafile.main'
COMMENT = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}';

-- ============================
-- PART 10: Master Processing Procedure
-- ============================

-- Create the main processing procedure that does everything in one call
CREATE OR REPLACE PROCEDURE process_pricing_transparency_file(
    data_file_name VARCHAR,
    segments_per_task INTEGER DEFAULT 500,
    dag_rows INTEGER DEFAULT 5,
    dag_cols INTEGER DEFAULT 5,
    warehouse_size VARCHAR DEFAULT 'MEDIUM'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS $$
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import json

def main(session: Session, data_file_name: str, segments_per_task: int, dag_rows: int, dag_cols: int, warehouse_size: str):
    try:
        # Configuration
        input_stage = "data_stg/data"  # or "ext_data_stg/data" for external stage
        target_stage = "@ext_data_stg/raw_parsed"
        warehouses = "PRICING_TRANS_DEMO_TASK_0_WH,PRICING_TRANS_DEMO_TASK_1_WH,PRICING_TRANS_DEMO_TASK_2_WH,PRICING_TRANS_DEMO_TASK_3_WH,PRICING_TRANS_DEMO_TASK_4_WH"
        
        # Get file basename for task naming
        basename = data_file_name.split('.')[0].replace('-', '_').replace(' ', '_')
        cleansed_basename = ''.join(c if c.isalnum() or c == '_' else '_' for c in basename)
        
        # Step 1: Clean up any existing data for this file
        session.sql(f"DELETE FROM segment_task_execution_status WHERE data_file = '{data_file_name}'").collect()
        session.sql(f"DELETE FROM task_to_segmentids WHERE data_file = '{data_file_name}'").collect()
        session.sql(f"DELETE FROM in_network_rates_file_header WHERE data_file = '{data_file_name}'").collect()
        session.sql(f"DELETE FROM in_network_rates_segment_header WHERE data_file = '{data_file_name}'").collect()
        
        # Step 2: Set warehouse sizes
        for wh in warehouses.split(','):
            session.sql(f"ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = {warehouse_size}").collect()
            session.sql(f"ALTER WAREHOUSE {wh} SET MAX_CONCURRENCY_LEVEL = 8").collect()
        
        # Step 3: Clean up existing DAG if it exists
        try:
            session.call('delete_dag_for_datafile', cleansed_basename, False)
        except:
            pass  # Ignore if no existing DAG
        
        # Step 4: Build and execute the DAG
        result = session.call('in_network_rates_dagbuilder_matrix', 
                            input_stage, data_file_name, target_stage, 
                            segments_per_task, warehouses, dag_rows, dag_cols)
        
        if isinstance(result, str):
            result_dict = json.loads(result)
        else:
            result_dict = result
            
        root_task_name = result_dict.get('root_task')
        
        # Step 5: Execute the DAG
        if root_task_name:
            session.sql(f"EXECUTE TASK {root_task_name}").collect()
            
        # Step 6: Refresh external stage and table
        session.sql("ALTER STAGE ext_data_stg REFRESH").collect()
        session.sql("ALTER EXTERNAL TABLE ext_negotiated_arrangments_staged REFRESH").collect()
        
        return {
            "status": "success",
            "message": f"Started processing for file: {data_file_name}",
            "root_task": root_task_name,
            "dag_details": result_dict
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "file": data_file_name
        }
$$;

-- ============================
-- PART 11: Upload Sample Data (Optional)
-- ============================

-- Upload sample data files to internal stage
-- PUT file://./data/* @data_stg/data AUTO_COMPRESS = FALSE OVERWRITE = TRUE PARALLEL = 5;

-- ============================
-- SETUP COMPLETE
-- ============================

-- Display setup completion message
SELECT 'Setup completed successfully! You can now process pricing transparency files.' AS status;

-- Display usage instructions
SELECT '
USAGE INSTRUCTIONS:

1. Upload your pricing transparency data file to a stage:
   PUT file://path/to/your/datafile.json.gz @data_stg/data;

2. Process the file with one command:
   CALL process_pricing_transparency_file(''your_datafile.json.gz'');

3. Monitor progress:
   SELECT * FROM current_segment_parsing_tasks_v;
   SELECT * FROM file_ingestion_elapsed_v;

4. Query results:
   SELECT * FROM negotiated_rates_segments_v LIMIT 10;

OPTIONAL PARAMETERS:
- segments_per_task: Number of segments per parallel task (default: 500)
- dag_rows: DAG matrix rows for parallelism (default: 5) 
- dag_cols: DAG matrix columns for parallelism (default: 5)
- warehouse_size: Size of warehouses to use (default: MEDIUM)

EXAMPLE WITH PARAMETERS:
CALL process_pricing_transparency_file(
    ''large_file.json'', 
    1000,    -- segments_per_task
    10,      -- dag_rows  
    8,       -- dag_cols
    ''LARGE'' -- warehouse_size
);
' AS instructions;

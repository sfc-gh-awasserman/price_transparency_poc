// Run first, before poc_setup.sql // 
use role accountadmin;

// https://docs.snowflake.com/en/user-guide/data-load-azure-config 

// fill in AZURE_TENANT_ID and STORAGE_ALLOWED_LOCATIONS
CREATE STORAGE INTEGRATION azure_storage_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = ''
  STORAGE_ALLOWED_LOCATIONS = ('');


// fill in URL 
CREATE OR REPLACE STAGE ext_data_stg
    URL = ''
    STORAGE_INTEGRATION = azure_storage_integration
    DIRECTORY = (ENABLE = TRUE REFRESH_ON_CREATE = FALSE)
    COMMENT = 'External stage for pricing transparency data files';
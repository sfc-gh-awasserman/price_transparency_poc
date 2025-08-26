-- Lab 2: Data Transformation and Analysis
-- File: data_transformation.sql

-- Step 1: Initial Transformation and Preview
-- This view performs the first layer of transformation on the raw, nested JSON data
-- stored in the external table. We are previewing the data to understand its structure.
SELECT * FROM NEGOTIATED_PRICES_V 
LIMIT 100;

SELECT data_file, AVG(NEGOTIATED_RATE) 
FROM NEGOTIATED_PRICES_V 
WHERE BILLING_CODE = '36905'
group by data_file;

-- Step 2: Persist Transformed Data
-- We use a Create Table As Select (CTAS) statement to persist the semi-structured data 
-- from the view into a structured Snowflake table. This improves query performance for
-- subsequent analysis by materializing the results.
CREATE OR REPLACE TABLE NEGOTIATED_PRICES AS
SELECT * FROM NEGOTIATED_PRICES_V;

-- Step 3: Flatten Nested JSON for Analysis
-- This query demonstrates how to use the LATERAL FLATTEN function to de-normalize 
-- nested JSON arrays within the data. We are un-nesting the provider groups and then 
-- the NPI numbers within those groups to create a flat, relational view of the data,
-- which is much easier to join and analyze.
SELECT
    DATA_FILE,
    NEGOTIATION_ARRANGEMENT,
    BILLING_CODE,
    BILLING_CODE_TYPE_VERSION_AND_VERSION,
    NAME,
    DESCRIPTION,
    PROVIDER_GROUPS,
    BILLING_CLASS,
    EXPIRATION_DATE,
    NEGOTIATED_RATE,
    NEGOTIATED_TYPE,
    npi.value::string AS NPI, 
    pg.value:tin:type::string AS TIN_TYPE,
    pg.value:tin:value::string AS TIN_VALUE
FROM
    negotiated_prices t,
    -- First, flatten the main provider_groups array.
    -- This creates a new row for each object in the array.
    -- We alias the result as "pg" (for provider_group).
    LATERAL FLATTEN(input => t.provider_groups) pg,

    -- Second, flatten the "npi" array that lives inside each object from the first flatten.
    -- This creates a new row for each NPI value.
    -- We alias this result as "npi".
    LATERAL FLATTEN(input => pg.value:npi) npi;

-- Step 4: Create a Flattened Table for Easy Analysis
-- Now we'll persist the flattened data into its own table using another CTAS.
-- This final table is fully relational and optimized for BI tools and analysts
-- to query without needing to understand the underlying JSON structure.
CREATE OR REPLACE TABLE NEGOTIATED_PRICES_FLAT AS
SELECT
    DATA_FILE,
    NEGOTIATION_ARRANGEMENT,
    BILLING_CODE,
    BILLING_CODE_TYPE_VERSION_AND_VERSION,
    NAME,
    DESCRIPTION,
    BILLING_CLASS,
    EXPIRATION_DATE,
    NEGOTIATED_RATE,
    NEGOTIATED_TYPE,
    npi.value::string AS NPI,
    pg.value:tin:type::string AS TIN_TYPE,
    pg.value:tin:value::string AS TIN_VALUE
FROM
    negotiated_prices t,
    LATERAL FLATTEN(input => t.provider_groups) pg,
    LATERAL FLATTEN(input => pg.value:npi) npi;


-- Step 5: Analyze prices across billing codes and providers 

select * from negotiated_prices_flat where billing_code ='36905' and npi = '1518985415';

-- We can also use the negotiated_prices_v view to perform analytics as well. Our external table is clustered, so Snowflake efficiently can query this view as well 


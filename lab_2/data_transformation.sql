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
 FROM NEGOTIATED_PRICES_V;

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

-- Step 6: Below shows how we transform and flatten the BCBS data from this file https://app0004702110a5prdnc868.blob.core.windows.net/output/2025-07-18_Blue-Cross-and-Blue-Shield-of-Illinois_Blue-Options-or-Blue-Choice-Options_in-network-rates.json.gz


CREATE OR REPLACE TABLE FLATTENED_RATES
AS 
SELECT
    rates.name,
    rates.description,
    rates.billing_class,
    rates.billing_code,
    rates.billing_code_type,
    rates.negotiated_rate,
    rate_group.value AS provider_group_id,
    provider_npi.value AS npi
FROM
    HEALTH_PLAN_RATES AS rates,
    LATERAL FLATTEN(INPUT => rates.PROVIDER_REFERENCES) AS rate_group
JOIN
    HEALTH_PLAN_PROVIDERS AS p ON rate_group.value = p.PROVIDER_GROUP_ID,
    LATERAL FLATTEN(INPUT => p.npi) AS provider_npi;



select * from flattened_rates where provider_group_id = '121.1529831' and billing_code = 'L6584';

select avg(negotiated_rate) from flattened_rates where billing_code = 'L6584';

select * from flattened_rates where billing_code = 'L6584';


select 
    avg(negotiated_rate),
    provider_group_id,
    name,
    description
from 
    flattened_rates
where 
    billing_code = 'L6584'
group by 
    provider_group_id,
    name,
    description
order by avg(negotiated_rate);



select * from flattened_rates limit 100;
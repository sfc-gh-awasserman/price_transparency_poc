-- Lab 3: RBAC and Data Quality
-- File: RBAC.sql

use schema price_transparency_poc.rates;


-- Step 1: Clone our Analytical table

create table rbac_clone clone flattened_rates;

CREATE ROLE IF NOT EXISTS ANALYST_ROLE;

GRANT ROLE ANALYST_ROLE TO USER AWASSERMAN_SFC;

-- Step 2: Create Masking Policy

CREATE OR REPLACE MASKING POLICY DESCRIPTION_MASKING_POLICY AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() = 'ANALYST_ROLE' THEN val
    ELSE '**MASKED**'
  END;


-- Step 3: Apply the masking policy to the description column 

ALTER TABLE RBAC_CLONE
  MODIFY COLUMN DESCRIPTION SET MASKING POLICY DESCRIPTION_MASKING_POLICY;


use role analyst_role; 

use role public;

select * from rbac_clone limit 100;

-- Step 4: Data quality monitoring

SELECT SNOWFLAKE.CORE.NULL_COUNT(
  SELECT billing_code_type
  FROM flattened_rates);

SELECT SNOWFLAKE.CORE.NULL_COUNT(
  SELECT service_code
  FROM health_plan_rates);


  SELECT SNOWFLAKE.CORE.NULL_PERCENT(
  SELECT service_code
  FROM health_plan_rates);
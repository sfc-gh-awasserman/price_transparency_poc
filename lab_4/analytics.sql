-- Lab 4: Analytics
-- File: analytics.sql

-- SET CONTEXT 
USE SCHEMA OPENFLOW_DATA.ANALYTICS;


-- First, we will cluster the PROVIDERS table 
-- This takes 32 seconds on an xmall warehouse 

CREATE OR REPLACE TABLE PROVIDERS CLUSTER BY (NPI)
AS SELECT * FROM PROVIDERS;


-- Now we will cluster the RATES table. We will use a larger warehouse for this because this table has 1.2 billion rows. 
-- This takes 3 minutes 1 second on a LARGE warehouse

CREATE OR REPLACE TABLE RATES CLUSTER BY (BILLING_CODE)
AS SELECT * FROM RATES;


-- Now we will test the performance of analytical queries on these clustered tables 
-- This command below makes Snowflake re run each query and not use the result set cache, so we can accurately test queries

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT * 
FROM 
    RATES p 
INNER JOIN 
    PROVIDERS n 
ON 
    p.provider_group_id = n.provider_group_id 
WHERE 
    NPI
IN 
    ('1972523959', '1487971941') 
AND 
    BILLING_CODE = '35601';

-- compare prices across two NPIs for a billing code 
SELECT * 
FROM 
    RATES p 
INNER JOIN 
    PROVIDERS n 
ON 
    p.provider_group_id = n.provider_group_id 
WHERE 
    NPI
IN 
    ('1619178571', '1376622340') 
AND 
    BILLING_CODE = '4004F';

-- calculate average cost for a billing code across provider groups
select 
    provider_group_id,
    avg(negotiated_rate)
from rates
where 
    billing_code = '35606'
group by provider_group_id 
order by avg(negotiated_rate);


-- compare prices across reporting entity (BCBS vs BETR Health)
select 
    reporting_entity_name,
    avg(negotiated_rate)
from
    rates
where 
     billing_code = '35606'
group by 
    reporting_entity_name;
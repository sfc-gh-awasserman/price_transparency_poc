-- We want to split the service code array into individual rows for each item in the service_code array 

USE SCHEMA OPENFLOW_DATA.ANALYTICS;

-- Let's make a clone of the RATES table to operate on 

CREATE TABLE RATES_CLONE CLONE RATES;


-- Let's add a column to the RATES_CLONE table for the service code array 

ALTER TABLE RATES_CLONE ADD COLUMN SERVICE_CODE_ARRAY ARRAY;


-- We will use PARSE_JSON() to convert the string into an array 
-- LARGE warehouse 
-- 2 minutes 42 seconds 
UPDATE OPENFLOW_DATA.ANALYTICS.RATES_CLONE
SET SERVICE_CODE_ARRAY = PARSE_JSON(SERVICE_CODE)::ARRAY;


-- Now we can see the SERVICE_CODE_ARRAY field populated and stored as an array 
select * from rates_clone limit 10;


-- Now we will use lateral flatten to split the service_code_array
-- Large warehouse
-- 9m 51s to complete 

CREATE TABLE RATES_EXPANDED AS
SELECT 
    REPORTING_ENTITY_TYPE,
    REPORTING_ENTITY_NAME,
    NAME,
    DESCRIPTION,
    NEGOTIATED_TYPE,
    NEGOTIATED_RATE,
    NEGOTIATION_ARRANGEMENT,
    BILLING_CODE,
    BILLING_CODE_TYPE,
    BILLING_CLASS,
    SERVICE_CODE_ARRAY,
    EXPIRATION_DATE,
    PROVIDER_GROUP_ID,
    service_code.value::VARCHAR AS SERVICE_CODE
FROM
    OPENFLOW_DATA.ANALYTICS.RATES_CLONE as rates,
    LATERAL FLATTEN(INPUT => rates.SERVICE_CODE_ARRAY) AS service_code;



select * from rates_expanded limit 1000;


-- Finally we will cluster the table by BILLING_CODE to improve selective query performance 
-- Large Warehouse
-- 19 minutes 

CREATE OR REPLACE TABLE RATES_EXPANDED CLUSTER BY (BILLING_CODE)
AS SELECT * FROM RATES_EXPANDED;



select * from rates_expanded limit 1000;


-- Let's look at the analytical queries now 

SELECT * 
FROM 
    RATES_EXPANDED p 
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



SELECT * 
FROM 
    RATES_EXPANDED p 
INNER JOIN 
    PROVIDERS n 
ON 
    p.provider_group_id = n.provider_group_id 
WHERE 
    NPI
IN 
    ('1619178571', '1376622340') 
AND 
    BILLING_CODE = '4004F'
AND 
    SERVICE_CODE = '22';


select 
    provider_group_id,
    avg(negotiated_rate)
from RATES_EXPANDED
where 
    billing_code = '35606'
group by provider_group_id 
order by avg(negotiated_rate);
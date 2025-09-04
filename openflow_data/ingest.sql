// Loading in BETR Health and BCBS Data from Openflow processing //

-- 1. create rate table and provider table
-- 2. load betr health rates, betr health providers
-- 3. set BETR health value 
-- 4. load BCBS data 
-- 5. set BCBS value 
-- 6. do transformations

// set context 

use database openflow_data;

-- 1. create rate table and provider table

create TABLE HEALTH_PLAN_RATES (
    	REPORTING_ENTITY_TYPE VARCHAR(16777216),
    	REPORTING_ENTITY_NAME VARCHAR(16777216),
    	NAME VARCHAR(16777216),
    	DESCRIPTION VARCHAR(16777216),
    	NEGOTIATED_TYPE VARCHAR(16777216),
    	NEGOTIATED_RATE VARCHAR(16777216),
    	NEGOTIATION_ARRANGEMENT VARCHAR(16777216),
    	BILLING_CODE VARCHAR(16777216),
    	BILLING_CODE_TYPE VARCHAR(16777216),
    	BILLING_CLASS VARCHAR(16777216),
    	SERVICE_CODE VARCHAR(16777216),
    	EXPIRATION_DATE VARCHAR(16777216),
    	PROVIDER_REFERENCES VARIANT
);

create TABLE PROVIDERS (
	PROVIDER_GROUP_ID VARCHAR(16777216),
	TIN_TYPE VARCHAR(16777216),
	NPI ARRAY,
	TIN_VALUE VARCHAR(16777216)
);



-- 2. load betr health rates, betr health providers


COPY INTO HEALTH_PLAN_RATES
FROM @DATA_STG/betr_health/rates/
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


COPY INTO PROVIDERS
FROM @DATA_STG/betr_health/providers/
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

SELECT * FROM HEALTH_PLAN_RATES LIMIT 100;


-- 3. set BETR health value 

UPDATE HEALTH_PLAN_RATES
SET REPORTING_ENTITY_TYPE = 'HEALTH_INSURANCE_PAYER';

UPDATE HEALTH_PLAN_RATES 
SET REPORTING_ENTITY_NAME = 'BETR-HEALTH_ILLINOIS';


-- 4. load BCBS data 

COPY INTO HEALTH_PLAN_RATES
FROM @DATA_STG/bcbs/rates/
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO PROVIDERS
FROM @DATA_STG/bcbs/providers/
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

select count(*)
from health_plan_rates
where REPORTING_ENTITY_TYPE = '';

select count(*)
from health_plan_rates
where REPORTING_ENTITY_NAME = 'BETR-HEALTH_ILLINOIS';


-- 5. set BCBS value 

UPDATE OPENFLOW_DATA.PUBLIC.HEALTH_PLAN_RATES
SET REPORTING_ENTITY_TYPE = 'HEALTH_INSURANCE_PAYER'
WHERE REPORTING_ENTITY_TYPE = '';


UPDATE HEALTH_PLAN_RATES 
SET REPORTING_ENTITY_NAME = 'BCBS_ILLINOIS'
WHERE REPORTING_ENTITY_NAME = '';


-- 6. do transformations

-- 1. Create new ANALYTICS schema 
-- 2. Create RATES table
-- 3. Create PROVIDERS table
-- 4. Test analytics queries


-- 1. Create new ANALYTICS schema 

CREATE SCHEMA ANALYTICS;

USE SCHEMA OPENFLOW_DATA.ANALYTICS;

-- 2. Create RATES table

CREATE TABLE RATES AS
SELECT
    REPORTING_ENTITY_TYPE,
    REPORTING_ENTITY_NAME,
    NAME,
    DESCRIPTION,
    NEGOTIATED_TYPE,
    NEGOTIATED_RATE::NUMBER(10,2) AS NEGOTIATED_RATE,
    NEGOTIATION_ARRANGEMENT,
    BILLING_CODE,
    BILLING_CODE_TYPE,
    BILLING_CLASS,
    SERVICE_CODE,
    EXPIRATION_DATE::DATE AS EXPIRATION_DATE,
    rate_group.value::VARCHAR AS PROVIDER_GROUP_ID
FROM
    OPENFLOW_DATA.PUBLIC.health_plan_rates as rates,
    LATERAL FLATTEN(INPUT => rates.PROVIDER_REFERENCES) AS rate_group;



-- 3. Create PROVIDERS table

CREATE TABLE PROVIDERS AS 
SELECT 
    provider_npi.value AS npi,
    p.provider_group_id,
    p.tin_type,
    p.tin_value
FROM providers p,
LATERAL FLATTEN(INPUT => p.npi) AS provider_npi;


-- 4. Test analytics queries

select * 
from 
    rates p 
inner join 
    providers n 
on 
    p.provider_group_id = n.provider_group_id 
where 
    npi
in 
    ('1972523959', '1487971941') 
and 
    billing_code = '35606';


select 
    provider_group_id,
    avg(negotiated_rate)
from rates
where 
    billing_code = '35606'
group by provider_group_id 
order by avg(negotiated_rate);
-- Staging model for customers
-- Purpose: Clean and standardize raw customer data
-- Purpose: dbt staging model that cleans raw data. Key line: ref() function references the seed data.

select
    id as customer_id,
    name as customer_name,
    email as customer_email,
    signup_date::date as signup_date
from {{ ref('raw_customers') }}
-- Customer summary mart
-- Purpose: Create customer analytics summary
-- Purpose: dbt mart model for analytics. Key line: ref() function references the staging model.

select
    customer_id,
    customer_name,
    customer_email,
    signup_date,
    case 
        when signup_date >= '2023-04-01' then 'Recent'
        else 'Older'
    end as customer_segment,
    current_date as last_updated
from {{ ref('stg_customers') }}
order by signup_date desc
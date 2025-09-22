-- Simple staging model for hello world
{{ config(
    materialized='view'
) }}

select
    current_timestamp as created_at,
    'Hello from dbt!' as message,
    {{ var('run_date', 'CURRENT_DATE') }} as run_date
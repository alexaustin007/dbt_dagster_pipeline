
{{ config(materialized='view') }}
SELECT
    Store,
    Dept,
    Date,
    Weekly_Sales,
    IsHoliday
FROM {{ source('retail_analytics', 'raw_sales') }}

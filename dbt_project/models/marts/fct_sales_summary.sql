
{{ config(materialized='table') }}
SELECT * FROM {{ ref('int_daily_sales') }}

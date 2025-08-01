
{{ config(materialized='view') }}
WITH daily AS (
    SELECT
        Date as sale_date,
        SUM(Weekly_Sales) AS total_sales,
        COUNT(*) AS total_transactions
    FROM {{ ref('stg_sales') }}
    GROUP BY Date
)
SELECT * FROM daily

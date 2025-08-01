
{{ config(materialized='table') }}
WITH hourly AS (
    SELECT
        DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00') AS hour_bucket,
        SUM(price * quantity) AS sales,
        SUM(quantity) AS qty
    FROM retail.stream_sales_events
    GROUP BY 1
)
SELECT * FROM hourly

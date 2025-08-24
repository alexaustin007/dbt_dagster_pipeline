-- Real-time inventory alerts based on sales velocity
-- Identifies products with high sales rates that may need restocking

{{ config(materialized='table') }}

WITH product_velocity AS (
    SELECT 
        store_id,
        dept_id,
        product_id,
        COUNT(*) as transactions_last_hour,
        SUM(quantity) as units_sold_last_hour,
        SUM(total_amount) as revenue_last_hour,
        AVG(total_amount) as avg_transaction_value,
        MAX(event_time) as last_sale_time
    FROM {{ source('retail_analytics', 'stream_sales_events') }}
    WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
    AND transaction_type = 'SALE'
    GROUP BY store_id, dept_id, product_id
),

alert_thresholds AS (
    SELECT 
        *,
        CASE 
            WHEN units_sold_last_hour >= 20 THEN 'HIGH_VELOCITY'
            WHEN units_sold_last_hour >= 10 THEN 'MEDIUM_VELOCITY'
            WHEN units_sold_last_hour >= 5 THEN 'NORMAL_VELOCITY'
            ELSE 'LOW_VELOCITY'
        END as velocity_category,
        CASE 
            WHEN units_sold_last_hour >= 20 THEN 'URGENT_RESTOCK'
            WHEN units_sold_last_hour >= 10 THEN 'MONITOR_STOCK'
            ELSE 'NO_ACTION'
        END as alert_level
    FROM product_velocity
)

SELECT 
    store_id,
    dept_id,
    product_id,
    transactions_last_hour,
    units_sold_last_hour,
    revenue_last_hour,
    avg_transaction_value,
    velocity_category,
    alert_level,
    last_sale_time,
    CURRENT_TIMESTAMP as alert_generated_at
FROM alert_thresholds
WHERE alert_level IN ('URGENT_RESTOCK', 'MONITOR_STOCK')
ORDER BY 
    units_sold_last_hour DESC,
    revenue_last_hour DESC
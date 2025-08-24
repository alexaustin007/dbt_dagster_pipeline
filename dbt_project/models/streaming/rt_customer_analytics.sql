-- Real-time customer behavior analytics
-- Tracks customer purchase patterns and value metrics

{{ config(materialized='table') }}

WITH customer_metrics AS (
    SELECT 
        customer_id,
        store_id,
        COUNT(*) as transactions_today,
        SUM(total_amount) as total_spent_today,
        AVG(total_amount) as avg_transaction_value,
        SUM(quantity) as total_items_purchased,
        COUNT(DISTINCT dept_id) as departments_visited,
        COUNT(DISTINCT product_id) as unique_products_bought,
        SUM(CASE WHEN promotion_applied = 1 THEN 1 ELSE 0 END) as promotional_transactions,
        MIN(event_time) as first_transaction_today,
        MAX(event_time) as last_transaction_today,
        TIMESTAMPDIFF(MINUTE, MIN(event_time), MAX(event_time)) as session_duration_minutes
    FROM {{ source('retail_analytics', 'stream_sales_events') }}
    WHERE DATE(event_time) = CURRENT_DATE
    AND transaction_type = 'SALE'
    GROUP BY customer_id, store_id
),

customer_segments AS (
    SELECT 
        *,
        CASE 
            WHEN total_spent_today >= 500 THEN 'HIGH_VALUE'
            WHEN total_spent_today >= 200 THEN 'MEDIUM_VALUE'
            WHEN total_spent_today >= 50 THEN 'REGULAR_VALUE'
            ELSE 'LOW_VALUE'
        END as value_segment,
        CASE 
            WHEN transactions_today >= 5 THEN 'FREQUENT_BUYER'
            WHEN transactions_today >= 3 THEN 'REGULAR_BUYER'
            ELSE 'OCCASIONAL_BUYER'
        END as frequency_segment,
        CASE 
            WHEN departments_visited >= 5 THEN 'CROSS_SHOPPER'
            WHEN departments_visited >= 3 THEN 'MULTI_CATEGORY'
            ELSE 'FOCUSED_SHOPPER'
        END as shopping_behavior
    FROM customer_metrics
)

SELECT 
    customer_id,
    store_id,
    transactions_today,
    total_spent_today,
    avg_transaction_value,
    total_items_purchased,
    departments_visited,
    unique_products_bought,
    promotional_transactions,
    value_segment,
    frequency_segment,
    shopping_behavior,
    session_duration_minutes,
    first_transaction_today,
    last_transaction_today,
    CURRENT_TIMESTAMP as analysis_timestamp
FROM customer_segments
ORDER BY total_spent_today DESC, transactions_today DESC
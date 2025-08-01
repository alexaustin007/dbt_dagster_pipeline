{{ config(materialized='view') }}
WITH store_sales AS (
    SELECT
        s.Store,
        s.Date,
        s.Weekly_Sales,
        st.Type as Store_Type,
        st.Size as Store_Size,
        f.Temperature,
        f.Fuel_Price,
        f.IsHoliday
    FROM {{ ref('stg_sales') }} s
    LEFT JOIN {{ ref('stg_stores') }} st ON s.Store = st.Store
    LEFT JOIN {{ ref('stg_features') }} f ON s.Store = f.Store AND s.Date = f.Date
),
store_metrics AS (
    SELECT
        Store,
        Store_Type,
        Store_Size,
        COUNT(*) as total_weeks,
        AVG(Weekly_Sales) as avg_weekly_sales,
        SUM(Weekly_Sales) as total_sales,
        AVG(Temperature) as avg_temperature,
        AVG(Fuel_Price) as avg_fuel_price
    FROM store_sales
    GROUP BY Store, Store_Type, Store_Size
)
SELECT * FROM store_metrics 
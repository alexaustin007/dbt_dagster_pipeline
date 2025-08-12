{{ config(materialized='view') }}
SELECT
  Store,
  Date,
  Temperature,
  Fuel_Price,
  MarkDown1,
  MarkDown2,
  MarkDown3,
  MarkDown4,
  MarkDown5,
  CPI,
  Unemployment,
  IsHoliday
FROM {{ source('retail_analytics', 'raw_features') }}



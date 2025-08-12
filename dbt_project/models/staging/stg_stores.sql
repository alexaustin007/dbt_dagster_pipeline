{{ config(materialized='view') }}
SELECT
  Store,
  Type,
  Size
FROM {{ source('retail_analytics', 'raw_stores') }}



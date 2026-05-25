{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    user_id,
    product_id,
    event_type,
    CAST(rating AS FLOAT) AS rating,
    CASE
        WHEN rating >= 4.0 THEN 'positive'
        WHEN rating = 3.0  THEN 'neutral'
        ELSE 'negative'
    END AS sentiment,
    category,
    CAST(verified_purchase AS BOOLEAN) AS verified_purchase,
    CAST(helpful_vote AS INT) AS helpful_vote,
    event_datetime,
    data_source,
    ingestion_ts
FROM {{ source('bronze', 'reviews_raw') }}
WHERE user_id IS NOT NULL
  AND rating IS NOT NULL
  AND rating BETWEEN 1.0 AND 5.0
  AND product_id IS NOT NULL
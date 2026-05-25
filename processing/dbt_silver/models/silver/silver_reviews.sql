{{ config(
    materialized='table',
    schema='silver'
) }}

WITH base AS (
    SELECT
        user_id,
        product_id,
        event_type,
        CAST(rating AS FLOAT)            AS rating,
        CASE
            WHEN rating >= 4.0 THEN 'positive'
            WHEN rating = 3.0  THEN 'neutral'
            ELSE 'negative'
        END                              AS sentiment,
        category,
        CAST(verified_purchase AS BOOLEAN) AS verified_purchase,
        CAST(helpful_vote AS INT)          AS helpful_vote,
        CAST(event_datetime AS TIMESTAMP)  AS event_datetime,
        YEAR(event_datetime)               AS review_year,
        MONTH(event_datetime)              AS review_month,
        DAY(event_datetime)                AS review_day,
        DAYOFWEEK(event_datetime)          AS day_of_week,
        data_source,
        ingestion_ts
    FROM {{ source('bronze', 'reviews_raw') }}
    WHERE user_id IS NOT NULL
      AND rating IS NOT NULL
      AND rating BETWEEN 1.0 AND 5.0
      AND product_id IS NOT NULL
),

product_stats AS (
    SELECT
        product_id,
        ROUND(AVG(rating), 2)  AS avg_product_rating,
        COUNT(*)               AS total_reviews_product
    FROM base
    GROUP BY product_id
)

SELECT
    b.*,
    p.avg_product_rating,
    p.total_reviews_product
FROM base b
LEFT JOIN product_stats p ON b.product_id = p.product_id
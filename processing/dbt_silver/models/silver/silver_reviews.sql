
{{ config(
    materialized='table',
    schema='silver'
) }}

WITH base AS (
    SELECT
        event_id,                                          -- ✅ FIX: incluir event_id (requerido por unique + relationships)
        user_id,
        product_id,
        COALESCE(event_type, 'review')          AS event_type,       -- ✅ FIX: default para NULLs
        CAST(rating AS FLOAT)                   AS rating,
        CASE
            WHEN rating >= 4.0 THEN 'positive'
            WHEN rating  = 3.0 THEN 'neutral'
            ELSE 'negative'
        END                                     AS sentiment,
        category,
        CAST(verified_purchase AS BOOLEAN)      AS verified_purchase,
        CAST(helpful_vote AS INT)               AS helpful_vote,
        CAST(event_datetime AS TIMESTAMP)       AS event_datetime,
        YEAR(event_datetime)                    AS review_year,
        MONTH(event_datetime)                   AS review_month,
        DAY(event_datetime)                     AS review_day,
        DAYOFWEEK(event_datetime)               AS day_of_week,
        COALESCE(data_source, 'amazon_reviews_2023') AS data_source, -- ✅ FIX: default para NULLs
        COALESCE(ingestion_ts, CAST(current_timestamp() AS STRING)) AS ingestion_ts -- ✅ FIX: default para NULLs
    FROM {{ source('bronze', 'reviews_raw') }}
    WHERE user_id    IS NOT NULL
      AND rating     IS NOT NULL
      AND rating     BETWEEN 1.0 AND 5.0
      AND product_id IS NOT NULL
      AND event_id   IS NOT NULL                          -- ✅ garantiza que unique/relationships funcionen
),

deduplicated AS (
    -- ✅ FIX: deduplicar por event_id para que el test unique pase
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_datetime DESC) AS _rn
        FROM base
    )
    WHERE _rn = 1
),

product_stats AS (
    SELECT
        product_id,
        ROUND(AVG(rating), 2) AS avg_product_rating,
        COUNT(*)              AS total_reviews_product
    FROM deduplicated
    GROUP BY product_id
)

SELECT
    d.event_id,
    d.user_id,
    d.product_id,
    d.event_type,
    d.rating,
    d.sentiment,
    d.category,
    d.verified_purchase,
    d.helpful_vote,
    d.event_datetime,
    d.review_year,
    d.review_month,
    d.review_day,
    d.day_of_week,
    d.data_source,
    d.ingestion_ts,
    p.avg_product_rating,
    p.total_reviews_product
FROM deduplicated d
LEFT JOIN product_stats p ON d.product_id = p.product_id
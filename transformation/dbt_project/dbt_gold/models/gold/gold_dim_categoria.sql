{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    category                                                        AS categoria,
    COUNT(*)                                                        AS total_resenas,
    COUNT(DISTINCT product_id)                                      AS total_productos,
    COUNT(DISTINCT user_id)                                         AS total_usuarios,
    ROUND(AVG(rating), 2)                                           AS avg_rating,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END)        AS resenas_positivas,
    SUM(CASE WHEN sentiment = 'neutral'  THEN 1 ELSE 0 END)        AS resenas_neutrales,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)        AS resenas_negativas,
    ROUND(
        SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                               AS pct_positivas
FROM {{ source('silver', 'reviews_clean') }}
WHERE category IS NOT NULL
GROUP BY category
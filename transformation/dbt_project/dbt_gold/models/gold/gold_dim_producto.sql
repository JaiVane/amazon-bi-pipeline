{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    product_id,
    category,
    ROUND(AVG(rating), 2)          AS avg_rating_producto,
    COUNT(*)                        AS total_resenas,
    SUM(helpful_vote)               AS total_votos_utiles,
    ROUND(AVG(helpful_vote), 2)     AS avg_votos_utiles,
    MIN(event_datetime)             AS primera_resena,
    MAX(event_datetime)             AS ultima_resena
FROM {{ source('silver', 'reviews_clean') }}
WHERE product_id IS NOT NULL
GROUP BY product_id, category
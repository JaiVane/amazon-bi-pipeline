{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    user_id,
    COUNT(*)                                                        AS total_resenas,
    ROUND(AVG(rating), 2)                                           AS avg_rating_dado,
    SUM(CASE WHEN verified_purchase THEN 1 ELSE 0 END)              AS compras_verificadas,
    ROUND(
        SUM(CASE WHEN verified_purchase THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                               AS pct_verificadas,
    MIN(event_datetime)                                             AS primera_resena,
    MAX(event_datetime)                                             AS ultima_resena,
    COUNT(DISTINCT category)                                        AS categorias_distintas
FROM {{ source('silver', 'reviews_clean') }}
WHERE user_id IS NOT NULL
GROUP BY user_id
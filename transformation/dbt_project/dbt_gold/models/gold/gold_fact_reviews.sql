{{ config(
    materialized='table',
    schema='gold',
    partition_by = 'category'
) }}

SELECT
    -- Clave generada porque Silver no trae event_id
    SHA2(CONCAT_WS('|', user_id, product_id, CAST(event_datetime AS STRING), CAST(rating AS STRING)), 256) AS event_id,

    user_id,
    product_id,
    CAST(DATE_FORMAT(event_datetime, 'yyyyMMdd') AS INT) AS fecha_key,
    category,

    rating,
    helpful_vote,
    CAST(verified_purchase AS INT) AS verified_purchase_flag,

    sentiment,
    event_type,
    YEAR(event_datetime) AS anio,
    MONTH(event_datetime) AS mes,
    DAY(event_datetime) AS dia,
    DAYOFWEEK(event_datetime) AS dia_semana,

    event_datetime,
    data_source,
    ingestion_ts
FROM {{ source('silver', 'reviews_clean') }}
WHERE user_id IS NOT NULL
  AND product_id IS NOT NULL
  AND event_datetime IS NOT NULL
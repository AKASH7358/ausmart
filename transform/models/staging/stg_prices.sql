WITH source AS (
    SELECT * FROM ausmart.raw_prices
),

deduplicated AS (
    SELECT
        price_id, 
        product_id,
        price,
        old_price,
        discount_pct,
        currency,
        retailer,
        fetched_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY fetched_at DESC
        ) AS row_num 
    FROM source
)

SELECT
    price_id, 
    product_id,
    price,
    old_price,
    discount_pct,
    currency,
    retailer,
    fetched_at
FROM deduplicated
WHERE row_num = 1
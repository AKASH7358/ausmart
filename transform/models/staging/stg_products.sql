WITH source AS (
    SELECT * FROM ausmart.raw_products
),

deduplicated AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        currency,
        source,
        fetched_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY fetched_at DESC
        ) AS row_num
    FROM source
)

SELECT 
    product_id,
    product_name,
    category,
    price,
    currency,
    source AS retailer,
    fetched_at
FROM deduplicated
WHERE row_num = 1
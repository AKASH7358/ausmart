WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

prices AS (
    SELECT * FROM {{ ref('stg_prices') }}
),

joined AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.retailer,
        p.price AS current_price,
        pr.old_price,
        pr.discount_pct,
        pr.currency,

        CASE 
            WHEN pr.discount_pct >= 15 THEN 'High Discount'
            WHEN pr.discount_pct >= 5 THEN 'Moderate Discount'
            ELSE 'Full Price'
        END AS price_status,

        CASE 
            WHEN pr.price < 50 THEN 'Budget'
            WHEN pr.price < 200 THEN 'Mid-range'
            ELSE 'Premium'
        END AS price_tier,

        pr.old_price - pr.price AS savings_aud,
        p.fetched_at 

    FROM products p 
    LEFT JOIN prices pr ON p.product_id = pr.product_id
)

SELECT * FROM joined
{{ config(materialized='table') }}

WITH daily_reviews AS (
    SELECT
        DATE(review_date) AS review_date,
        COUNT(*) AS review_count
    FROM {{ ref('stg_allBeauty_reviewsProducts') }}
    GROUP BY DATE(review_date)
)

SELECT
    review_date,
    review_count
FROM daily_reviews
ORDER BY review_date

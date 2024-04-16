{{ config(materialized='table') }}

select
    asin,
    COUNT(*) as total_reviews,
    AVG(user_rating) as average_rating,
    SUM(helpful_vote) as total_helpful_votes,
    COUNTIF(verified_purchase) as count_verified_purchases,
    COUNTIF(sentiment = 'Positive') as positive_reviews,
    COUNTIF(sentiment = 'Negative') as negative_reviews
from {{ ref('stg_beauty_reviews') }}
group by asin

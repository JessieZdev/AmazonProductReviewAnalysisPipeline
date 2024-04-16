{{ config(
   materialized="view",
   schema="staging"
) }}

with source as (
    select * from {{source('staging', 'All_Beautyreview_product_table')}}
), renamed AS (
   SELECT
        user_rating,
        review_title,
        review_text,
        asin,
        parent_asin,
        user_id,
        TIMESTAMP_MILLIS(timestamp) as review_date,
        CAST(verified_purchase AS BOOLEAN) as is_verified_purchase,
        helpful_vote,
        average_rating,
        main_category,
        price,
        rating_number,
        store,
        item_title,
        row_number() over (partition by asin order by timestamp) as review_id, 
        
   FROM source
)

select
    *, 
    {{ calculate_sentiment('user_rating') }} as sentiment, 
    current_date as etl_loaded_date
from renamed

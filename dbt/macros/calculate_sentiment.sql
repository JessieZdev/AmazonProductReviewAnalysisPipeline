{% macro calculate_sentiment(rating_column) %}
    CASE
        WHEN {{ rating_column }} >= 4 THEN 'Positive'
        WHEN {{ rating_column }} <= 2 THEN 'Negative'
        ELSE 'Neutral'
    END
{% endmacro %}

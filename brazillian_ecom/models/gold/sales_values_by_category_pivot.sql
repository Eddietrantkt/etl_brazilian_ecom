WITH src AS (
    SELECT *
    FROM {{ ref('sales_values_by_category') }}
)

SELECT
    category,
    {{ dbt_utils.pivot(
            'monthly',
            dbt_utils.get_column_values(ref('sales_values_by_category'),'monthly'),
            then_value = 'total_bills',
            quote_identifiers=False)}}
FROM src
GROUP BY category
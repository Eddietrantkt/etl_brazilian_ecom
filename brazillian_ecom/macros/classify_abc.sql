SELECT
    *,
    {{ classify_abc("total_bills") }} AS class
FROM {{ ref('sales_values_by_category') }}

SELECT *
FROM {{ source('raw', 'olist_order_payments_dataset') }}

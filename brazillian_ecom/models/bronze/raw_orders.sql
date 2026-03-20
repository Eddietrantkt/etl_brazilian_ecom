SELECT *
FROM {{ source('raw', 'olist_orders_dataset') }}

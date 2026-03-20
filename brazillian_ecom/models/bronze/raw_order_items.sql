SELECT *
FROM {{ source('raw', 'olist_order_items_dataset') }}

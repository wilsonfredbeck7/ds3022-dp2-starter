-- One clean row per fragment. DISTINCT collapses a redelivered message.
select distinct
    cast(order_no as integer) as order_no,   -- arrives as a string
    trim(word)                as word
from {{ source('raw', 'fragments') }}

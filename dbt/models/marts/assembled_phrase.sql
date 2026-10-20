select
    count(*)                                as fragment_count,
    string_agg(word, ' ' order by order_no) as phrase
from {{ ref('stg_fragments') }}

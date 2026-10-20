-- Returns a row (= the test fails) unless there are exactly
-- 21 fragments and no gaps in order_no.
select count(*) as n, min(order_no) as lo, max(order_no) as hi
from {{ ref('stg_fragments') }}
having count(*) <> {{ var('expected_fragments', 21) }}
    or max(order_no) - min(order_no) + 1 <> count(*)

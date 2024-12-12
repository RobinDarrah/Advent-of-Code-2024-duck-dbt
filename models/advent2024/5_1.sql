with page_orders as (
SELECT *
, row_number() over(partition by update_) page_order
FROM {{ ref('5_input_pages') }}),

good_updates as (
SELECT page_orders.update_, bool_and(case when page_orders.page_order > b.page_order then False else True end) good
FROM page_orders
LEFT JOIN {{ ref('5_input_rules') }} 
using(page)
LEFT JOIN page_orders b
ON page_before = b.page and page_orders.update_ = b.update_
group by 1
),

middle_num as (
SELECT array_agg(page order by page_order) pages_array
, pages_array[((len(pages_array)+1)/2)::int] middle
FROM page_orders
WHERE update_ in (SELECT update_ FROM good_updates where good)
group by update_
)

SELECT sum(middle) answer
FROM middle_num
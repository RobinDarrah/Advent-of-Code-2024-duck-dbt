with rowned as (
SELECT col0, row_number() over(order by col0) rown_0
, col1, row_number() over(order by col1) rown_1
FROM {{ ref('1_input') }}
)

SELECT SUM(ABS(c0.col0-c1.col1)) answer
FROM rowned c0
join rowned c1 on c0.rown_0 = c1.rown_1
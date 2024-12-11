with counts as (
SELECT l.col0, count(*) counts
FROM {{ ref('1_input') }} l
JOIN {{ ref('1_input') }} r
ON l.col0 = r.col1
group by 1)

SELECT sum(col0 * counts) answer
FROM counts
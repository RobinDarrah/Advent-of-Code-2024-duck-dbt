with unnested as (
SELECT report, unnest(levels) level_
FROM {{ ref('2_input') }}
),

rules as (
SELECT report
, LAG(level_) over(partition by report) prev_level_
, level_
, case when ABS(COALESCE(prev_level_,level_+1)-level_) between 1 and 3 then True else False end one_to_three
, case when COALESCE(prev_level_,level_-1) < level_ then True else False end increasing
, case when COALESCE(prev_level_,level_+1) > level_ then True else False end decreasing
from unnested
),

reports as (
SELECT bool_and(one_to_three) and (bool_and(increasing) or bool_and(decreasing)) safe
FROM rules
group by report)

SELECT count(*) answer
FROM reports
WHERE safe
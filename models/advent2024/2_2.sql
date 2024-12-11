WITH unnested as (
SELECT report, unnest(levels) level_
FROM {{ ref('2_input') }}
),

level_numbers as (
SELECT report
, level_
, row_number() over(partition by report) leveln
from unnested
),

report_version_numbers as (
SELECT distinct leveln report_version_n
FROM level_numbers
),

rules as (
SELECT report
, report_version_n
, leveln
, LAG(level_) over(partition by report,report_version_n order by leveln) prev_level_
, level_
, case when ABS(COALESCE(prev_level_,level_+1)-level_) between 1 and 3 then True else False end one_to_three
, case when COALESCE(prev_level_,level_-1) < level_ then True else False end increasing
, case when COALESCE(prev_level_,level_+1) > level_ then True else False end decreasing
FROM level_numbers
JOIN report_version_numbers ON report_version_n != leveln
),

reports as (
SELECT report
, report_version_n
, bool_and(one_to_three) and (bool_and(increasing) or bool_and(decreasing)) safe
FROM rules
group by report, report_version_n),

reports_2 as (
SELECT bool_or(safe) safe
FROM reports
group by report)

SELECT count(*) answer
FROM reports_2
where safe
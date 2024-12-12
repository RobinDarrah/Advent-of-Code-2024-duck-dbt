with rowns as (
SELECT *, row_number() over() rown
FROM {{ ref('4_input') }}
),

unnested as (
SELECT rown
, unnest(split(col1,'')) letter
FROM rowns),

cols_and_rows as (
SELECT rown
, row_number() OVER(partition by rown) coln
, letter
FROM unnested
),

col_nums as (
SELECT distinct coln
FROM cols_and_rows
),

row_nums as (
SELECT distinct rown
FROM cols_and_rows
),

col_row_union as (
SELECT coln
FROM col_nums
UNION ALL
SELECT rown
FROM row_nums
),

col_and_row_nums as (
SELECT row_number() over() num
FROM col_row_union),

col_diags as ( --south west
SELECT listagg(letter,'' ORDER BY cols_and_rows.rown) line
FROM cols_and_rows
cross join col_nums
WHERE cols_and_rows.rown + col_nums.coln - 1 = cols_and_rows.coln
group by col_nums.coln),


row_diags as ( --south west
SELECT listagg(letter,'' ORDER BY cols_and_rows.rown) line
FROM cols_and_rows
cross join row_nums
WHERE cols_and_rows.rown = cols_and_rows.coln + row_nums.rown - 1 
and row_nums.rown != 1
group by row_nums.rown),

diags_2 as ( --north east
SELECT listagg(letter,'' ORDER BY cols_and_rows.rown) line
FROM cols_and_rows
cross join col_and_row_nums
WHERE cols_and_rows.rown + cols_and_rows.coln = col_and_row_nums.num
group by col_and_row_nums.num),

lines as (
SELECT line
FROM col_diags

UNION ALL

SELECT line
FROM row_diags

UNION ALL

SELECT line
FROM diags_2

UNION ALL

SELECT col1 --(untouched rows)
FROM "4_input"

UNION ALL

SELECT listagg(letter,'' ORDER BY rown)
FROM cols_and_rows
GROUP BY coln)

SELECT sum(LEN(regexp_extract_all(line,'XMAS')) + LEN(regexp_extract_all(line,'SAMX'))) answer
FROM lines
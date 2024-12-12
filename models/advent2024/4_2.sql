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
)

SELECT *
FROM cols_and_rows mid
JOIN cols_and_rows top_left ON mid.rown - 1 = top_left.rown AND mid.coln - 1 = top_left.coln
JOIN cols_and_rows top_right ON mid.rown - 1 = top_right.rown AND mid.coln + 1 = top_right.coln
JOIN cols_and_rows bottom_left ON mid.rown + 1 = bottom_left.rown AND mid.coln - 1 = bottom_left.coln
JOIN cols_and_rows bottom_right ON mid.rown + 1 = bottom_right.rown AND mid.coln + 1 = bottom_right.coln
where mid.letter = 'A' 
and top_left.letter||bottom_right.letter in ('MS','SM') 
and top_right.letter||bottom_left.letter in ('MS','SM')
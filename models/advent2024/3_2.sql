with regexed as (
SELECT unnest(regexp_split_to_array(input_string,'do\(\)')) as do_and_dont
, regexp_split_to_array(do_and_dont,'don''t\(\)')[1] do_
, regexp_extract_all(do_,'mul\([0-9]{1,3},[0-9]{1,3}\)') muls
FROM {{ ref('3_input') }}),

multed as (
SELECT unnest(muls) mul
, regexp_extract_all(mul,'[0-9]+') mul_items
, mul_items[1]::INT * mul_items[2]::INT multed
FROM regexed)

SELECT sum(multed) answer
FROM multed
with regexed as (
SELECT unnest(regexp_extract_all(input_string,'mul\([0-9]{1,3},[0-9]{1,3}\)')) as mul
, regexp_extract_all(mul,'[0-9]+') mul_items
, mul_items[1]::INT * mul_items[2]::INT multed
FROM {{ ref('3_input') }})

SELECT sum(multed) answer
FROM regexed
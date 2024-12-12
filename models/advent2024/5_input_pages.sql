SELECT unnest(split(col1,','))::int page
, row_number() over() update_
FROM {{ ref('5_input') }}
WHERE col1 like '%,%'
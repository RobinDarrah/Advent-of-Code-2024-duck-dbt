SELECT split_part(col1,'|',1)::int page
, split_part(col1,'|',2)::int page_before
FROM {{ ref('5_input') }}
WHERE col1 like '%|%'
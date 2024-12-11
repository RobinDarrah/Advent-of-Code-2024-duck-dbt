with array_table as (
SELECT split(col1, '   ')::int[] col_array
FROM read_csv('inputs/1',
    delim = '|',
    header = false,
    columns = {
        'col1': 'VARCHAR',
    })
)

SELECT col_array[1] col0
, col_array[2] col1
FROM array_table
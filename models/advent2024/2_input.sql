SELECT split(col1, ' ')::int[] levels
, row_number() over() report
FROM read_csv('inputs/2',
    delim = '|',
    header = false,
    columns = {
        'col1': 'VARCHAR',
    })
SELECT col1
FROM read_csv('inputs/5',
    delim = '.', 
    header = false,
    columns = {
        'col1': 'VARCHAR',
    })
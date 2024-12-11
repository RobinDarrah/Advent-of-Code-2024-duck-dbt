SELECT col1
FROM read_csv('inputs/4',
    delim = '|', --risk here, what if input contains | ??
    header = false,
    columns = {
        'col1': 'VARCHAR',
    })
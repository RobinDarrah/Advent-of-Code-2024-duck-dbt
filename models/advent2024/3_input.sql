SELECT listagg(col1) input_string
FROM read_csv('inputs/3',
    delim = '|', --risk here, what if input contains | ??
    header = false,
    columns = {
        'col1': 'VARCHAR',
    })
const returnColumns = (columns) => {
    return ['id', 'created_at', 'updated_at', ...columns.map(e => e.column_name)]
}
const sql_type_mapper = {
    number: [
        'integer', 'bigInteger',
        'decimal', 'float',
    ],
    string: [
        'string', 'uuid', 'date'
    ]
}

module.exports = { returnColumns, sql_type_mapper }

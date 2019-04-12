const returnColumns = (columns) => {
  return ['id', 'created_at', 'updated_at', ...columns.map(e => e.column_name)]
}

function sanitizeData(value, column) {
  if (['date', 'datetime'].includes(column.type)) {
    return value ? new Date(value).toISOString() : null
  }
  return value
}

const sql_type_mapper = {
  number: [
      'integer', 'bigInteger',
      'decimal', 'float',
  ],
  string: [
      'string', 'uuid', 'date', 'datetime'
  ],
  boolean: [
    'boolean'
  ]
}

module.exports = { returnColumns, sql_type_mapper, sanitizeData }

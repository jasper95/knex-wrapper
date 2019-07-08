const returnColumns = (columns) => {
  return ['id', 'created_date', 'updated_date', ...columns.map(e => e.column_name)]
}

function sanitizeData(value, column) {
  if (['date', 'datetime'].includes(column.type)) {
    return value ? new Date(value).toISOString() : null
  }
  if (['json', 'jsonb'].includes(column.type)) {
    return JSON.stringify(value)
  }
  return value
}

const sql_type_mapper = {
  number: [
      'integer', 'bigInteger',
      'decimal', 'float',
  ],
  string: [
      'string', 'uuid', 'date', 'datetime', 'timestamp'
  ],
  boolean: [
    'boolean'
  ],
  object: [
    'json', 'jsonb'
  ]
}

module.exports = { returnColumns, sql_type_mapper, sanitizeData }

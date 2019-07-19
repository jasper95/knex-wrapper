const {
    sql_type_mapper,
    sanitizeData,
    returnColumns
} = require('./utility')
const { pick, get } = require('lodash')
const joi = require('joi')

function validateAndFormat(data, columns, action) {
  if (['delete', 'update', 'upsert'].includes(action) && !data.id) {
    throw { success: false, message: 'id is required' }
  }
  if (['insert', 'upsert'].includes(action)) {
    validateKeysExists(
      columns
      .filter(e => e.required)
      .map(e => e.column_name),
      data
    )
  }

  if (['update', 'upsert'].includes(action) && !data.updated_date) {
    data.updated_date = new Date().toISOString()
  }

  const fields = returnColumns(columns)
  data = pick(data, fields)
  if (action === 'delete') {
    return data
  }
  return Object.entries(data)
    .reduce((acc, [key, value]) => {
      const column = columns.find(e => e.column_name === key)
      if (column && column.type === 'uuid' && !value) {
        acc[key] = null
      } else if (!['id', 'created_date', 'updated_date'].includes(key)) {
        const val_type = typeof value
        const types = sql_type_mapper[val_type]
        if (!types) {
            throw { success: false, message: `Type ${typeof value} not supported` }
        } else if(!types.includes(column.type) && ![null, undefined].includes(value)) {
            throw { success: false, message: `Column ${column.column_name} type mismatch. Expected ${column.type} found ${val_type}`}
        }
        acc[key] = sanitizeData(value, column)
      } else {
        acc[key] = value
      }
      return acc
    }, {})
}

function getObjectValidator(columns) {
    const object = columns.reduce((result, column) => {
        const { name, required, validations = {} } = column
        let [type] = Object.entries(sql_type_mapper).find(([key, vals]) => {
            return vals.includes(column.type)
        }) || []
        if (type) {
            let validate = joi[type]()
            validate = Object.entries(validations).reduce((res, [key, val]) => {
                if (typeof val === 'boolean') {
                    res = res[key]()
                } else {
                    res = res[key](val)
                }
                return res
            }, validate)
            if (required) {
                validate = validate.required()
            }
            result[name] = { [name]: validate }
        }
        return result
    },{})
    return joi.object.keys(object).required()
}

function validateParams(schema, table, data, action) {
    let columns = validateTableColumns(schema, table)
    let is_array = false
    if(Array.isArray(data)) {
        is_array = true
        if (data.length)
            data = data.map(e => validateAndFormat(e, columns, action))
        else
            throw { success: false, message: 'Data is Empty' }
    }
    else
        data = validateAndFormat(data, columns, action)
    return { data, is_array, columns }
}

function validateTableColumns(schema, table) {
    const table_exists = schema.tables.find(e => e.table_name === table)
    if (!table_exists)
        throw { success: false, message: `Table ${table} does not exists` }
    const { columns } = table_exists
    if (columns.length === 0)
      throw { success: false, message: `Columns for Table ${table} is empty` }
    return columns
}

function validateKeysExists(keys, data) {
    const provided_keys = Object.keys(data)
    return keys
        .every(key => {
            if (!provided_keys.includes(key) || !get(data, key)) {
                throw { success: false, message: `${key} is required` }
            }
            return true
        })
}

module.exports = {
    getObjectValidator,
    validateParams,
    validateKeysExists,
    validateTableColumns
}
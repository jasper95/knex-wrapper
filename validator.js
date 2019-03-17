const {
    sql_type_mapper
} = require('./utility')
const joi = require('joi')

function validateCreate(data, columns, upsert) {
    data =  { ...data }
    validateKeysExists(
        columns.filter(e => e.required)
            .map(e => e.column_name),
        data
    )
    Object.entries(data)
        .forEach(([key, value]) => {
            const column = columns.find(e => e.column_name === key)
            if (column) {
                const val_type = typeof value
                const types = sql_type_mapper[val_type]
                if (!types) {
                    throw { success: false, message: `Type ${typeof value} not supported` }
                } else if(!types.includes(column.type)) {
                    throw { success: false, message: `Column ${column.column_name} type mismatch. Expected ${column.type} found ${val_type}`}
                }
            } else {
                if((!upsert && key !== 'id') || !upsert)
                    delete data[key]
            }
        })
    return data
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

function validateUpdate(data) {
    if (!data.id)
        throw { message: 'id is required', success: false }
    const { created_at, updated_at, ...rest } = data
    return rest
}

function validateDelete(data) {
    if (!data.id)
        throw { message: 'id is required', success: false }
    return data.id
}

function validateParams(schema, table, data, validator, upsert = false) {
    let columns = validateTableColumns(schema, table)
    let is_array = false
    if(Array.isArray(data)) {
        is_array = true
        if (data.length)
            data = data.map(e => validator(e, columns))
        else
            throw { success: false, message: 'Data is Empty' }
    }
    else
        data = validator(data, columns, upsert)
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
            if (
                (!provided_keys.includes(key))
                || (typeof data[key] === 'string' && !data[key])
            ) {
                throw { success: false, message: `${key} is required` }
            }
            return true
        })
}

module.exports = {
    validateCreate,
    getObjectValidator,
    validateUpdate,
    validateDelete,
    validateParams,
    validateKeysExists,
    validateTableColumns
}
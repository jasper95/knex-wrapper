const {
    sql_type_mapper
} = require('./utility')

class Validator {
    static validateCreate(data, columns) {
        data =  { ...data }
        const col_names = columns.map(e => e.column_name)
        Object.entries(data)
            .forEach(([key, value]) => {
                const column = columns.find(e => e.column_name === key)
                if (column) {
                    const val_type = typeof value
                    const types = sql_type_mapper[val_type]
                    if (!types) {
                        console.log('@validation error')
                        throw { success: false, message: `Type ${typeof value} not supported` }
                    } else if(!types.includes(column.type)) {
                        throw { success: false, message: `Column ${column.name} type mismatch. Expected ${column.type} found ${val_type}`}
                    }
                } else {
                    delete data[key]
                }
            })
        console.log('@validated')
        return data
    }

    static validateUpdate() {

    }

    static validateTableColumns(schema, table) {
        console.log('@im here')
        const table_exists = schema.tables.find(e => e.table_name === table)
        if (!table_exists)
            throw { success: false, message: `Table ${table} does not exists` }
        const { columns } = table_exists
        if (columns.length === 0)
          throw { success: false, message: `Columns for Table ${table} is empty` }
        console.log('@validate column')
        return columns
    }
}

module.exports = Validator
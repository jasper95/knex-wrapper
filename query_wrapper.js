const Validator = require('./validator')
const { returnColumns } = require('./utility')

class QueryWrapper {
    constructor(schema, knex) {
        this.schema = schema
        this.knex = knex
    }

    list(table, fields = [], sort_column, order = 'asc') {
        return this.knex.select(...fields).from(table)
    }

    async create(table, data) {
        let columns = Validator.validateTableColumns(this.schema, table)
        if(Array.isArray(data)) {
            if (data.length)
                data.forEach(e => Validator.validateCreate(e, columns))
            else
                throw { success: false, message: 'Data is Empty' }
        }
        else
            Validator.validateCreate(data, columns)

        return this
            .knex(table)
            .returning(returnColumns(columns))
            .insert(data)
    }
}

module.exports = QueryWrapper
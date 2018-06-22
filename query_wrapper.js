const Validator = require('./validator')
const { returnColumns } = require('./utility')

class QueryWrapper {
    constructor(schema, knex) {
        this.schema = schema
        this.knex = knex
        this.create = this.create.bind(this)
        // this.create = this.list.bind(this)
    }

    list(table, fields = [], sort_column, order = 'asc') {
        return this.knex.select(...fields).from(table)
    }

    async create(table, data) {
        let columns = Validator.validateTableColumns(this.schema, table)
        if(Array.isArray(data)) {
            if (data.length)
                data = data.map(e => Validator.validateCreate(e, columns))
            else
                throw { success: false, message: 'Data is Empty' }
        }
        else
            data = Validator.validateCreate(data, columns)

        return this
            .knex(table)
            .returning(returnColumns(columns))
            .insert(data)
    }
}

module.exports = QueryWrapper
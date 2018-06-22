const Validator = require('./validator')
const { returnColumns } = require('./utility')

class QueryWrapper {
    constructor(schema, knex) {
        this.schema = schema
        this.knex = knex
        this.insert = this.insert.bind(this)
        this.filter = this.filter.bind(this)
        this.updateById = this.updateById.bind(this)
        this.updateById = this.updateById.bind(this)
        this.deleteById = this.deleteById.bind(this)
        this.deleteByFilter = this.deleteByFilter.bind(this)
    }

    filter(table, filter = {}, fields = [], sort = [{ column: 'created_at', direction: 'asc'}]) {
        let query = this.knex(table)
               .select(...fields)
               .where(filter)
       return sort.reduce((q, sortEl) => {
           return q.orderBy(sortEl.column, sortEl.direction)
       }, query)
   }

    insert(table, data) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
            .validateParams(
                this.schema, table, data, Validator.validateCreate.bind(Validator))
            )
        return this
            .knex(table)
            .returning(returnColumns(columns))
            .insert(data)
            .then(response => is_array ? response : response[0])
    }

    updateById(table, data) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
            .validateParams(
                this.schema, table, data, Validator.validateUpdate.bind(Validator))
            )
        const update = (e) =>
            this.knex
                .table(table)
                .where({ id: e.id})
                .returning(returnColumns(columns))
                .update(e)
                .then(([res]) => res)

        return is_array ? Promise.map(data, update) : update(data)
    }

    updateByFilter(table, data, filter = {}) {
        const columns = Validator.validateTableColumns(this.schema, table)
        return this.knex(table)
            .where(filter)
            .returning(returnColumns(columns))
            .update(data)
    }

    deleteById(table, data) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
            .validateParams(this.schema, table, data, Validator.validateDelete.bind(Validator)))
        let query = this.knex(table)
        if(is_array) {
            query = query
                .where(builder => {
                    builder.whereIn('id', data)
                })
        } else {
            query = query
                .where({ id: data })
        }
        return query
            .returning('id')
            .delete()
            .then((res) => is_array ? res : res[0])
    }

    deleteByFilter(table, filter = {}) {
        return this.knex(table)
            .where(filter)
            .returning('id')
            .delete()
    }
}

module.exports = QueryWrapper
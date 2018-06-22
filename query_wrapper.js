const Validator = require('./validator')
const { returnColumns } = require('./utility')
const util = require('util')

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
        this.upser = this.upsert.bind(this)
    }

    withTransaction(query) {
        return this.knex.transaction((trx) => {
            return query
                .then(trx.commit)
                .catch(trx.rollback)
        })
    }

    filter(table, filter = {}, fields = [], sort = [{ column: 'created_at', direction: 'asc'}]) {
        let query = this.knex(table)
               .select(...fields)
               .where(filter)
       return sort.reduce((q, sortEl) => {
           return q.orderBy(sortEl.column, sortEl.direction)
       }, query)
   }

    insert(table, data, options = { batch_size: 1000 }) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
            .validateParams(
                this.schema, table, data, Validator.validateCreate.bind(Validator))
            )
        if (is_array) {
            return this.withTransaction(
                this.knex
                    .batchInsert(table, data, options.batch_size)
                    .returning(returnColumns(columns))
            )
        }
        return this
            .knex(table)
            .returning(returnColumns(columns))
            .insert(data)
            .then(response => response[0])
    }

    upsert(table, data) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
                .validateParams(
                    this.schema, table, data, Validator.validateCreate.bind(Validator), true
                )
            )
        const upsertData = (e) => {
            let insert = this.knex(table).insert({...e})
            delete e.id
            let update = this.knex(table).returning(returnColumns(columns)).update(e)
            let query = util.format('%s on conflict (id) do update set %s',
              insert.toString(), update.toString().replace(/^update ([`"])[^\1]+\1 set/i, ''))
            return this.knex.raw(query)
                .then(res => res.rows[0])
        }
        if (is_array) {
            return this.withTransaction(
                Promise.map(data, upsertData)
            )
        }
        return upsertData(data)
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

        if (is_array) {
            return this.withTransaction(
                Promise.map(data, update)
            )
        }
        return update(data)
    }

    updateByFilter(table, data, filter = {}) {
        const columns = Validator.validateTableColumns(this.schema, table)
        return this.withTransaction(
            this.knex(table)
            .where(filter)
            .returning(returnColumns(columns))
            .update(data)
        )
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
        return this.withTransaction(
            query
                .returning('id')
                .delete()
                .then((res) => is_array ? res : res[0])
        )
    }

    deleteByFilter(table, filter = {}) {
        return this.withTransaction(
            this.knex(table)
                .where(filter)
                .returning('id')
                .delete()
        )
    }
}

module.exports = QueryWrapper
const Validator = require('./validator')
const { returnColumns } = require('./utility')
const util = require('util')
const _ = require('lodash')

class QueryWrapper {
    constructor(schema, knex, config) {
        this.schema = schema
        this.knex_lib = knex
        this.knex = knex(config)
        this.config = config
    }

    _checkDatabase() {
        return this.knex.raw('select 1+1 as result')
            .then(() => true)
            .catch(() => false)
    }

    _listTables() {
        return this.knex
            .raw(`SELECT * FROM pg_catalog.pg_tables WHERE schemaname='public'`)
            .then(res => res.rows)
    }

    _listIndices(table) {
        return this.knex
            .raw(`select * from pg_indexes where tablename = '${table}'`)
            .then(res => res.rows)
    }

    _listForeignKeys(table) {
        return this.knex
            .raw(`select * from information_schema.table_constraints where table_name = '${table}' AND constraint_type = 'FOREIGN KEY'`)
            .then(res => res.rows)
    }

    _listColumns(table) {
        return this.knex
            .table(table).columnInfo()
            .then(res => Object.keys(res))
    }

    async _createOrDropDatabase(action, database) {
        await this.knex.destroy() // destroy temporarily
        delete this.config.connection.database
        this.knex = this.knex_lib(this.config) //
        return this.knex
            .raw(action.toLowerCase())
            .then(() => {
                this.config.connection.database = database
                this.knex = this.knex_lib(this.config)
                return true
            })
            .catch(() => false)
    }

    async createDatabase(database) {
        return this._createOrDropDatabase('CREATE DATABASE ' + database, database)
    }

    createTable(table) {
        return this.knex.schema
            .createTable(table, (t) => {
                t.uuid('id').defaultTo(knex.raw('uuid_generate_v4()')).primary()
                t.timestamps(true, true)
            })
    }

    async createColumns(table, columns) {
        const initColumn = (col, t) => {
            const {
                type, type_params = [],
                unique, column_name, default: defaultTo,
                required = false, unsigned = false, index,
            } = col
            let query = t[type](column_name, ...[type_params])

            if (required) {
                query = query.notNullable()
            } else {
                query = query.nullable()
            }
            if (defaultTo || defaultTo === '' || defaultTo === 0) {
                query = query.defaultTo(defaultTo)
            }
            if (unsigned) {
                query = query.unsigned()
            }
            if (unique) {
                query = query.unique()
            }
            if (index) {
                query = query.index()
            }
        }
        await this.knex.schema.alterTable(table, (t) => {
            columns.forEach(e => initColumn(e, t))
        })
        return Promise.mapSeries(
            columns.filter(e => e.foreign_key),
            ({ column_name, ...col}) => this.createForeignKey(table, { column: column_name, ...col })
        )
    }

    createIndex(table, column) {
        return this.knex.schema.alterTable(table, (t) => {
            t.index([column])
          })
    }

    createUnique(table, column) {
        return this.knex.schema.alterTable(table, (t) => {
            t.unique(column)
          })
    }

    createForeignKey(table, { column, on_update, on_delete, reference_table, reference_column }) {
        return this.knex.schema.table(table, (t) => {
            t.foreign(column)
              .references(reference_column)
              .inTable(reference_table)
              .onUpdate(on_update || 'NO ACTION')
              .onDelete(on_delete || 'NO ACTION')
          })
    }

    async dropDatabase(database) {
        return this._createOrDropDatabase('DROP DATABASE IF EXISTS ' + database, database)
    }

    dropTable(table) {
        return this.knex.schema.dropTable(table)
    }

    dropColumns(table, columns) {
        return this.knex.schema.table(table, (t) => {
            t.dropColumn(columns)
        })
    }

    dropIndex(table, column) {
        return this.knex.schema.alterTable(table, (t) => {
            t.dropIndex(column)
        })
    }

    dropUnique(table, column) {
        return this.knex.schema.alterTable(table, (t) => {
            t.dropUnique(column)
          })
    }

    async dropForeignKey(table, column) {
        await this.knex.schema.table(table, (t) => {
            t.dropForeign(column)
          })
        return this.knex.schema.table(table, (t) => {
            t.dropIndex([], `${table}_${column}_foreign`.toLowerCase())
        })
    }

    _withTransaction(query) {
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
                this.schema, table, data, Validator.validateCreate)
            )
        if (is_array) {
            return this._withTransaction(
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
                    this.schema, table, data, Validator.validateCreate, true
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
            return this._withTransaction(
                Promise.map(data, upsertData)
            )
        }
        return upsertData(data)
    }

    updateById(table, data) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
            .validateParams(
                this.schema, table, data, Validator.validateUpdate)
            )
        const update = (e) =>
            this.knex
                .table(table)
                .where({ id: e.id})
                .returning(returnColumns(columns))
                .update(_.pick(e, columns))
                .then(([res]) => res)

        if (is_array) {
            return this._withTransaction(
                Promise.map(data, update)
            )
        }
        return update(data)
    }

    updateByFilter(table, data, filter = {}) {
        const columns = Validator.validateTableColumns(this.schema, table)
        return this._withTransaction(
            this.knex(table)
            .where(filter)
            .returning(returnColumns(columns))
            .update(data)
        )
    }

    deleteById(table, data) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
            .validateParams(this.schema, table, data, Validator.validateDelete))
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
        return this._withTransaction(
            query
                .returning('id')
                .delete()
                .then((res) => is_array ? res : res[0])
        )
    }

    deleteByFilter(table, filter = {}) {
        return this._withTransaction(
            this.knex(table)
                .where(filter)
                .returning('id')
                .delete()
        )
    }
}

module.exports = QueryWrapper
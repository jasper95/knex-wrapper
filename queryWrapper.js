const Validator = require('./validator')
const { returnColumns } = require('./utility')
const util = require('util')
const _ = require('lodash')
const Promise = require('bluebird')
const knex = require('knex')

class QueryWrapper {
    constructor(schema, config) {
        this.schema = schema
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
            .raw(`SELECT tablename = t.name FROM sys.tables t`)
    }

    _listIndices(table) {
        return this.knex.raw(`
            SELECT
                indexname = ind.name
                FROM 
                    sys.indexes ind
                INNER JOIN 
                    sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id 
                INNER JOIN 
                    sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id 
                INNER JOIN 
                    sys.tables t ON ind.object_id = t.object_id 
                WHERE 
                    ind.is_primary_key = 0 
                    AND ind.is_unique = 0 
                    AND ind.is_unique_constraint = 0 
                    AND t.is_ms_shipped = 0
                    AND t.name = '${table}'
        `)
    }

    _listForeignKeys(table) {
        return this.knex
            .raw(`
                select
                    constraint_name = tc.CONSTRAINT_NAME
                from
                    information_schema.table_constraints tc
                where
                    table_name = '${table}'
                    AND constraint_type = 'FOREIGN KEY'
        `)
    }

    _listColumns(table) {
        return this.knex
            .table(table).columnInfo()
            .then(res => Object.keys(res))
    }

    async _createOrDropDatabase(action) {
        await this.knex.destroy()
        const { database } = this.config.connection
        this.config.connection.database = 'tempdb'
        this.knex = knex({
            ...this.config,
            pool: { min: 0, max: 1 }
        })
        await this.knex
            .raw(action.toLowerCase())
            .catch(() => false)
        await this.knex.destroy()
        this.config.connection.database = database
        this.knex = knex(this.config)
        return true
    }

    async createDatabase(database) {
        return this._createOrDropDatabase('CREATE DATABASE ' + database, database)
    }

    createTable(table) {
        return this.knex.schema
            .createTable(table, (t) => {
                t.uuid('id').defaultTo(this.knex.raw('newid()')).primary()
                t.timestamp('created_date', { precision: 6, useTz: true }).defaultTo(this.knex.fn.now())
                t.timestamp('updated_date', { precision: 6, useTz: true }).defaultTo(this.knex.fn.now())
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

    find(table, id, fields = [], key_filter = 'id') {
        return this.knex(table)
            .select(...fields)
            .where({ [key_filter]: id })
            .first()
    }

    filter(table, filter = {}, options = {}) {
        const pagination = options.pagination || {}
        const sort= options.sort || [{ column: 'created_date', direction: 'asc'}]
        const fields = options.fields || []
        const search = options.search || { fields: [], value: '' }
        const { page, size } = pagination
        let query = this.knex(table)
            .where(filter)
        const { value: search_value = '', fields: search_fields = [] } = search || {}
        if (search_value && search_fields.length) {
            query = query.andWhere((builder) => {
                return search_fields
                .filter(e => e !== 'id')
                .reduce((q, field) => {
                    return q.orWhereRaw(`LOWER(${field}) LIKE '%${search_value.toLowerCase()}%'`, )
                }, builder)
            })
        }
        if (![page, size].includes(undefined)) {
            const count = query.clone()
                .count({ count: '*' })
                .then((response) => response[0].count)
            query = sort.reduce((q, sortEl) => q.orderBy(sortEl.column, sortEl.direction), query)
            query = query
                .offset(Number(page) * Number(size))
                .limit(Number(size))
                .select(...fields)

            return Promise.props({
                data: query,
                count
            })
        }
        return sort.reduce((q, sortEl) => q.orderBy(sortEl.column, sortEl.direction),
            query.select(...fields))
   }

    insert(table, data, options = { batch_size: 1000 }) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
            .validateParams(
                this.schema, table, data, 'insert')
            )
        const fields = returnColumns(columns)
        if (is_array) {
            return this.knex
                .batchInsert(table, data, options.batch_size)
                .returning(fields)
        }
        return this
            ._insert(table, data, fields)
    }

    upsert(table, data) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
                .validateParams(
                    this.schema, table, data, Validator.validateCreate, 'upsert'
                )
            )
        const fields = returnColumns(columns)
        const upsertData = (e) => {
            if (!e.id) return this._insert(table, e, fields)
            return this
            ._update(table, e, fields)
            .then((response) => {
                if (!response)
                    return this._insert(table, e, fields)
                return response
            })
        }
        if (is_array) {
            return Promise.map(data, upsertData)
        }
        return upsertData(data)
    }

    updateById(table, data) {
        let is_array, columns;
        ({ data, is_array, columns} = Validator
            .validateParams(
                this.schema, table, data, 'update')
            )
        const fields = returnColumns(columns)
        if (is_array) {
            return Promise.map(data, e => this._update(table, e, fields))
        }
        return this._update(table, data, fields)
    }

    _insert(table, data, fields) {
        return this
        .knex(table)
        .returning(fields)
        .insert(data)
        .then(([response2]) => response2)
    }

    _update(table, data, fields) {
        return this.knex
            .table(table)
            .where({ id: data.id })
            .update(_.pick(data, fields), fields)
            .then(([res]) => res)
    }

    updateByFilter(table, data, filter = {}) {
        const columns = Validator.validateTableColumns(this.schema, table)
        return this.knex(table)
        .where(filter)
        .update(data, returnColumns(columns))
    }

    deleteById(table, data) {
        let is_array;
        ({ data, is_array} = Validator
            .validateParams(this.schema, table, data, 'delete'))
        let query = this.knex(table)
        if(is_array) {
            query = query
                .where(builder => {
                    builder.whereIn('id', data.map(e => e.id))
                })
        } else {
            query = query
                .where(_.pick(data, 'id'))
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
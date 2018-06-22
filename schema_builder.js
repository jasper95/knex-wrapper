const Promise = require('bluebird')
const _ = require('lodash')

class SchemaBuilder {
  constructor(schema, knex) {
    this.schema = schema
    this.knex = knex
  }

  async setupSchema() {
    const createTables = (tables) => Promise.mapSeries(tables, this.createTable.bind(this))
    await this.knex.raw('create extension if not exists "uuid-ossp"')
    return createTables(this.schema.tables)
  }

  async createTable(table) {
    const { table_name, columns, indices = [] } = table
    const { knex } = this
    const initColumn = (col, t, update = false) => {
      const {
        type, type_params = [],
        unique, column_name, default: defaultTo = '',
        required = false, unsigned = false,
        foreign_key } = col
      let query = t[type](column_name, ...[type_params])

      if (required) {
        query = query.notNullable()
      } else if (defaultTo) {
        query = query.defaultTo(defaultTo)
      } else {
        query = query.nullable()
      }
      if (unsigned) {
        query = query.unsigned()
      }
      if (unique && !update) {
        query = query.unique(column_name)
      }
      if (foreign_key && !update) {
        const {
          reference_column, reference_table,
          on_update, on_delete
        } = col
        t.foreign(column_name)
          .references(reference_column)
          .inTable(reference_table)
          .onUpdate(on_update || 'NO ACTION')
          .onDelete(on_delete || 'NO ACTION')
      }
      if(update)
        query.alter()
    }

    const hasTable = await knex.schema.hasTable(table_name)
    let new_columns = columns
    if (hasTable) {
      new_columns = await Promise.filter(columns, async(col) => !(await knex.schema.hasColumn(table_name, col.column_name)))
      await Promise.map(columns, col => this.updateIndex(table_name, col, indices))
      await Promise.map(columns, col => this.updateUnique(table_name, col))
      await Promise.map(columns, col => this.updateForeignKey(table_name, col))
    }

    if (!hasTable) {
      await knex
        .schema
        .createTable(table_name, (t) => {
          t.uuid('id').defaultTo(knex.raw('uuid_generate_v4()')).primary()
          table.timestamp('created_at').defaultTo(knex.fn.now());
          table.timestamp('updated_at').defaultTo(knex.raw('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'));
        })
    }
    await knex.schema.alterTable(table_name, (t) => {
      new_columns
        .map(col => initColumn(col, t))
      _.differenceBy(columns, new_columns, 'column_name')
        .map(col => initColumn(col, t, true))
    })
  }

  async updateForeignKey(table_name, { column_name, foreign_key, on_update, on_delete, reference_table, reference_column }) {
    const { knex } = this
    await knex.schema.table(table_name, (t) => {
      t.dropForeign(column_name)
    }).catch(err => {})
    await knex.schema.table(table_name, (t) => {
      t.dropIndex([], `${table_name}_${column_name}_foreign`.toLowerCase())
    }).catch(err => {})
    if (foreign_key) {
      await knex.schema.table(table_name, (t) => {
        t.foreign(column_name)
          .references(reference_column)
          .inTable(reference_table)
          .onUpdate(on_update || 'NO ACTION')
          .onDelete(on_delete || 'NO ACTION')
      }).catch(err => {})
    }
  }

  async updateUnique(table_name, { column_name, unique }) {
    const { knex } = this
    await knex.schema.alterTable(table_name, (t) => {
      t.dropUnique(column_name)
    }).catch(err => {})
    if (unique) {
      await knex.schema.alterTable(table_name, (t) => {
        t.unique(column_name)
      }).catch(err => {})
    }
  }
  async updateIndex(table_name, { column_name }, indices) {
    const { knex } = this
    await knex.schema.alterTable(table_name, (t) => {
      t.dropIndex(column_name)
    }).catch(err => {})
    if (indices.includes(column_name)) {
      await knex.schema.alterTable(table_name, (t) => {
        t.index([column_name])
      }).catch(err => { })
    }
  }
}

module.exports = SchemaBuilder
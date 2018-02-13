const config = require('./config')
const db_schema = require('./sample')
const knex = require('knex')(config)
const Promise = require('bluebird')
const _ = require('lodash')

class SchemaBuilder {
  constructor(schema) {
    this.schema = schema
  }

  setupSchema() {
    const createTables = (tables) => Promise.mapSeries(tables, this.createTable.bind(this))
    return createTables(this.schema.tables)
  }

  async createTable(table) {
    const { table_name, columns, indices = [] } = table

    const initColumn = (col, t, update = false) => {
      const {
        type, type_params = [],
        unique, column_name,
        required = true, unsigned = false,
        foreign_key } = col
      let query = t[type](column_name, ...[type_params])
      if (required) {
        query = query.notNullable()
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
    const updateForeignKey = async(
      table_name,
      { column_name, foreign_key, on_update, on_delete, reference_table, reference_column }
    ) => {
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

    const updateUnique = async(table_name, { column_name, unique }) => {
      await knex.schema.alterTable(table_name, (t) => {
        t.dropUnique(column_name)
      }).catch(err => {
      })
      if (unique) {
        await knex.schema.alterTable(table_name, (t) => {
          t.unique(column_name).alter()
        }).catch(err => {})
      }
    }

    const updateIndex = async(table_name, { column_name }, indices) => {
      await knex.schema.alterTable(table_name, (t) => {
        t.dropIndex(column_name)
      }).catch(err => {})
      if (indices.includes(column_name)) {
        await knex.schema.alterTable(table_name, (t) => {
          t.index([column_name])
        }).catch(err => {})
      }
    }
    const hasTable = await knex.schema.hasTable(table_name)
    let new_columns = columns
    if (hasTable) {
      new_columns = await Promise.filter(columns, async(col) => !(await knex.schema.hasColumn(table_name, col.column_name)))
      await Promise.map(columns, col => updateIndex(table_name, col, indices))
      await Promise.map(columns, col => updateUnique(table_name, col))
      await Promise.map(columns, col => updateForeignKey(table_name, col))
    }

    await knex
            .schema
            .createTableIfNotExists(table_name, (t) => {
              t.increments('id')
              new_columns
                .map(col => initColumn(col, t))
              t.timestamps()
            })
    await knex.schema.alterTable(table_name, (t) => {
      _.differenceBy(columns, new_columns, 'column_name')
        .map(col => initColumn(col, t, true))
    })
  }
}

(async() => {
  const schema_builder = new SchemaBuilder(db_schema)
  await schema_builder.setupSchema()
})()
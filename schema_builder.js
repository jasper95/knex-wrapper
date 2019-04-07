const Promise = require('bluebird')
const _ = require('lodash')

const default_columns = ['id', 'created_at', 'updated_at']

class SchemaBuilder {
  constructor(schema, query_wrapper) {
    this.schema = schema
    this.query_wrapper = query_wrapper
  }

  async setupSchema() {
    const hasDatabase = await this.query_wrapper._checkDatabase()
    if (!hasDatabase)
      await this.query_wrapper.createDatabase(this.query_wrapper.config.connection.database)
    const setupTables = (tables) => Promise.mapSeries(tables, e => this.setupTable(e))
    const dropTables = (tables) => Promise.map(tables, e => this.query_wrapper.dropTable(e))
    const { knex } = this.query_wrapper
    await Promise.all([
      knex.raw('create extension if not exists "uuid-ossp"'),
      knex.raw(
        `CREATE OR REPLACE FUNCTION notify_trigger() RETURNS trigger AS $$
          DECLARE
            old_val json;
            new_val json;
          BEGIN
            IF TG_OP = 'INSERT' THEN
              new_val = row_to_json(NEW);
              old_val = null;
            ELSIF TG_OP = 'UPDATE' THEN
              old_val = row_to_json(OLD);
              new_val = row_to_json(NEW);
            ELSE
              old_val = row_to_json(OLD);
              new_val = null;
              END IF;
            PERFORM pg_notify('watchers', json_build_object('table', TG_TABLE_NAME, 'old_val', old_val, 'new_val', new_val, 'type', TG_OP)::text);
            RETURN new;
          END;
        $$ LANGUAGE plpgsql;`
      )
    ])
    const current_tables = (await this.query_wrapper._listTables()).map(e => e.tablename)
    const table_names = this.schema.tables.map(e => e.table_name)
    const dropped_tables = current_tables.filter(e => !table_names.includes(e))
    await this.dropTriggers(current_tables)
    await Promise.all([setupTables(this.schema.tables), dropTables(dropped_tables)])
    await this.initTriggers(table_names)
  }

  initTriggers(tables) {
    const { knex } = this.query_wrapper
    return Promise
      .map(tables, table => knex.raw(`CREATE TRIGGER watched_table_trigger AFTER INSERT ON "${table}" FOR EACH ROW EXECUTE PROCEDURE notify_trigger();`))
  }

  dropTriggers(tables) {
    const { knex } = this.query_wrapper
    return Promise
      .map(tables, table => knex.raw(`DROP TRIGGER IF EXISTS watched_table_trigger ON "${table}"`))
  }

  async setupTable(table) {
    const { table_name, columns } = table
    const { knex } = this.query_wrapper
    const syncColumn = (col, t) => {
      const {
        type, type_params = [],
        column_name, default: defaultTo,
        required = false, unsigned = false} = col
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
      query.alter()
    }

    const hasTable = await knex.schema.hasTable(table_name)
    let new_columns = columns
    let table_columns = []
    const old_columns = _.differenceBy(columns, new_columns, 'column_name')
    if (hasTable) {
      ([table_columns, new_columns] = await Promise.all([
        this.query_wrapper._listColumns(table_name),
        Promise.filter(columns, async(col) => !(await knex.schema.hasColumn(table_name, col.column_name)))
      ]))
      const col_names = columns.map(e => e.column_name)
      const dropped_columns = table_columns.filter(e => !col_names.includes(e) && !default_columns.includes(e))
      if (dropped_columns.length)
        await this.query_wrapper.dropColumns(table_name, dropped_columns)
      let current_indices = (await this.query_wrapper._listIndices(table_name))
          .map(e => e.indexname)
      let current_fks = (await this.query_wrapper._listForeignKeys(table_name))
          .map(e => e.constraint_name)
      await Promise.map(old_columns, col => this.updateIndex(table_name, col, current_indices))
      await Promise.map(old_columns, col => this.updateUnique(table_name, col, current_indices))
      await Promise.map(old_columns, col => this.updateForeignKey(table_name, col, current_fks))
    } else {
      await knex
        .schema
        .createTable(table_name, (t) => {
          t.uuid('id').defaultTo(knex.raw('uuid_generate_v4()')).primary()
          t.timestamps(true, true)
        })
    }
    if (new_columns.length){
      await this.query_wrapper.createColumns(table_name, new_columns)
    }
    await knex.schema.alterTable(table_name, (t) => {
      old_columns.map(col => syncColumn(col, t))
    })
  }

  async updateForeignKey(table_name, { column_name, foreign_key, on_update, on_delete, reference_table, reference_column }, current_fks) {
    const indexname = `${table_name}_${column_name}_foreign`.toLowerCase()
    if (current_fks.includes(indexname) && !foreign_key) {
      return this.query_wrapper.dropForeignKey(table_name, column_name)
    } else if (!current_fks.includes(indexname) && foreign_key) {
      return this.query_wrapper
        .createForeignKey(table_name, { column: column_name, on_update, on_delete, reference_table, reference_column })
    }
    return
  }

  async updateUnique(table_name, { column_name, unique }, current_indices) {
    const indexname = `${table_name}_${column_name}_unique`.toLowerCase()
    if (current_indices.includes(indexname) && !unique) {
      return this.query_wrapper.dropUnique(table_name, column_name)
    } else if (!current_indices.includes(indexname) && unique) {
      return this.query_wrapper.createUnique(table_name, column_name)
    }
    return
  }

  async updateIndex(table_name, { column_name, index },  current_indices) {
    const indexname = `${table_name}_${column_name}_index`.toLowerCase()
    if (current_indices.includes(indexname) && !index) {
      return this.query_wrapper.dropIndex(table_name, column_name)
    } else if (!current_indices.includes(indexname) && index) {
      return this.query_wrapper.createIndex(table_name, column_name)
    }
    return
  }
}

module.exports = SchemaBuilder
const knex = require('knex')(require('./config'))
const db_schema = require('./db_schema')
const QueryWrapper = new (require('./query_wrapper'))(db_schema, knex)
const SchemaBuilder = new (require('./schema_builder'))(db_schema, knex);

(async() =>{
    await SchemaBuilder.setupSchema()
    const result = await QueryWrapper.create('tbl_Company', { name: 'Test', legal_name: 'wat'})
    .catch(err => console.log(err.message))
    console.log('@res', result)
})()
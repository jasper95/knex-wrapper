const db_schema = require('./db_schema')
const config = require('../config')
const knex = require('knex')
const chai = require('chai')
    , { expect } = chai
const Promise = require('bluebird')
const QueryWrapper = new (require('../query_wrapper'))(db_schema, knex, config)
const SchemaBuilder = new (require('../schema_builder'))(db_schema, QueryWrapper)
const table = 'tbl_Company'
const legal_name = 'nana'
const arr = [{name: 'im', legal_name }, {name: 'in', legal_name }]

describe('Schema Builder Tests', () => {
    it('Query helper should create database', async() => {
        await QueryWrapper.dropDatabase(db_schema.database)
        await QueryWrapper.createDatabase(db_schema.database)
        const res = await QueryWrapper._checkDatabase()
        expect(res).be.equal(true)
    })
    it('Schema Builder should setup schema', async() => {
        await QueryWrapper.dropDatabase(db_schema.database)
        await SchemaBuilder.setupSchema()
        const res = await QueryWrapper._checkDatabase()
        expect(res).be.equal(true)
    })
    it('Should Display Tables', async() => {
        const res = await QueryWrapper._listTables()
        expect(res).be.a('array')
    })
    it('Should Display Table colmuns', async() => {
        const res = await QueryWrapper._listColumns(table)
        expect(res).be.a('array')
    })
})

describe('Query Wrapper Tests', () => {
    describe('Test Mutations', () => {
        const name = 'Changed man'
        const name2 = 'Wtf Man'
        let inserted
        let inserted_arr
        before(async() => {
            await QueryWrapper.knex.table(table).delete()
        })
        it('Should insert', async() => {
            ([inserted, inserted_arr] = await
                Promise.all([
                    QueryWrapper.insert(table, { name: 'Testing dude', legal_name: 'Waaa' }),
                    QueryWrapper.insert(table, arr)
                ])
            )
            expect(inserted).be.a('object')
            expect(inserted_arr).be.a('array')
            expect(inserted_arr.length).be.equal(2)
        })
        it('Should upsert', async() => {
            const new_name = 'Im new Dude'
            const new_inserted = { name: 'Someone like new', legal_name: 'Someone like new' }
            const updated = await QueryWrapper.upsert(table, {...inserted, name: new_name })
            const res = await QueryWrapper.upsert(table, new_inserted)
            expect(res).be.a('object')
            expect(updated.id).be.equal(inserted.id)
            expect(updated.name).be.equal(new_name)
        })
        it('Should update by id', async() => {
            const name = 'Changed man'
            const updated = await QueryWrapper.updateById(table, { id: inserted.id, name })
            expect(updated.name).be.equal(name)
        })
        it('Should update by filter', async() => {
            const updated = await QueryWrapper.updateByFilter(table, { name: name2 }, { name })
            expect(updated.length).not.equal(0)
        })
        it('Should delete by id', async() => {
            const deleted = await QueryWrapper.deleteById(table, inserted)
            expect(deleted).be.equal(inserted.id)
        })
        it('Should delete by filter', async() => {
            const deleted = await QueryWrapper.deleteByFilter(table, { legal_name })
            expect(deleted.length).be.equal(inserted_arr.length)
        })
    })
    describe('Test Queries', async() => {
        before(async() => {
            // add Data
            await QueryWrapper.knex.table(table).delete()
            await QueryWrapper.knex.table(table).insert(arr)
        })
        it('Query List', async() => {
            const result = await QueryWrapper.filter(table, {}, ['name', 'legal_name'])
            expect(result).deep.equal(arr)
        })
        it('Query By Id', async() => {
            const [data1] = await QueryWrapper.filter(table, {})
            const data2 = await QueryWrapper.find(table, data1.id)
            expect(data1).deep.equal(data2)
        })
        it('Query List by filter', async() => {
            const [res1, res2] = await Promise.all([
                QueryWrapper.filter(table, { name: 'in' }, ['name', 'legal_name']),
                QueryWrapper.filter(table, { legal_name }, ['name', 'legal_name'])
            ])
            expect(res1).to.deep.equal([arr[1]])
            expect(res2).deep.equal(arr)
        })
    });
})


const db_schema = require('./db_schema')
const knex = require('knex')(require('../config'))
const chai = require('chai')
    , { expect } = chai
const Promise = require('bluebird')
const QueryWrapper = new (require('../query_wrapper'))(db_schema, knex)
const table = 'tbl_Company'
const legal_name = 'nana'
const arr = [{name: 'im', legal_name }, {name: 'in', legal_name }]
describe('Query Wrapper Tests', () => {
    before(async() => {
        await knex.table(table).delete()
    })
    describe('Test Mutations', () => {
        const name = 'Changed man'
        const name2 = 'Wtf Man'
        let inserted
        let inserted_arr
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
            await knex.table(table).insert(arr)
        })
        it('Query List', async() => {
            const result = await QueryWrapper.filter(table, {}, ['name', 'legal_name'])
            expect(result).deep.equal(arr)
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


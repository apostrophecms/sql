const assert = require('assert');
const fs = require('fs');

describe('sql', function () {

  const dbFile = `${__dirname}/test.sqlite`;
  let knex;
  let sql;
  let docs;

  before(function() {
    if (fs.existsSync(dbFile)) {
      fs.unlinkSync(dbFile)
    }
    knex = require('knex')({
      client: 'sqlite3',
      connection: {
        filename: dbFile
      }
    });
  });

  after(async function() {
    await knex.destroy();
  });

  it('should return an object', function() {
    sql = require('../index.js')({ knex });
    assert(sql);
  });

  it('should return a collection', function() {
    docs = sql.collection('docs');
    assert(docs);
  });

  it('should insert documents', async function() {
    const result = await docs.insertOne({
      name: 'spyspy',
      fur: 'black'
    });
    assert(result);
    assert.strictEqual(result.result.nModified, 1);
    await docs.insertOne({
      name: 'pypy',
      fur: 'black'
    });
    await docs.insertOne({
      name: 'spike',
      fur: 'tortoiseshell'
    });
  });

  it('should find all documents with toArray', async function() {
    const result = await docs.find({}).toArray();
    assert(result);
    console.log(result);
    assert.strictEqual(result.length, 3);
  });

  it('should find one specified document with toArray', async function() {
    const result = await docs.find({
      name: 'pypy'
    }).toArray();
    assert(result);
    console.log(result);
    assert.strictEqual(result.length, 1);
    assert(result[0].name === 'pypy');
    assert(result[0].fur === 'black');
  });

  it('should find one specified document with findOne', async function() {
    const result = await docs.findOne({
      name: 'pypy'
    });
    assert(result);
    assert(result.name === 'pypy');
    assert(result.fur === 'black');
  });

});
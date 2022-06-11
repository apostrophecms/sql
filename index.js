const sift = require('sift');
const { EJSON } = require('bson');
const cuid = require('cuid');

module.exports = ({ knex }) => {
  return {
    collection(name) {
      return collection(name);
    },
    close() {
      return knex.destroy();
    }
  };
  function collection(name) {
    const readyQueue = [];
    let tablePending = true;
    let tableFailed = false;
    const indexes = [];
    // .collection(name) does not have to be awaited, so we
    // need to prepare the table in the background and shake hands
    // with the ready() method on first use
    (async () => {
      try {
        if (!await knex.schema.hasTable(name)) {
          await knex.schema.createTable(name, table => {
            table.string('_id').unique().primary();
            // A 16MB mongodb BSON document might take up more
            // than the 16MB limit of mediumtext as extended JSON
            table.text('_ext_json', 'longtext');
          });
        }
        tablePending = false;
        for (const item of readyQueue) {
          item.resolve();
        }
      } catch (e) {
        tableFailed = e;
        for (const item of readyQueue) {
          item.reject(e);
        }
      }
    })();
    const self = {
      name,
      indexes: [
        {
          fields: {
            _id: 1
          },
          options: {}
        }
      ],
      async ready() {
        if (tableFailed) {
          throw tableFailed;
        }
        if (!tablePending) {
          return;
        }
        return new Promise((resolve, reject) => {
          readyQueue.push({ resolve, reject });          
        });
      },
      async createIndex(fields, options = {}) {
        await self.ready();
        const columnNames = Object.keys(fields);
        self.indexes.push({
          fields,
          options
        });
        for (const columnName of columnNames) {
          if (!await knex.schema.hasColumn(name, columnName)) {
            await knex.schema.alterTable(name, table => {
              table.string(columnName);
            });
          }
        }
        await knex.table.alterTable(name, table => {
          if (options.unique) {
            table.unique(columnNames);
          } else {
            table.index(columnNames);
          }
        });
      },
      find(criteria) {
        return query(self, criteria);
      },
      async findOne(criteria) {
        return (await query(self, criteria).limit(1).toArray())[0];
      },
      // TODO completely unoptimized memory hog
      async distinct(propertyName, criteria = {}) {
        return [...new Set((await query(self, criteria).toArray()).map(value => value[propertyName]))];
      },
      async insertOne(doc) {
        await self.ready();
        await knex(name).insert(prepareForInsert(indexes, doc));
        return {
          result: {
            nModified: 1
          }
        };
      },
      async insertMany(docs) {
        await self.ready();
        // TODO more performant batch insert
        for (const doc of docs) {
          await knex(name).insert(prepareForInsert(indexes, doc));
        }
        return {
          result: {
            nModified: docs.length
          }
        };
      },
      async replaceOne(doc) {
        await self.ready();
        await knex(name).update(prepareForUpdate(indexes, doc)).where('_id', '=', doc._id);
        return {
          result: {
            nModified: 1
          }
        };
      },
      // TODO operators are not atomic, that's bad
      async updateOne(criteria, operations) {
        await self.ready();
        const doc = await self.findOne(criteria);
        if (!doc) {
          return {
            result: {
              nModified: 0
            }
          };
        }
        return self.updateBody(doc, operations);
      },
      async updateBody(doc, operations) {
        for (const [ key, value ] of Object.entries(operations)) {
          switch(key) {
            case '$set':
              Object.assign(doc, value);
              break;
            case '$unset':
              for (const prop of Object.keys(value)) {
                delete doc[prop];
              }
              break;
            case '$inc':
              for (const [ prop, increment ] of Object.entries(value)) {
                if (!has(doc, prop)) {
                  doc[prop] = 0;
                }
                doc[prop] += increment;
              }
              break;
            case '$pull':
              for (const [ prop, pulled ] of Object.entries(value)) {
                doc[prop] = (doc[prop] || []).filter(value => value !== pulled);
              }
              break;
            case '$addToSet':
              for (const [ prop, added ] of Object.entries(value)) {
                doc[prop] = [...new Set(doc[prop] || [], added)];
              }
              break;
            default:
              throw new Error(`@apostrophecms/sql does not support ${key}`);
          }
        }
        await knex(name).update(doc).where('id', '=', doc._id);
        return {
          result: {
            nModified: 1
          }
        };
      },
      async updateMany(criteria, operations) {
        await self.ready();
        // TODO do this in reasonable batches to avoid wasting memory
        const docs = await self.find(criteria).toArray();
        for (const doc of docs) {
          // TODO reasonable parallelism
          await updateBody(doc, operations);
        }
      }
      // TODO limited support for aggregation queries
    };
    return self;
  }
  function query(collection, criteria) {
    let sortParams = null;
    let skipParam = null;
    let limitParam = null;
    const self = {
      sort(params) {
        sortParams = params;
        return self;
      },
      skip(n) {
        skipParam = n;
        return self;
      },
      limit(n) {
        limitParam = n;
        return self;
      },
      async toArray() {
        await collection.ready();
        const query = knex(collection.name);
        if (skipParam) {
          query.offset(skipParam);
        }
        if (limitParam) {
          query.offset(limitParam);
        }
        if (sortParams) {
          const orderBy = [];
          for (const [ columnName, direction ] of Object.entries(sortParams)) {
            orderBy.push({
              column: columnName,
              order: (direction > 0) ? 'asc' : 'desc'
            });
          }
          query.orderBy(orderBy);
        }
        for (const index of collection.indexes) {
          let n = 0;
          for (const columnName of Object.keys(index.fields)) {
            const value = deepGet(criteria, columnName);
            if ((value !== undefined) && ((typeof value) !== 'object')) {
              query.andWhere(() => {
                this.where(columnName, '=', value);
              });
            } else {
              break;
            }
          }
        }
        // TODO paginate buckets of results so we don't run out of memory
        // if the criteria would have reduced it to a reasonable response
        const results = await query;
        const sifter = sift(criteria);
        return results.filter(row => sifter(EJSON.parse(row._ext_json))).map(row => EJSON.parse(row._ext_json));
      }
    }
    return self;
  }
};

// Looks for key as a property name of criteria,
// or of a nested sub-object accessed via $and.
// Returns the value corresponding to key, or
// undefined if none exists.

function deepGet(criteria, key) {
  if (has(criteria, key)) {
    return criteria[key];
  }
  for (const value of Object.values(criteria.$and || [])) {
    const found = deepGet(value, key);
    if ((typeof found) !== 'undefined') {
      return found;
    }
  }
  return undefined;
}

function has(object, key) {
  return Object.hasOwnProperty.call(object, key);
}

function prepareForInsert(indexes, doc) {
  const row = {
    _id: doc._id || cuid(),
    _ext_json: EJSON.stringify(doc)
  };
  for (const index of indexes) {
    for (const columnName of index) {
      row[columnName] = doc[columnName];
    }
  }
  return row;
}

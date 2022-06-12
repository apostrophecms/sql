const sift = require('sift');
const { EJSON } = require('bson');
const cuid = require('cuid');

module.exports = ({ knex }) => {
  const names = new Set();
  return {
    db(name) {
      if (name) {
        if ((names.size > 0) && !names.has(name)) {
          throw new Error('@apostrophecms/sql is not currently able to manage multiple databases through one client connection');
        }
        names.add(name);
      }
      return {
        collection(name) {
          return collection(name);
        },
        close() {
          return knex.destroy();
        }
      };
    },
    isConnected() {
      return true;
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
        const columnNames = Object.keys(fields).map(mangleName);
        self.indexes.push({
          fields,
          options
        });
        const alterations = [];
        for (const columnName of columnNames) {
          if (!await knex.schema.hasColumn(name, columnName)) {
            alterations.push(table => {
              table.string(columnName);
            });
          }
        }
        alterations.push(table => {
          if (options.unique) {
            table.unique(columnNames);
          } else {
            table.index(columnNames);
          }
        });
        try {
          await knex.schema.alterTable(name, table => {
            for (const alteration of alterations) {
              alteration(table);
            }
          });
        } catch (e) {
          // knex has no portable way to list existing indexes,
          // so giving them names and checking for those names isn't
          // useful. This test works for sqlite3, we'll need more
          // of these
          if (!e.toString().includes('already exists')) {
            throw e;
          }
        }
      },
      find(criteria = {}) {
        return query(self, criteria);
      },
      async count(criteria = {}) {
        return self.countDocuments(criteria);
      },
      async countDocuments(criteria = {}) {
        // TODO because of the need for sift() for some queries this won't always be optimizable
        // but sometimes it is and the pain of fetching everything just to count it,
        // with no projection, is obviously terrible
        return (await self.find(criteria).toArray()).length;
      },
      async findOne(criteria = {}) {
        return (await self.find(criteria).limit(1).toArray())[0];
      },
      async deleteOne(criteria = {}) {
        // TODO we need the query because of sift, but in simple cases
        // involving indexed columns or _id we can optimize it away
        const matching = (await query(self, criteria).limit(1).toArray())[0];
        if (!matching) {
          return {
            result: {
              nDeleted: 0
            }
          };
        }
        const n = await knex(name).where('_id', matching._id).delete();
        return {
          result: {
            nDeleted: n
          }
        };
      },
      async deleteMany(criteria = {}) {
        // TODO we need the query because of sift, but in simple cases
        // involving indexed columns or _id we can optimize it away
        const matching = await query(self, criteria).limit(1).toArray();
        const ids = matching.map(match => match._id);
        if (!ids.length) {
          return {
            result: {
              nDeleted: 0
            }
          };  
        }
        const n = await knex(name).whereIn('_id', ids).delete();
        return {
          result: {
            nDeleted: n
          }
        };
      },
      async removeOne(criteria = {}) {
        return self.deleteOne(criteria);
      },
      async removeMany(criteria = {}) {
        return self.deleteMany(criteria);
      },
      // TODO we need the query because of sift, but in simple cases
      // involving only indexed columns and _id we can optimize it away
      async distinct(propertyName, criteria = {}) {
        return [...new Set((await query(self, criteria).toArray()).map(value => value[propertyName]))];
      },
      async insertOne(doc) {
        await self.ready();
        try {
          await knex(name).insert(prepareForInsert(self.indexes, doc));
          return {
            result: {
              nModified: 1
            }
          };
        } catch (e) {
          throw compatibleError(e);
        }
      },
      async insertMany(docs) {
        await self.ready();
        try {
          // TODO more performant batch insert
          for (const doc of docs) {
            await knex(name).insert(prepareForInsert(self.indexes, doc));
          }
          return {
            result: {
              nModified: docs.length
            }
          };
        } catch (e) {
          throw compatibleError(e);
        }
      },
      async replaceOne(doc) {
        await self.ready();
        await knex(name).update(prepareForUpdate(self.indexes, doc)).where('_id', '=', doc._id);
        return {
          result: {
            nModified: 1
          }
        };
      },
      // TODO operators are not atomic, that's bad
      async updateOne(criteria, operations, options = {}) {
        await self.ready();
        const doc = await self.findOne(criteria);
        if (!doc) {
          if (options.upsert) {
            return self.insertOne(operations.$set);
          }
          return {
            result: {
              nModified: 0
            }
          };
        }
        return self.updateBody(doc, operations);
      },
      async updateBody(doc, operations) {
        if (operations.$currentDate) {
          operations = {
            ...operations
          };
          const now = new Date();
          operations.$set = {
            ...(operations.$set || {})
          };
          for (const key of Object.keys(operations.$currentDate)) {
            operations.$set[key] = now;
          }
          delete operations.$currentDate;
        }
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
            // TODO this isn't atomic and can lead to race conditions, we can
            // fix that by creating a column for any property that gets
            // manipulated by $inc on first use of $inc
            case '$inc':
              for (const [ prop, increment ] of Object.entries(value)) {
                if (!Object.hasOwn(doc, prop)) {
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
        await knex(name).update(prepareForUpdate(self.indexes, doc)).where('_id', '=', doc._id);
        return {
          result: {
            nModified: 1
          }
        };
      },
      async updateMany(criteria, operations, options = {}) {
        await self.ready();
        // TODO do this in reasonable batches to avoid wasting memory
        const docs = await self.find(criteria).toArray();
        if (options.upsert && !docs.length) {
          return self.insertOne(operations.$set);
        }
        for (const doc of docs) {
          // TODO reasonable parallelism
          await self.updateBody(doc, operations);
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
      project(projection = {}) {
        // TODO we can't do a lot here but we could
        // support projections of _id only and we should
        // definitely fake it by filtering the result
        // at the end
        return self;
      },
      async toArray() {
        await collection.ready();
        const query = knex(collection.name);
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
              query.andWhere(function() {
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
        let filtered = results.filter(row => sifter(EJSON.parse(row._ext_json))).map(row => EJSON.parse(row._ext_json));
        // TODO this can be quite inefficient but because of the need for the sifter a naive
        // use of "offset" and "limit" would produce missing results. Optimize this away in cases
        // where the SQL query can include everything, and in cases where we can't do that,
        // query for batches at a time in hopes that we get to stop short of looking at every
        // record in the table
        if (skipParam) {
          filtered = filtered.slice(skipParam);
        }
        if (limitParam) {
          filtered = filtered.slice(0, limitParam);
        }
        return filtered;
      },
      async count() {
        return (await self.toArray()).length;
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
  if (Object.hasOwn(criteria, key)) {
    return criteria[key];
  }
  for (const value of Object.values(criteria.$and || [])) {
    if (value) {
      const found = deepGet(value, key);
      if ((typeof found) !== 'undefined') {
        return found;
      }
    }
  }
  return undefined;
}

function prepareForUpdate(indexes, doc) {
  const row = {
    _id: doc._id,
    _ext_json: EJSON.stringify(doc)
  };
  for (const index of indexes) {
    for (const field of Object.keys(index.fields)) {
      row[mangleName(field)] = deepGet(doc, field);
    }
  }
  return row;
}

function prepareForInsert(indexes, doc) {
  return {
    ...prepareForUpdate(indexes, doc),
    _id: doc._id || cuid()
  };
}

function mangleName(name) {
  return name.replace(/\./g, '__');
}

function compatibleError(e) {
  if (e.code === 'SQLITE_CONSTRAINT') {
    const error = new Error('Not Unique');
    // Apostrophe is expecting this error code for unique keys
    error.code = 11000;
    return error;
  } else {
    return e;
  }
}
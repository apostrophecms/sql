# @apostrophecms/sql

## Stability: pre-alpha, work in progress

This module is not ready for practical use.

## About

A MongoDB-like API wrapper for SQL databases such as sqlite, mysql and postgreSQL. This module provides the option of using any SQL database fully compatible with the popular [knex](http://knexjs.org/) query builder for Node.js. That gives ApostropheCMS access to your database of choice through an interface compatible with a subset MongoDB query syntax.

`sqlite3` is a popular choice with this module because it allows you to use a local file for your database, which is OK for small projects and means you don't have to set up a database server at all.

> ⚠️ This **is not** a module for querying your existing SQL tables from ApostropheCMS. For that, we suggest using [knex](http://knexjs.org/) directly in your own custom `apiRoutes` and async `components`. This module's purpose is to allow code that normally requires MongoDB to store content in another type of database. This module will create database tables corresponding to Apostrophe's usual collection names and store most content in extended JSON form. If you have unrelated SQL tables, we recommend that you use a unique prefix for their names to be safe, or use a separate database for them.

## Installation

```bash
# install the package, plus knex and
# a knex-compatible database driver
npm install @apostrophecms/sql knex sqlite3
```

## Configuration

```javascript
// in app.js

const apostrophe = require('apostrophe');

const shortName = 'mysite';
const knex = require('knex')({
  client: 'sqlite3',
  connection: {
    // other databases have different connection options
    filename: `./data/${shortName}.sqlite`,
  },
  // required with sqlite
  useNullAsDefault: true
});

const sql = require('@apostrophecms/sql')({
  knex,
  metadata: {
    folder: `${__dirname}/sql-metadata`,
    locked: false
  }
});

apostrophe({
  shortName,
  modules: {
    '@apostrophecms/db': {
      options: {
        // Substitute for mongodb database connection
        client: sql
      }
    }
  }
});
```

Note that you are responsible for configuring `knex` and passing it to this module. This gives you a client object that can be passed as the `client` option of the built-in `@apostrophecms/db` module.

## The metadata file, and special requirements for production

Certain MongoDB features like `createIndex`, `$set`, and `$inc` don't map one-to-one to SQL unless they are backed by separate columns in the SQL table for each collection. Yet MongoDB doesn't require creating schemas in advance. To reconcile the two with acceptable performance, a compromise is required:

* In a development environment, these operations work as expected, as long as there is only one process using the database, and a metadata folder is automatically created and updated with JSON files containing the special information about each collection needed to support these operations later in production. This means `$inc`, `$set` and `$unset` are atomic operations, after the first time. **The resulting metadata folder must be committed and deployed with the project.**
* In a production environment, the metadata folder is loaded at startup, any missing SQL columns and indexes are created according to the metadata, and any use of these operations that is not specifically authorized by the existing metadata folder will **fail**.
* This means that all database indexes and uses of `$set`, `$inc`, etc. must be tested first in development (as they ought to be anyway, before deploying your code). That testing can happen manually or via unit test coverage.
* As an alternative, you can also directly edit the metadata folder, spelling out the specific indexes and `$set`/`$unset`/`$inc` — friendly properties your collections require. To prevent further automatic updates to the metadata in a development environment you may choose to set the `locked: true` subproperty of the `metadata` option (see example above). To learn the format, first use this module without `locked: true` and observe the metadata files that it builds.
* The benefit is that in production `$inc`, `$set` and `$unset` are always atomic operations, as is expected and required by ApostropheCMS and other applications.

> ⚠️ This module distinguishes development environments from production-like environments by looking for `NODE_ENV=production`. Note that "staging servers" and the like are still "production-like" environments and should still set this variable, as many other modules, including Express, are designed to optimize only when they see this value.

## Known limitations

* Indexes on array properties are not supported.
* If you wish to `sort` on a property, you must first index that property with `createIndex`.
* Queries are "sifted" in-memory using the [sift](https://github.com/crcn/sift.js) module, with one exception: simple equality tests of indexed properties are converted to `WHERE` clauses for efficient evaluation by the database. Support for range queries on indexed properties is expected to be added.
* `distinct` is currently inefficient because it fetches the entire object before winnowing down to just the property of interest. This will be sped up later for indexed properties.
* Callbacks are not supported, only promises (Apostrophe always uses `await` when making database calls).
* If [sift](https://github.com/crcn/sift.js) doesn't support something, this module doesn't either. However we don't promise to keep any `sift` features not also present in the MongoDB API.
* Aggregation support is very limited.
* Legacy API methods are mostly unsupported. Use `insertOne`, `replaceOne`, `updateOne`, `removeMany`, etc.
* The case of names of properties that require a corresponding column (see below) is preserved, i.e. a property named `.Foo` will come back as `.Foo` and not `.foo`, however attempts to store separate properties named `.foo` and `.Foo` may not work.

## Design Notes

Operations like `$set`, `$unset`, and `$inc` are made atomic in the following way:

* If the metadata indicates we haven't seen this operation before for this property, we create a column for the property and note it in the metadata (dev environment) or throw an error (production environment).
* To implement the operation we SET the property to its own extended JSON value.
* On all reads, after parsing the main extended JSON we also parse any properties that have their own extended JSON columns and deep-set those too before returning the value, sorting by property name to get the nesting right.
* Columns for subproperties created to either these operations or indexes on subproperties are named like this: `parent__child`. Note the double underscore.
* For broader database compatibility, if the column name would exceed 30 characters, then in development a name is assigned to it in the metadata file.

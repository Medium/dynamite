/**
 * Provides ultility function for unit testing.
 *
 * @module utils
 **/
var AWS = require('aws-sdk')
var localDynamo = require('local-dynamo')
var Q = require('kew')
var dynamite = require('../../dynamite')
var AWSName = require('../../lib/common').AWSName

var utils = {}

var apiVersion = AWSName.API_VERSION_2012

// These options make dynamite connect to the fake Dynamo DB instance.
// The good thing here is that we can initialize a dynamite.Client
// using the exact same way as we use in production.
var options = {
  apiVersion: apiVersion,
  sslEnabled: false,
  endpoint: 'localhost:4567',
  accessKeyId: 'xxx',
  secretAccessKey: 'xxx',
  region: 'xxx',
  retryHandler: function (method, table, response) {
    console.log('retrying', method, table, response)
  }
}

utils.getMockDatabase = function () {
  AWS.config.update(options)
  return new AWS.DynamoDB()
}

utils.getMockDatabaseClient = function () {
  return new dynamite.Client(options)
}

var localDynamoProc = null
utils.ensureLocalDynamo = function () {
  if (!localDynamoProc) {
    localDynamoProc = localDynamo.launch({
      port: 4567,
      detached: true,
      heap: '1g'
    })
    localDynamoProc.on('exit', function () {
      localDynamoProc = null
    })
    localDynamoProc.unref()
  }

  return localDynamoProc
}
process.on('exit', function () {
  if (localDynamoProc) {
    localDynamoProc.kill()
  }
})

/*
 * A helper function that delete the testing table.
 *
 * @param db {AWS.DynamoDB} The database instance.
 * @return {Promise}
 */
utils.deleteTable = function (db, tableName) {
  var defer = Q.defer()
  db.deleteTable(
    {TableName: tableName},
    defer.makeNodeResolver()
  )
  return defer.promise
}

/*
 * A helper to generate an index name for testing indices.
 *
 * @param hashKey {string}
 * @param rangeKey {string}
 */
utils.indexNameGenerator = function (hashKey, rangeKey) {
  var name = 'index-' + hashKey
  if (rangeKey) name = name + '-' + rangeKey
  return name
}

/*
 * A helper function that creates the testing table.
 *
 * @param db {AWS.DynamoDB} The database instance.
 * @return {Promise}
 */
utils.createTable = function (db, tableName, hashKey, rangeKey, gsiDefinitions) {
  var defer = Q.defer()
  var opts = {}
  if (apiVersion === AWSName.API_VERSION_2011) {
    opts =  {
      TableName: tableName,
      KeySchema: {
        HashKeyElement: {AttributeName: hashKey, AttributeType: "S"}
      },
      ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}
    }

    if (rangeKey) {
      opts.KeySchema.RangeKeyElement = {AttributeName: rangeKey, AttributeType: "S"}
    }

    db.createTable(opts, defer.makeNodeResolver())
  } else if (apiVersion === AWSName.API_VERSION_2012) {
    var attributeDefinitions = {}
    attributeDefinitions[hashKey] = "S"
    opts = {
      TableName: tableName,
      AttributeDefinitions: [],
      KeySchema: [
        {AttributeName: hashKey, KeyType: "HASH"}
      ],
      ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}
    }

    if (rangeKey) {
      attributeDefinitions[rangeKey] = "S"
      opts.KeySchema.push({
        AttributeName: rangeKey,
        KeyType: "RANGE"
      })
    }

    if (gsiDefinitions) {
      opts.GlobalSecondaryIndexes = gsiDefinitions.map(function (index) {

        var keySchema = [
          {AttributeName: index.hashKey, KeyType: "HASH"}
        ]
        var hashKeyType = index.hashKeyType || "S"
        attributeDefinitions[index.hashKey] = hashKeyType

        if (index.rangeKey) {
          var rangeKeyType = index.rangeKeyType || "S"
          keySchema.push({AttributeName: index.rangeKey, KeyType: "RANGE"})
          attributeDefinitions[index.rangeKey] = rangeKeyType
        }
        return {
          IndexName: utils.indexNameGenerator(index.hashKey, index.rangeKey),
          KeySchema: keySchema,
          Projection: {
            ProjectionType: "ALL"
          },
          ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}
        }
      })
    }

    for (var field in attributeDefinitions) {
      opts.AttributeDefinitions.push({AttributeName: field, AttributeType: attributeDefinitions[field]})
    }

    db.createTable(opts, defer.makeNodeResolver())
  } else {
    defer.reject(new Error('No api version found'))
  }
  return defer.promise
}

/*
 * A helper function that converts raw data JSON into AWS JSON format.
 *
 * Example:
 *
 * raw data JSON: { userId: 'userA', column: '@', age: '29' }
 *
 * AWS JSON: { userId: { S: 'userA' }, column: { S: '@' }, age: { N: '29' } }
 *
 * @param obj {Object} The raw JSON data
 * @return {Object} The same data in AWS JSON
 */
var convert = function (obj) {
  var items = {}
  for (var key in obj) {
    if (Array.isArray(obj[key]) && isNaN(obj[key][0])) {
      items[key] = {"SS": obj[key]}
    } else if (Array.isArray(obj[key])) {
      var numArray = []
      for (var i in obj[key]) {
        numArray.push(String(obj[key][i]))
      }

      items[key] = {"NS": numArray}
    } else if (isNaN(obj[key])) {
      items[key] = {"S": obj[key]}
    } else {
      items[key] = {"N": obj[key]}
    }
  }

  return items
}

/*
 * A helper function that incert one record directly using AWS API (not
 * our own putItem)
 *
 * @param db {Object} The database instance
 * @param tableName {String} The name of the table to insert
 * @param record {Object} The raw JSON data
 * @return {Q.Promise}
 */
var putOneRecord = function(db, tableName, record) {
  var defer = Q.defer()
  db.putItem(
    {TableName: tableName, Item: convert(record)},
    defer.makeNodeResolver()
  )
  return defer.promise
}

/*
 * A helper function that initializes the testing database with
 * some data.
 *
 * @return {Promise}
 */
utils.initTable = function (context) {
  var db = context.db
  var promises = []
  for (var i = 0; i < context.data.length; i += 1) {
    promises.push(putOneRecord(db, context.tableName, context.data[i]))
  }
  return Q.all(promises)
}

/*
 * Get a record from the database with the original AWS SDK.
 * The reason we don't use dynamite.getItem() here is to focus this test suite
 * on putItem().
 *
 * @param db {AWS.DynamoDB} The database instance.
 * @param hashKey {String}
 * @param rangeKey {String}
 */
utils.getItemWithSDK = function (db, hashKey, rangeKey, table) {
  var defer = Q.defer()
  var opts = {}

  table = table || 'user'

  if (apiVersion === AWSName.API_VERSION_2011) {
    opts = {
      TableName: table,
      Key: {
        HashKeyElement: {"S": hashKey}
      }
    }

    if (rangeKey) {
      opts.Key.RangeKeyElement = {"S": rangeKey}
    }

    db.getItem(
      opts,
      defer.makeNodeResolver()
    )
  } else if (apiVersion === AWSName.API_VERSION_2012) {
    opts = {
      TableName: table,
      Key: {
        userId: {"S": hashKey}
      }
    }

    if (rangeKey) {
      opts.Key.column = {"S": rangeKey}
    }

    db.getItem(
      opts,
      defer.makeNodeResolver()
    )
  }
  return defer.promise
}

exports = module.exports = utils

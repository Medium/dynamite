/**
 * Provides ultility function for unit testing.
 *
 * @module utils
 **/
var AWS = require('aws-sdk')
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
  retryHandler: function (method, table) {
    console.log('retrying', method, table)
  }
}

utils.getMockDatabase = function () {
  AWS.config.update(options)
  return new AWS.DynamoDB()
}

utils.getMockDatabaseClient = function () {
  return new dynamite.Client(options)
}

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
 * A helper function that creates the testing table.
 *
 * @param db {AWS.DynamoDB} The database instance.
 * @return {Promise}
 */
utils.createTable = function (db, tableName, hashKey, rangeKey) {
  var defer = Q.defer()
  if (apiVersion === AWSName.API_VERSION_2011) {
    db.createTable(
      {TableName: tableName,
       KeySchema:
         {HashKeyElement: {AttributeName: hashKey, AttributeType: "S"},
          RangeKeyElement: {AttributeName: rangeKey, AttributeType: "S"}},
       ProvisionedThroughput: {ReadCapacityUnits: 0, WriteCapacityUnits: 0}
      },
      defer.makeNodeResolver()
    )
  } else if (apiVersion === AWSName.API_VERSION_2012) {
    db.createTable(
      {TableName: tableName,
       AttributeDefinitions: [{AttributeName: hashKey, AttributeType: "S"},
                              {AttributeName: rangeKey, AttributeType: "S"}],
       KeySchema: [{AttributeName: hashKey, KeyType: "HASH"},
                   {AttributeName: rangeKey, KeyType: "RANGE"}],
       ProvisionedThroughput: {ReadCapacityUnits: 0, WriteCapacityUnits: 0}
      },
      defer.makeNodeResolver()
    )
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
  items = {}
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
    {TableName: tableName,
     Item: convert(record)
    },
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
utils.initTable = function (response, context) {
  var db = context.db
  var p = null
  var helper = function (i) {
    return function (e) {
      return putOneRecord(db, context.tableName, context.data[i])
    };
  }
  p = putOneRecord(db, context.tableName, context.data[0]);
  for (var i = 1; i < context.data.length; i += 1) {
    p = p.then(helper(i))
  }
  return p
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
utils.getItemWithSDK = function (db, hashKey, rangeKey) {
  var defer = Q.defer()
  if (apiVersion === AWSName.API_VERSION_2011) {
    db.getItem(
      {TableName: "user",
       Key: {HashKeyElement: {"S": hashKey},
             RangeKeyElement: {"S": rangeKey}}
      },
      defer.makeNodeResolver()
    )
  } else if (apiVersion === AWSName.API_VERSION_2012) {
    db.getItem(
      {TableName: "user",
       Key: {userId: {"S": hashKey},
             column: {"S": rangeKey}}
      },
      defer.makeNodeResolver()
    )
  }
  return defer.promise
}

exports = module.exports = utils

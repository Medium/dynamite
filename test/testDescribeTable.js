// Copyright 2013 The Obvious Corporation.

var utils = require('./utils/testUtils.js')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)

// basic setup for the tests, creating record userA with range key @
exports.setUp = function (done) {
  this.db = utils.getMockDatabase()
  this.client = utils.getMockDatabaseClient()
  utils.ensureLocalDynamo()
  utils.createTable(this.db, "user", "userId", "column")
    .fail(onError)
    .fin(done)
}

exports.tearDown = function (done) {
  utils.deleteTable(this.db, "user")
    .fin(done)
}

// check that an item exists
builder.add(function testSimpleDescribeTable(test) {
  return this.client.describeTable("user")
    .execute()
    .then(function (data) {
      test.equal(data.Table.TableName, "user", "Table name should be 'user'")
      test.equal(data.Table.KeySchema[0].AttributeName, "userId", "Hash key name should be 'userId'")
      test.equal(data.Table.KeySchema[1].AttributeName, "column", "Hash key name should be 'column'")
    })
})

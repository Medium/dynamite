// Copyright 2013 The Obvious Corporation.

var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)
var initialData = [{"userId": "userA", "column": "@", "age": "29"}]

// basic setup for the tests, creating record userA with range key @
exports.setUp = function (done) {
  this.db = utils.getMockDatabase()
  this.client = utils.getMockDatabaseClient()
  utils.createTable(this.db, "user", "userId", "column")
    .thenBound(utils.initTable, null, {db: this.db, tableName: "user", data: initialData})
    .fail(onError)
    .fin(done)
}

exports.tearDown = function (done) {
  utils.deleteTable(this.db, "user")
    .then(function () {
      done()
    })
}

// check that an item exists
builder.add(function testItemExists(test) {
  return this.client.getItem('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .execute()
    .then(function (data) {
      test.equal(data.result.age, 29, 'Age should match the provided age')
    })
})

// check that only selected attributes are returned
builder.add(function testSelectedAttributes(test) {
  return this.client.getItem('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .selectAttributes(['userId', 'column'])
    .execute()
    .then(function (data) {
      test.equal(data.result.column, '@', 'Column should be defined')
      test.equal(data.result.age, undefined, 'Age should be undefined')
    })
})

// check that an item doesn't exist
builder.add(function testItemDoesNotExist(test) {
  return this.client.getItem('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .execute()
    .then(function (data) {
      test.equal(data.result, undefined, 'Record should not exist')
    })
})

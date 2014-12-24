// Copyright 2013 The Obvious Corporation.
var AWS = require('aws-sdk');
var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var typ = require('typ')
var errors = require('../lib/errors')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = function (err) {
  console.error(err.stack)
}
var initialData = [{"userId": "userA", "column": "@", "age": "29"}]

// basic setup for the tests, creating record userA with range key @
exports.setUp = function (done) {
  var self = this
  this.db = utils.getMockDatabase()
  this.client = utils.getMockDatabaseClient()
  utils.createTable(self.db, "userRangeOnly", "userId")
    .thenBound(utils.initTable, null, {db: self.db, tableName: "userRangeOnly", data: initialData})
    .fail(onError)
    .fin(done)
}

exports.tearDown = function (done) {
  utils.deleteTable(this.db, 'userRangeOnly')
    .fin(done)
}

// check that an item exists
builder.add(function testItemExists(test) {
  return this.client.getItem('userRangeOnly')
    .setHashKey('userId', 'userA')
    .execute()
    .then(function (data) {
      test.equal(data.result.age, 29, 'Age should match the provided age')
    })
})

// put an item and check that it exists
builder.add(function testSimplePut(test) {
  var self = this
  return this.client.putItem("userRangeOnly", {
    userId: 'userB',
    column: '@',
    age: 30
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userB", null, 'userRangeOnly')
  })
  .then(function (data) {
    test.equal(data['Item']['age'].N, "30", "Age should be set")
  })
})

// put a list of strings and check if they exist
builder.add(function testStringSetPut(test) {
  var self = this
  return this.client.putItem("userRangeOnly", {
    userId: 'userC',
    column: '@',
    postIds: ['3a', '3b', '3c']
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userC", null, 'userRangeOnly')
  })
  .then(function (data) {
    test.deepEqual(data['Item']['postIds'].SS, ['3a', '3b', '3c'], "postIds should be ['3a', '3b', '3c']")
  })
})

builder.add(function testDeleteItem(test) {
  //AWS.config.logger = process.stdout

  var self = this
  return self.client.deleteItem('userRangeOnly')
    .setHashKey('userId', 'userA')
    .execute()
    .then(function (data) {
      return utils.getItemWithSDK(self.db, "userA", null, "userRangeOnly")
    })
    .then(function (data) {
      test.equal(data['Item'], undefined, "User should be deleted~~~" + JSON.stringify(data))
    })
})
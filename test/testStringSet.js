var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)
var initialData = [  {"userId": "userA", "column": "@", "postIds": ['1a', '1b', '1c']}
                   , {"userId": "userB", "column": "@", "postIds": [1, 2, 3]}]

/*
 * Sets up for test, and creates a record userA with range key @.
 */
exports.setUp = function (done) {
  this.db = utils.getMockDatabase()
  this.client = utils.getMockDatabaseClient()
  utils.ensureLocalDynamo()
  utils.createTable(this.db, "user", "userId", "column")
    .thenBound(utils.initTable, null, {db: this.db, tableName: "user", data: initialData})
    .fail(onError)
    .fin(done)
}

exports.tearDown = function (done) {
  utils.deleteTable(this.db, "user")
    .fin(done)
}

// put a list of strings and check if they exist
builder.add(function testStringSetPut(test) {
  var self = this
  return this.client.putItem("user", {
    userId: 'userC',
    column: '@',
    postIds: ['3a', '3b', '3c']
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userC", "@")
  })
  .then(function (data) {
    test.deepEqual(data['Item']['postIds'].SS, ['3a', '3b', '3c'], "postIds should be ['3a', '3b', '3c']")
  })
})

// put a list of numbers and check if they exist
builder.add(function testNumberSetPut(test) {
  var self = this
  return this.client.putItem("user", {
    userId: 'userD',
    column: '@',
    postIds: [1, 2, 3]
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userD", "@")
  })
  .then(function (data) {
    test.deepEqual(data['Item']['postIds'].NS, [1, 2, 3], "postIds should be [1, 2, 3]")
  })
})

// get the set of strings
builder.add(function testStringSetRetrieve(test) {
  return this.client.getItem('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .execute()
    .then(function (data) {
      test.deepEqual(data.result.postIds, ['1a', '1b', '1c'], "postIds should be ['1a', '1b', '1c']")
    })
})

// get the set of numbers
builder.add(function testNumberSetRetrieve(test) {
  return this.client.getItem('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .execute()
    .then(function (data) {
      test.deepEqual(data.result.postIds, [1, 2, 3], "postIds should be [1, 2, 3]")
    })
})

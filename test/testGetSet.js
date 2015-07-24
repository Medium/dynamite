var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)
var initialData = [  {"userId": "userA", "column": "@", "postIds": ['1a', '1b', '1c']}
                   , {"userId": "userB", "column": "@", "postIds": [1, 2, 3]}
                   , {"userId": "userC", "column": "@"}]

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

// get a set that doesn't exist
builder.add(function testSetDoesNotExist(test) {
  return this.client.getItem('user')
    .setHashKey('userId', 'userC')
    .setRangeKey('column', '@')
    .execute()
    .then(function (data) {
      test.equal(data.result.postIds, undefined, "postIds should not exist for userC")
    })
})

var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var errors = require('../lib/errors')
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

// override all string set fields
builder.add(function testStringSetPutOverride(test) {
  var self = this
  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    otherIds: ['3a', '3b', '3c']
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['postIds'], undefined, "postIds should not exist")
    test.deepEqual(data['Item']['otherIds'].SS, ['3a', '3b', '3c'], "otherIds should be ['3a', '3b', '3c']")
  })
})

// override all number set fields
builder.add(function testNumberSetPutOverride(test) {
  var self = this
  return this.client.putItem("user", {
    userId: 'userB',
    column: '@',
    otherIds: [4, 5, 6]
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userB", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['postIds'], undefined, "postIds should not exist")
    test.deepEqual(data['Item']['otherIds'].NS, [4, 5, 6], "otherIds should be [4, 5, 6]")
  })
})

// override all number set fields with a string set
builder.add(function testNumberSetPutOverrideWithStringSet(test) {
  var self = this
  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    otherIds: [4, 5, 6]
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['postIds'], undefined, "postIds should not exist")
    test.deepEqual(data['Item']['otherIds'].NS, [4, 5, 6], "otherIds should be [4, 5, 6]")
  })
})

// put string set with successful conditional exists
builder.add(function testStringSetPutWithConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
  .expectAttributeEquals('postIds', ['1a', '1b', '1c'])

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    otherIds: ['5a', '5b', '5c']
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['otherIds'].SS[0], '5a', "otherIds[0] should be 5a")
  })
})

// put string set with successful absent conditional exists
builder.add(function testStringSetPutWithAbsentConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
  .expectAttributeAbsent('otherIds')

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    otherIds: ['5a', '5b', '5c']
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['otherIds'].SS[0], '5a', "otherIds[0] should be 5a")
  })
})

// put with successful absent conditional doesn't exist
builder.add(function testStringSetPutWithAbsentConditionalDoesntExist(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
  .expectAttributeAbsent('otherIds')

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    otherIds: ['5a', '5b', '5c']
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['otherIds'].SS[0], '5a', "otherIds[0] should be 5a")
  })
})

// put set with failed conditional exists
builder.add(function testStringSetPutWithFailedConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
  .expectAttributeEquals('postIds', ['a', 'b', 'c'])

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    otherIds: ['5a', '5b', '5c']
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.fail("'testStringSetPutWithFailedConditional' failed")
  })
  .fail(this.client.throwUnlessConditionalError)
})

// put set with failed conditional doesn't exist
builder.add(function testStringSetPutWithFailedConditionalForNoRecord(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
  .expectAttributeEquals('postIds', ['a', 'b', 'c'])

  return this.client.putItem("user", {
    userId: 'userC',
    column: '@',
    otherIds: ['5a', '5b', '5c']
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userC", "@")
  })
  .then(function (data) {
    test.fail("'testStringSetPutWithFailedConditionalForNoRecord' failed")
  })
  .fail(this.client.throwUnlessConditionalError)
})

// put set with failed absent conditional exists
builder.add(function testStringSetPutWithFailedConditionalExists(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
  .expectAttributeAbsent('postIds', ['1a', '1b', '1c'])

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    otherIds: ['5a', '5b', '5c']
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.fail("'testStringSetPutWithFailedConditionalForNoRecord' failed")
  })
  .fail(this.client.throwUnlessConditionalError)
})

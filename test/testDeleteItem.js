// Copyright 2013 The Obvious Corporation.

var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var typ = require('typ')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)
var initialData = [{"userId": "userA", "column": "@", "age": "29"}]

// basic setup for the tests, creating record userA with range key @
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

// check that an item can be deleted
builder.add(function testDeleteExistingItem(test) {
  var self = this

  return this.client.deleteItem('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .execute()
    .then(function (data) {
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item'], undefined, "User should be deleted~~~" + JSON.stringify(data))
    })
})

// check that an item isn't inadvertently deleted when deleting another
builder.add(function testDeleteNonexistingItem(test) {
  var self = this

  return this.client.deleteItem('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .execute()
    .then(function (data) {
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(typ.isNullish(data['Item']), false, "User should not be deleted")
    })
})

// check that an item matches a conditional when deleting
builder.add(function testDeleteExistingItemWithConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('column', '@')

  return this.client.deleteItem('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .execute()
    .then(function (data) {
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item'], undefined, "User should be deleted")
    })
})

// check that an item matches an absent conditional when deleting
builder.add(function testDeleteExistingItemWithConditionalAbsent(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('height')

  return this.client.deleteItem('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .execute()
    .then(function (data) {
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item'], undefined, "User should be deleted")
    })
})

// check that an item fails a conditional when deleting
builder.add(function testDeleteExistingItemWithFailedConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('column', 'bug')

  return this.client.deleteItem('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .execute()
    .then(function () {
      test.fail("'testDeleteExistingItemWithFailedConditional' failed")
    })
    .fail(this.client.throwUnlessConditionalError)
})

// check that an item fails an absent conditional when deleting
builder.add(function testDeleteExistingItemWithFailedAbsentConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('age')

  return this.client.deleteItem('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .execute()
    .then(function () {
      test.fail("'testDeleteExistingItemWithFailedAbsentConditional' failed")
    })
    .fail(this.client.throwUnlessConditionalError)
})

// check that non-existent items can't be deleted if a conditional expects a value
builder.add(function testDeleteNonexistingItemWithConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('column', '@')

  return this.client.deleteItem('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .execute()
    .then(function (e) {
      test.fail("'testDeleteNonexistingItemWithConditional' failed")
    })
    .fail(this.client.throwUnlessConditionalError)
})

// check that non-existent items can't be deleted if a conditional expects a value
builder.add(function testDeleteNonexistingItemWithConditionalAbsent(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('column')

  return this.client.deleteItem('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .execute()
    .then(function (data) {
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(typ.isNullish(data['Item']), false, "User should not be deleted")
    })
})

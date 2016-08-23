// Copyright 2013 The Obvious Corporation.

var utils = require('./utils/testUtils.js')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)
var errors = require('../lib/errors')

var onError = console.error.bind(console)
var initialData = [{"userId": "userA", "column": "@", "age": "29"}]

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

builder.add(function testSetInvalidReturnValue(test) {
  var putBuilder = this.client.putItem('user', {
    userId: 'userB',
    age: 30
  })

  test.throws(function () {
    putBuilder.setReturnValues('ALL_SOMETHING')
  }, errors.InvalidReturnValuesError)

  test.done()
})

// put an item and check that it exists
builder.add(function testSimplePut(test) {
  var self = this
  return this.client.putItem("user", {
    userId: 'userB',
    column: '@',
    age: 30
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userB", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['age'].N, "30", "Age should be set")
  })
})

builder.add(function testPutItemWithReturnValuesNone(test) {
  var self = this
  return this.client.putItem("user", {
    userId: 'userB',
    column: '@',
    age: 30
  })
  .setReturnValues('NONE')
  .execute()
  .then(function (data) {
    test.equal(data.result, undefined)

    return utils.getItemWithSDK(self.db, "userB", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['age'].N, "30", "Age should be set")
  })
})

// put overrides all fields
builder.add(function testOverridePut(test) {
  var self = this
  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    height: 72
  })
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['age'], undefined, "Age should be undefined")
    test.equal(data['Item']['height'].N, "72", "Height should be 72")
  })
})

// put with successful conditional exists
builder.add(function testPutWithConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('age', 29)

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    height: 72
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['height'].N, "72", "Height should be 72")
  })
})

// put with successful absent conditional exists
builder.add(function testPutWithAbsentConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('height')

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    height: 72
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userA", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['height'].N, "72", "Height should be 72")
  })
})

// put with successful absent conditional doesn't exist
builder.add(function testPutWithAbsentConditionalAndNoRecord(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('age')

  return this.client.putItem("user", {
    userId: 'userB',
    column: '@',
    height: 72
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    return utils.getItemWithSDK(self.db, "userB", "@")
  })
  .then(function (data) {
    test.equal(data['Item']['height'].N, "72", "Height should be 72")
  })
})

// put with failed conditional exists
builder.add(function testPutWithFailedConditional(test) {
  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('age', 30)

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    height: 72
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    test.fail("'testPutWithFailedConditional' failed")
  })
  .fail(this.client.throwUnlessConditionalError)
})

// put with failed conditional doesn't exist
builder.add(function testPutWithFailedConditionalForNoRecord(test) {
  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('age', 29)

  return this.client.putItem("user", {
    userId: 'userB',
    column: '@',
    height: 72
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    test.fail("'testPutWithFailedConditionalForNoRecord' failed")
  })
  .fail(this.client.throwUnlessConditionalError)
})

// put set with failed absent conditional exists
builder.add(function testPutWithFailedAbsentConditionalExists(test) {
  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('age')

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    height: 72
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    test.fail("'testPutWithFailedAbsentConditionalExists' failed")
  })
  .fail(this.client.throwUnlessConditionalError)
})

// trigger a conditional error and check its content
builder.add(function testConditionalErrorFormat(test) {
  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('age')

  return this.client.putItem("user", {
    userId: 'userA',
    column: '@',
    height: 72
  })
  .withCondition(conditions)
  .execute()
  .then(function () {
    test.fail('This putItem request should fail due to a conditional error')
  })
  .fail(function (err) {
    test.ok(err instanceof errors.ConditionalError)
    test.equals('The conditional request failed', err.message, 'The "message" field should be the custom message')
    test.equals('The conditional request failed', err.details, 'The "details" field should be the custom message')
    test.equals('user', err.table, 'The "table" field should be the right table name')
    test.ok(!!err.requestId, 'The "requestId" field should exist')
  })
})

// Copyright 2013 The Obvious Corporation

var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)
var Client = require('../lib/Client')
var FakeDynamo = require('../lib/FakeDynamo')
var typeUtil = require('../lib/TypeUtil')

var utils = require('./utils/testUtils.js')

var onError = console.error.bind(console)
var userA = {'userId': 'userA', 'column': '@', 'age': '29'}

var db, client
exports.setUp = function (done) {
  db = new FakeDynamo()
  client = new Client({dbClient: db})

  var table = db.createTable('user')
  table.setHashKey('userId', 'S')
  table.setRangeKey('column', 'S')
  table.setData({userA: {'@': userA}})
  done()
}

builder.add(function testConditionalUpdateFails(test) {
  var conditions = client.newConditionBuilder()
    .expectAttributeEquals('userId', 'gibberish')

  return client.newUpdateBuilder('user')
    .setHashKey('userId', 'gibberish')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .putAttribute('age', 30)
    .execute()
    .then(function () {
      test.fail('Expected conditional error')
    })
    .fail(function (e) {
      test.ok(client.isConditionalError(e))
      throw e
    })
    .fail(client.throwUnlessConditionalError)
})

builder.add(function testConditionalUpdateOk(test) {
  var conditions = client.newConditionBuilder()
    .expectAttributeEquals('userId', 'userA')

  return client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .putAttribute('age', 30)
    .execute()
    .then(function () {
      return client.getItem('user')
        .setHashKey('userId', 'userA')
        .setRangeKey('column', '@')
        .execute()
    })
    .then(function (data) {
      test.equal(data.result.age, 30)
    })
})

builder.add(function testBatchGetDupeKeys(test) {
  // Real Dynamo throws an exception if a BatchGet has duplicate keys.
  // Ruby FakeDynamo does not have this validation.
  return client.newBatchGetBuilder()
    .requestItems('user', [{'userId': 'userA', 'column': '@'},
                           {'userId': 'userA', 'column': '@'}])
    .execute()
    .then(function () {
      test.fail('Expected validation failure')
    })
    .fail(function (e) {
      if (!/Provided list of item keys contains duplicates/.test(e.message)) {
        throw e
      }
    })
})

builder.add(function testConditionalBuilderMethods(test) {
  var expected = client.newConditionBuilder()
    .expectAttributeEquals('userId', 'gibberish')
    .expectAttributeAbsent('userId2')

  var actual = client.conditions({userId: 'gibberish', 'userId2': null})
  test.deepEqual(expected, actual)

  test.done()
})

builder.add(function testScan(test) {
  db.getTable('user').setData({
    'userA': {
        '1': {userId: 'userA', column: '1', age: '27'},
        '2': {userId: 'userA', column: '2', age: '28'},
        '3': {userId: 'userA', column: '3', age: '29'},
    },
    'userB': {
        '1': {userId: 'userB', column: '1', age: '29'},
    }
  })
  return client.newScanBuilder('user')
      .execute()
      .then(function (data) {
        var result = data.result
        test.deepEqual(result[0], {userId: 'userA', column: '1', age: '27'})
        test.deepEqual(result[1], {userId: 'userA', column: '2', age: '28'})
        test.deepEqual(result[2], {userId: 'userA', column: '3', age: '29'})
        test.deepEqual(result[3], {userId: 'userB', column: '1', age: '29'})
      })
})

builder.add(function testScanWithLimit(test) {
  db.getTable('user').setData({
    'userA': {
        '1': {userId: 'userA', column: '1', age: '27'},
        '2': {userId: 'userA', column: '2', age: '28'},
        '3': {userId: 'userA', column: '3', age: '29'},
    }
  })
  return client.newScanBuilder('user')
      .setLimit(2)
      .execute()
      .then(function (data) {
        var result = data.result
        test.deepEqual(result[0], {userId: 'userA', column: '1', age: '27'})
        test.deepEqual(result[1], {userId: 'userA', column: '2', age: '28'})
        test.deepEqual(data.LastEvaluatedKey, typeUtil.packObjects({userId: 'userA', column: '2'}))
      })
})

builder.add(function testScanWithStartKey(test) {
  db.getTable('user').setData({
    'userA': {
        '1': {userId: 'userA', column: '1', age: '27'},
        '2': {userId: 'userA', column: '2', age: '28'},
        '3': {userId: 'userA', column: '3', age: '29'},
    },
    'userB': {
        '1': {userId: 'userB', column: '1', age: '29'},
    }
  })
  return client.newScanBuilder('user')
      .setStartKey(typeUtil.packObjects({userId: 'userA', column: '2'}))
      .execute()
      .then(function (data) {
        var result = data.result
        test.deepEqual(result[0], {userId: 'userA', column: '3', age: '29'})
        test.deepEqual(result[1], {userId: 'userB', column: '1', age: '29'})
      })
})

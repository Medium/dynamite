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
        test.deepEqual(data.LastEvaluatedKey, {userId: 'userA', column: '2'})
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
      .setStartKey({userId: 'userA', column: '2'})
      .execute()
      .then(function (data) {
        var result = data.result
        test.deepEqual(result[0], {userId: 'userA', column: '3', age: '29'})
        test.deepEqual(result[1], {userId: 'userB', column: '1', age: '29'})
      })
})


// test querying secondary index using greater than condition
builder.add(function testQueryOnSecondaryIndexGreaterThan(test) {
  db.getTable('user').setData({
    'userA': {
        '1': {userId: 'userA', column: '1', age: '27'},
        '2': {userId: 'userA', column: '2', age: '28'},
        '3': {userId: 'userA', column: '3', age: '29'},
        '4': {userId: 'userA', column: '4', age: '30'},
    },
    'userB': {
        '1': {userId: 'userB', column: '1', age: '29'},
    }
  })
  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexGreaterThan('age', 28)
    .execute()
    .then(function (data) {
      for (var i = 0; i < data.result.length; i++) {
        test.equal(data.result[i].age > 28, true, "Age should be greater than 28")
      }
      test.equal(data.result.length, 2, '"2" should be returned')
    })
})

// test querying secondary index using equals condition
builder.add(function testQueryOnSecondaryIndexEquals(test) {
  db.getTable('user').setData({
    'userA': {
        '1': {userId: 'userA', column: '1', age: '27'},
        '2': {userId: 'userA', column: '2', age: '28'},
        '3': {userId: 'userA', column: '3', age: '29'},
        '4': {userId: 'userA', column: '4', age: '30'},
    },
    'userB': {
        '1': {userId: 'userB', column: '1', age: '29'},
    }
  })
  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexEqual('age', 28)
    .execute()
    .then(function (data) {
      test.equal(data.result[0].age, 28, "Age should match 28")
      test.equal(data.result.length, 1, '"1" should be returned')
    })
})

// test querying secondary index that have repeated column values
// this is a test for a regression where fake dynamo may reinsert values
// when the keys match again
builder.add(function testQueryOnMultipleIndexes(test) {
  db.getTable('user').setData({
    'userA': {
        '1': {userId: 'userA', column: '1', age: '27'},
        '2': {userId: 'userA', column: '2', age: '28'},
        '3': {userId: 'userA', column: '3', age: '29'},
        '4': {userId: 'userA', column: '4', age: '30'},
        '5': {userId: 'userA', column: '5', age: '30'},
    },
    'userB': {
        '1': {userId: 'userB', column: '1', age: '29'},
    }
  })

  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexGreaterThanEqual('age', 28)
    .execute()
    .then(function (data) {
      test.equal(data.result[0].age, 28, "Age should match 28")
      test.equal(data.result[1].age, 29, "Age should match 29")
      test.equal(data.result[2].age, 30, "Age should match 30")
      test.equal(data.result[3].age, 30, "Age should match 30")
      test.equal(data.result.length, 4, '"4" should be returned')
    })
})

builder.add(function testQueryWithLimit(test) {
  db.getTable('user').setData({
    'userA': {
        '1': {userId: 'userA', column: '1', age: '27'},
        '2': {userId: 'userA', column: '2', age: '28'},
        '3': {userId: 'userA', column: '3', age: '29'},
        '4': {userId: 'userA', column: '4', age: '30'}
    }
  })

  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexGreaterThanEqual('age', 28)
    .setLimit(2)
    .execute()
    .then(function (data) {
      test.equal(data.result[0].age, 28)
      test.equal(data.result[1].age, 29)
      test.equal(data.result.length, 2)
      test.deepEqual(data.LastEvaluatedKey, {userId: 'userA', column: '3'})

      return client.newQueryBuilder('user')
        .setStartKey(data.LastEvaluatedKey)
        .setHashKey('userId', 'userA')
        .setIndexName('age-index')
        .indexGreaterThanEqual('age', 28)
        .execute()
    })
    .then(function (data) {
      test.equal(data.result[0].age, 30)
      test.equal(data.result.length, 1)
    })
})

builder.add(function testQueryWithMaxResultSize(test) {
  db.getTable('user').setData({
    'userA': {
        '1': {userId: 'userA', column: '1', age: '27'},
        '2': {userId: 'userA', column: '2', age: '28'},
        '3': {userId: 'userA', column: '3', age: '29'},
        '4': {userId: 'userA', column: '4', age: '30'}
    }
  })
  db.getTable('user').setMaxResultSetSize(1)

  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexGreaterThanEqual('age', 28)
    .setLimit(2)
    .execute()
    .then(function (data) {
      test.equal(data.result[0].age, 28)
      test.equal(data.result.length, 1)
      test.deepEqual(data.LastEvaluatedKey, {userId: 'userA', column: '2'})
    })
})

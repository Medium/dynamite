// Copyright 2013 The Obvious Corporation

var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)
var Client = require('../lib/Client')
var FakeDynamo = require('../lib/FakeDynamo')
var typeUtil = require('../lib/TypeUtil')
var utils = require('./utils/testUtils.js')

var onError = console.error.bind(console)
var userA = {'userId': 'userA', 'column': '@', 'age': 29}

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
      test.equal(data.result.age, 30, 'Age should match 30')
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
        1: {userId: 'userA', column: '1', age: 27},
        2: {userId: 'userA', column: '2', age: 28},
        3: {userId: 'userA', column: '3', age: 29},
    },
    'userB': {
        1: {userId: 'userB', column: '1', age: 29},
    }
  })
  return client.newScanBuilder('user')
      .execute()
      .then(function (data) {
        var result = data.result
        test.deepEqual(result[0], {userId: 'userA', column: '1', age: 27})
        test.deepEqual(result[1], {userId: 'userA', column: '2', age: 28})
        test.deepEqual(result[2], {userId: 'userA', column: '3', age: 29})
        test.deepEqual(result[3], {userId: 'userB', column: '1', age: 29})
      })
})

builder.add(function testScanWithLimit(test) {
  db.getTable('user').setData({
    'userA': {
        1: {userId: 'userA', column: '1', age: 27},
        2: {userId: 'userA', column: '2', age: 28},
        3: {userId: 'userA', column: '3', age: 29},
    }
  })
  return client.newScanBuilder('user')
      .setLimit(2)
      .execute()
      .then(function (data) {
        var result = data.result
        test.deepEqual(result[0], {userId: 'userA', column: '1', age: 27})
        test.deepEqual(result[1], {userId: 'userA', column: '2', age: 28})
        test.deepEqual(data.LastEvaluatedKey, {userId: 'userA', column: '2'})
      })
})

builder.add(function testScanWithStartKey(test) {
  db.getTable('user').setData({
    'userA': {
        1: {userId: 'userA', column: '1', age: 27},
        2: {userId: 'userA', column: '2', age: 28},
        3: {userId: 'userA', column: '3', age: 29},
    },
    'userB': {
        1: {userId: 'userB', column: '1', age: 29},
    }
  })
  return client.newScanBuilder('user')
      .setStartKey({userId: 'userA', column: '2'})
      .execute()
      .then(function (data) {
        var result = data.result
        test.deepEqual(result[0], {userId: 'userA', column: '3', age: 29})
        test.deepEqual(result[1], {userId: 'userB', column: '1', age: 29})
      })
})


// test querying secondary index using greater than condition
builder.add(function testQueryOnSecondaryIndexGreaterThan(test) {
  db.getTable('user').setData({
    'userA': {
        1: {userId: 'userA', column: 1, age: 27},
        2: {userId: 'userA', column: 2, age: 28},
        3: {userId: 'userA', column: 3, age: 3000},
        4: {userId: 'userA', column: 4, age: 29},
    },
    'userB': {
        1: {userId: 'userB', column: '1', age: 29},
    }
  })
  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexGreaterThan('age', 28)
    .execute()
    .then(function (data) {
      test.equal(data.result.length, 2, '2 results should be returned')
      test.equal(data.result[0].age, 29, 'First entry should be 29')
      test.equal(data.result[1].age, 3000, 'Second entry should be 3000')
    })
})

// test querying secondary index using less than condition
builder.add(function testQueryOnSecondaryIndexLessThan(test) {
  db.getTable('user').setData({
    'userA': {
        0: {userId: 'userA', column: 0, age: 27},
        1: {userId: 'userA', column: 1, age: 26},
        2: {userId: 'userA', column: 2, age: 28},
        3: {userId: 'userA', column: 3, age: 30},
        4: {userId: 'userA', column: 4, age: 29},
    },
    'userB': {
        1: {userId: 'userB', column: '1', age: 29},
    }
  })
  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexLessThan('age', 29)
    .scanBackward()
    .execute()
    .then(function (data) {
      test.equal(data.result[0].age, 28, 'First entry should be 28')
      test.equal(data.result[1].age, 27, 'Second entry should be 27')
      test.equal(data.result[2].age, 26, 'Third entry should be 26')
      test.equal(data.result.length, 3, '3 results should be returned')
    })
})

// test querying secondary index using less than equals condition
builder.add(function testQueryOnSecondaryIndexLessThanEquals(test) {
  db.getTable('user').setData({
    'userA': {
        0: {userId: 'userA', column: 0, age: 27},
        1: {userId: 'userA', column: 1, age: 26},
        2: {userId: 'userA', column: 2, age: 28},
        3: {userId: 'userA', column: 3, age: 29},
        4: {userId: 'userA', column: 4, age: 30},
    },
    'userB': {
        1: {userId: 'userB', column: '1', age: 29},
    }
  })
  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexLessThanEqual('age', 29)
    .scanBackward()
    .execute()
    .then(function (data) {
      test.equal(data.result[0].age, 29, 'First entry should be 29')
      test.equal(data.result[1].age, 28, 'Second entry should be 28')
      test.equal(data.result[2].age, 27, 'Third entry should be 27')
      test.equal(data.result[3].age, 26, 'Fourth entry should be 26')
      test.equal(data.result.length, 4, '4 results should be returned')
    })
})


// test querying secondary index using equals condition
builder.add(function testQueryOnSecondaryIndexEquals(test) {
  db.getTable('user').setData({
    'userA': {
        1: {userId: 'userA', column: 1, age: 27},
        2: {userId: 'userA', column: 2, age: 28},
        3: {userId: 'userA', column: 3, age: 29},
        4: {userId: 'userA', column: 4, age: 30},
    },
    'userB': {
        1: {userId: 'userB', column: '1', age: 29},
    }
  })
  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexEqual('age', 28)
    .execute()
    .then(function (data) {
      test.equal(data.result[0].age, 28, 'Age should match 28')
      test.equal(data.result.length, 1, '1 result should be returned')
    })
})

// test querying secondary index that have repeated column values
// this is a test for a regression where fake dynamo may reinsert values
// when the keys match again
builder.add(function testQueryOnMultipleIndexes(test) {
  db.getTable('user').setData({
    'userA': {
        1: {userId: 'userA', column: '1', age: 27},
        2: {userId: 'userA', column: '2', age: 28},
        3: {userId: 'userA', column: '3', age: 29},
        4: {userId: 'userA', column: '4', age: 30},
        5: {userId: 'userA', column: '5', age: 30},
    },
    'userB': {
        1: {userId: 'userB', column: '1', age: 29},
    }
  })

  return client.newQueryBuilder('user')
    .setHashKey('userId', 'userA')
    .setIndexName('age-index')
    .indexGreaterThanEqual('age', 28)
    .execute()
    .then(function (data) {
      test.equal(data.result[0].age, 28, 'Age should match 28')
      test.equal(data.result[1].age, 29, 'Age should match 29')
      test.equal(data.result[2].age, 30, 'Age should match 30')
      test.equal(data.result[3].age, 30, 'Age should match 30')
      test.equal(data.result.length, 4, '4 results should be returned')
    })
})

/**
 * Basic test for Global Secondary Index support
 * Does not currently support testing for existence of GSIs
 */
builder.add(function testQueryOnGlobalSecondaryIndexes(test) {
  // It is important in this test to set the hash key of the table
  // That way we know that it is a GSI query
  db.getTable('user').setHashKey('userId', 'S')
    .setData({
    'userA': {
        1: {userId: 'userA', column: '1', age: 27, height: 160},
        2: {userId: 'userA', column: '2', age: 28, height: 170},
        3: {userId: 'userA', column: '3', age: 28, height: 180},
        4: {userId: 'userA', column: '4', age: 29, height: 150},
    },
    'userB': {
        1: {userId: 'userB', column: '1', age: 27, height: 200},
        2: {userId: 'userB', column: '2', age: 28, height: 170},
        3: {userId: 'userB', column: '3', age: 28, height: 180},
        4: {userId: 'userB', column: '4', age: 29, height: 190},
    }
  })

  return client.newQueryBuilder('user')
    .setHashKey('age', 28)
    .setIndexName('age-height-gsi')
    .indexGreaterThan('height', 175)
    .execute()
    .then(function (data) {
      // results from userA
      test.equal(data.result[0].age, 28, 'Age should match 28')
      test.equal(data.result[0].height, 180, 'Height should match 180')

      // results from userB
      test.equal(data.result[1].age, 28, 'Age should match 28')
      test.equal(data.result[1].height, 180, 'Height should match 180')

      test.equal(data.result.length, 2, '2 results should be returned')
    })
})

builder.add(function testQueryWithLimit(test) {
  db.getTable('user').setData({
    'userA': {
        1: {userId: 'userA', column: '1', age: 27},
        2: {userId: 'userA', column: '2', age: 28},
        3: {userId: 'userA', column: '3', age: 29},
        4: {userId: 'userA', column: '4', age: 30}
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
      test.equal(data.result[0].age, 30, 'Age should match 30')
      test.equal(data.result.length, 1, '1 result should be returned')
    })
})

builder.add(function testQueryWithMaxResultSize(test) {
  db.getTable('user').setData({
    'userA': {
        1: {userId: 'userA', column: '1', age: 27},
        2: {userId: 'userA', column: '2', age: 28},
        3: {userId: 'userA', column: '3', age: 29},
        4: {userId: 'userA', column: '4', age: 30}
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
      test.equal(data.result.length, 1, '1 result should be returned')
      test.equal(data.result[0].age, 28, 'Age should match 28')
      test.deepEqual(data.LastEvaluatedKey, {userId: 'userA', column: '2'})
    })
})

builder.add(function testDescribeTable(test) {
  return client.describeTable('user')
    .execute()
    .then(function (data) {
      var tableDescription = data.Table
      var attributes = tableDescription.AttributeDefinitions
      var keySchema = tableDescription.KeySchema

      test.ok(tableDescription, 'Table description should exist.')
      test.equal(attributes.length, 2, 'Table should have 2 attributes in AttributeDefinitions (keys).')
      test.equal(keySchema.length, 2, 'Table should have 2 attributes in KeySchema (keys).')
      test.equal(tableDescription.TableName, 'user', 'Table name should be user.')
      test.equal(tableDescription.TableStatus, 'ACTIVE', 'Table status should be active.')

      // deep check attributes
      for (var i = 0; i < attributes.length; i++) {
        var attribute = attributes[i]
        if (attribute.AttributeName == 'userId') {
          test.deepEqual(attribute, {AttributeName: 'userId', AttributeType: 'S'})
        } else if (attribute.AttributeName == 'column') {
          test.deepEqual(attribute, {AttributeName: 'column', AttributeType: 'S'})
        }
      }

      // deep check key schemas
      for (var i = 0; i < keySchema.length; i++) {
        var key = keySchema[i]
        if (key.AttributeName == 'userId') {
          test.deepEqual(key, {AttributeName: 'userId', KeyType: 'HASH'})
        } else if (key.AttributeName == 'column') {
          test.deepEqual(key, {AttributeName: 'column', KeyType: 'RANGE'})
        }
      }

      test.expect(9) // make sure the tests in conditionals ran
    })
})

builder.add(function testLongKey(test) {
  // Create a string 2^10 chars long.
  var str = '.'
  for (var i = 0; i < 10; i++) {
    str = str + str
  }

  return client.getItem('user')
      .setHashKey('userId', 'userA')
      .setRangeKey('column', str)
      .execute()
  .then(function () {
    test.fail('Expected validation exception')
  })
  .fail(function (e) {
    if (!client.isValidationError(e)) throw e
  })
})

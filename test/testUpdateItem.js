// Copyright 2013 The Obvious Corporation.

var utils = require('./utils/testUtils.js')
var errors = require('../lib/errors')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)
var initialData = [{
  "userId": "userA",
  "column": "@",
  "age": "29",
  "someStringSet": ['a', 'b', 'c']
}]

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

builder.add(function testSetInvalidReturnValue(test) {
  var updateBuilder = this.client.newUpdateBuilder('user')

  test.throws(function () {
    updateBuilder.setReturnValues('ALL_SOMETHING')
  }, errors.InvalidReturnValuesError)

  test.done()
})

// test putting an attribute for an existing record
builder.add(function testPutAttributeExisting(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .enableUpsert()
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .execute()
    .then(function (data) {
      test.equal(data.result.age, 30, 'result age should be 30')
      test.equal(data.result.height, 72, 'result height should be 72')
      test.equal(data.previous.age, 29, 'previous age should be 29')
      test.equal(data.previous.height, undefined, 'previous height should be undefined')
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'].N, "30", "result age should be 30")
      test.equal(data['Item']['height'].N, "72", "height should be 72")
    })
})

// test putting an attribute for a non-existing record
builder.add(function testPutAttributeNonExisting(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .enableUpsert()
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .execute()
    .then(function (data) {
      test.equal(data.previous, null, 'previous should be null')
      test.equal(data.result.age, 30, 'result age should be 30')
      test.equal(data.result.height, 72, 'result height should be 72')
      return utils.getItemWithSDK(self.db, "userB", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'].N, "30", "result age should be 30")
      test.equal(data['Item']['height'].N, "72", "Height should be 72")
    })
})

//test putting attributes with empty would succeed
builder.add(function testPutAttributeEmpty(test) {
  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .enableUpsert()
    .putAttribute('name', '')
    .execute()
    .then(function () {
      test.fail("'testPutAttributeEmpty' failed - the query is expected to fail, but it didn't.")
    })
    .fail(function (e) {
      test.equal(e.message.indexOf('An AttributeValue may not contain an empty') !== -1, true, "Conditional request should fail")
    })
})

// test adding an attribute for an existing record
builder.add(function testAddAttributeExisting(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .enableUpsert()
    .addToAttribute('age', 1)
    .addToAttribute('views', -1)
    .execute()
    .then(function (data) {
      test.equal(data.result.age, 30, 'result age should be 30')
      test.equal(data.result.views, -1, 'result views should be -1')
      test.equal(data.previous.age, 29, 'previous age should be 29')
      test.equal(data.previous.views, undefined, 'previous views should be undefined')
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'].N, "30", "result age should be 30")
      test.equal(data['Item']['views'].N, "-1", "views should be -1")
    })
})

// test adding an attribute for a non-existing record
builder.add(function testAddAttributeNonExisting(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .enableUpsert()
    .addToAttribute('age', 1)
    .addToAttribute('views', -1)
    .execute()
    .then(function (data) {
      test.equal(data.result.age, 1, 'result age should be 30')
      test.equal(data.result.views, -1, 'views should be -1')
      return utils.getItemWithSDK(self.db, "userB", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'].N, "1", "result age should be 1")
      test.equal(data['Item']['views'].N, "-1", "views should be -1")
    })
})

// test deleting an attribute for an existing record
builder.add(function testDeleteAttributeExisting(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .enableUpsert()
    .deleteAttribute('age')
    .deleteAttribute('height')
    .deleteFromAttribute('someStringSet', ['b', 'c', 'd'])
    .execute()
    .then(function (data) {
      test.equal(data.result.age, undefined, 'result age should be undefined')
      test.equal(data.result.height, undefined, 'height should be undefined')
      test.deepEqual(data.result.someStringSet, ['a'], 'someStringSet should contain "a"')
      test.equal(data.previous.age, 29, 'previous age should be 29')
      test.equal(data.previous.height, undefined, 'height should be undefined')
      test.deepEqual(data.previous.someStringSet, ['a', 'b', 'c'], 'someStringSet should contain "a", "b", "c"')
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'], undefined, 'result age should be undefined')
      test.equal(data['Item']['height'], undefined, 'height should be undefined')
      test.deepEqual(data['Item']['someStringSet'].SS, ['a'], 'someStringSet should contain only "a"')
    })
})

builder.add(function testDeleteAllItemsFromStringSet(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .enableUpsert()
    .deleteFromAttribute('someStringSet', ['a', 'b', 'c', 'd'])
    .execute()
    .then(function (data) {
      test.deepEqual(data.result.someStringSet, undefined, 'someStringSet should be undefined')
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.deepEqual(data['Item']['someStringSet'], undefined, 'someStringSet should be undefined')
    })
})

// test deleting an attribute for a non-existing record
builder.add(function testDeleteAttributeNonExisting(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .enableUpsert()
    .deleteAttribute('age')
    .deleteAttribute('height')
    .execute()
    .then(function (data) {
      // The data from AWS SDK is something like this:
      // { ConsumedCapacityUnits: 1,
      //   LastEvaluatedKey: undefined,
      //   Count: undefined }
      // whereas the data from dynamo-client is like this:
      // { ConsumedCapacityUnits: 1,
      //   LastEvaluatedKey: undefined,
      //   Count: undefined,
      //   result: {} }
      // so the original testing code is:
      // test.deepEqual(data.result, {}, 'result should be undefined')
      test.deepEqual(data.result, {userId: 'userA', column: '@', someStringSet: ['a', 'b', 'c']}, 'fields should be updated')
      return utils.getItemWithSDK(self.db, "userB", "@")
    })
    .then(function (data) {
      test.equal(data['Item'], undefined, "userB with range key @ should be undefined")
    })
})

// test updating with conditional exists
builder.add(function testUpdateWithConditional(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('column', '@')

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .execute()
    .then(function (data) {
      test.equal(data.result.age, 30, 'result age should be 30')
      test.equal(data.result.height, 72, 'height should be 72')
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'].N, "30", 'result age should be 30')
      test.equal(data['Item']['height'].N, "72", 'height should be 72')
    })
})

// test updating with returnValues set to NONE
builder.add(function testUpdateWithReturnValuesNone(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .enableUpsert()
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .setReturnValues('NONE')
    .execute()
    .then(function (data) {
      test.equal(data.result, undefined)

      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'].N, "30", 'result age should be 30')
      test.equal(data['Item']['height'].N, "72", 'height should be 72')
    })
})

// test updating with absent conditional exists
builder.add(function testUpdateWithAbsentConditionalExists(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('height')

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .execute()
    .then(function (data) {
      test.equal(data.result.age, 30, 'result age should be 30')
      test.equal(data.result.height, 72, 'height should be 72')
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'].N, "30", 'result age should be 30')
      test.equal(data['Item']['height'].N, "72", 'height should be 72')
    })
})

// test updating with absent conditional doesn't exist
builder.add(function testUpdateWithAbsentConditionalDoesNotExist(test) {
  var self = this

  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('height')

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .execute()
    .then(function (data) {
      test.equal(data.result.age, 30, 'result age should be 30')
      test.equal(data.result.height, 72, 'height should be 72')
      return utils.getItemWithSDK(self.db, "userB", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['age'].N, "30", 'result age should be 30')
      test.equal(data['Item']['height'].N, "72", 'height should be 72')
    })
})

// Test that deleting from a non-existant record upserts a new item
builder.add(function testUpdateWithDeleteAttributeDoesNotExist(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userC')
    .setRangeKey('column', '@')
    .enableUpsert()
    .deleteAttribute('age')
    .deleteAttribute('height')
    .execute()
    .then(function (data) {
      test.equal(data.result.userId, 'userC', 'userId should be set')
      return utils.getItemWithSDK(self.db, "userC", "@")
    })
    .then(function (data) {
      test.equal(data.Item.userId.S, 'userC', 'userId should be set')
    })
})

// test updating fails with conditional exists
builder.add(function testUpdateFailsWithConditional(test) {
  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('age', 30)

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .execute()
    .then(function () {
      test.fail("'testUpdateFailsWithConditional' failed - the query is expected to fail, but it didn't.")
    })
    .fail(this.client.throwUnlessConditionalError)
})

// test updating fails with conditional doesnt exist
builder.add(function testUpdateFailsWithConditionalDoesNotExist(test) {
  var conditions = this.client.newConditionBuilder()
    .expectAttributeEquals('age', 30)

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userB')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .execute()
    .then(function () {
      test.fail("'testUpdateFailsWithConditionalDoesNotExist' failed - the query is expected to fail, but it didn't.")
    })
    .fail(this.client.throwUnlessConditionalError)
})

// test updating fails with absent conditional exists
builder.add(function testUpdateFailsWithAbsentConditional(test) {
  var conditions = this.client.newConditionBuilder()
    .expectAttributeAbsent('age')

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .withCondition(conditions)
    .putAttribute('age', 30)
    .putAttribute('height', 72)
    .execute()
    .then(function () {
      test.fail("'testUpdateFailsWithAbsentConditional' failed - the query is expected to fail, but it didn't.")
    })
    .fail(this.client.throwUnlessConditionalError)
})

builder.add(function testUpdateFailsWhenConditionalArgumentBad(test) {
  try {
    this.client.newUpdateBuilder('user')
      .setHashKey('userId', 'userA')
      .setRangeKey('column', '@')
      .withCondition({age: null})
      .putAttribute('age', 30)
      .execute()
    test.fail('Expected error')
  } catch (e) {
    if (!/Expected ConditionBuilder/.test(e.message)) {
      throw e
    }
  }
  test.done()
})

builder.add(function testPutAttributeWithUnderscores(test) {
  var self = this

  return this.client.newUpdateBuilder('user')
    .setHashKey('userId', 'userA')
    .setRangeKey('column', '@')
    .enableUpsert()
    .putAttribute('__age', 30)
    .putAttribute('00height', 72)
    .execute()
    .then(function (data) {
      test.equal(data.result.__age, 30, 'result __age should be 30')
      test.equal(data.result['00height'], 72, 'result 00height should be 72')
      return utils.getItemWithSDK(self.db, "userA", "@")
    })
    .then(function (data) {
      test.equal(data['Item']['__age'].N, "30", "result age should be 30")
      test.equal(data['Item']['00height'].N, "72", "height should be 72")
    })
})

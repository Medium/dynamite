// Copyright 2013 The Obvious Corporation.

var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var Q = require("kew")
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)
var tableName = "comments"
var rawData = [{"postId": "post1", "column": "@", "title": "This is my post", "content": "And here is some content!", "tags": ['foo', 'bar']},
               {"postId": "post1", "column": "/comment/timestamp/002123", "comment": "this is slightly later"},
               {"postId": "post1", "column": "/comment/timestamp/010000", "comment": "where am I?"},
               {"postId": "post1", "column": "/comment/timestamp/001111", "comment": "HEYYOOOOO"},
               {"postId": "post1", "column": "/comment/timestamp/001112", "comment": "what's up?"},
               {"postId": "post1", "column": "/canEdit/user/AAA", "userId": "AAA"}];

// sorted data for checking the order of returned data
var sortedRawData = []
for (var i = 0; i < rawData.length; i++) {
  sortedRawData[i] = rawData[i]
}
sortedRawData.sort(function(obj1, obj2) {
  return obj1.column > obj2.column ? 1 : -1
})

// basic setup for the tests, creating record userA with Index key @
exports.setUp = function (done) {
  this.db = utils.getMockDatabase()
  this.client = utils.getMockDatabaseClient()
  utils.createTable(this.db, tableName, "postId", "column")
    .thenBound(utils.initTable, null, {"db": this.db, "tableName": tableName, "data": rawData})
    .fail(onError)
    .fin(done)
}

exports.tearDown = function (done) {
  utils.deleteTable(this.db, tableName)
    .then(function () {
      done()
    })
}

function checkResults(test, total, offset) {
  return function (data) {
    test.equal(data.result.length, total, total + " records should be returned")
    for (var i = 0; i < data.result.length; i++) {
      test.deepEqual(data.result[i], sortedRawData[i + offset], "Row should be retrieved in the correct order")
    }
  }
}

// test basic query
builder.add(function testBasicQuery(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .execute()
    .then(checkResults(test, 6, 0))
})

// test Index key begins with
builder.add(function testindexBeginsWith(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBeginsWith('column', '/comment/')
    .execute()
    .then(checkResults(test, 4, 1))
})

// test Index key between
builder.add(function testIndexKeyBetween(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/', '/comment/timestamp/009999')
    .execute()
    .then(checkResults(test, 3, 1))
})

// test Index key less than
builder.add(function testIndexKeyLessThan(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexLessThan('column', '/comment/timestamp/001111')
    .execute()
    .then(checkResults(test, 1, 0))
})

// test Index key less than equal
builder.add(function testIndexKeyLessThanEqual(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexLessThanEqual('column', '/comment/timestamp/001111')
    .execute()
    .then(checkResults(test, 2, 0))
})

// test Index key greater than
builder.add(function testIndexKeyGreaterThan(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexGreaterThan('column', '/comment/timestamp/001111')
    .execute()
    .then(checkResults(test, 4, 2))
})

// test Index key greater than equal
builder.add(function testIndexKeyGreaterThanEqual(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexGreaterThanEqual('column', '/comment/timestamp/001111')
    .execute()
    .then(checkResults(test, 5, 1))
})

// test Index key equal
builder.add(function testIndexKeyEqual(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexEqual('column', '/comment/timestamp/001111')
    .execute()
    .then(checkResults(test, 1, 1))
})

// test limit
builder.add(function testLimit(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/', '/comment/timestamp/999999')
    .setLimit(3)
    .execute()
    .then(checkResults(test, 3, 1))
})

// test scan forward
builder.add(function testScanForward(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/', '/comment/timestamp/999999')
    .execute()
    .then(checkResults(test, 4, 1))
})

// test cursoring forward
builder.add(function testCursorForward(test) {
  var client = this.client

  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/', '/comment/timestamp/999999')
    .setLimit(3)
    .execute()
    .then(function (data) {
      return client.newQueryBuilder('comments')
        .setHashKey('postId', 'post1')
        .indexBetween('column', '/comment/', '/comment/timestamp/999999')
        .setStartKey(data.LastEvaluatedKey)
        .execute()
    })
    .then(function (data) {
      test.equal(data.result.length, 1, "1 record should be returned")
      test.equal(data.result[0].comment, "where am I?", "Row comment should be set")
    })
})

// test cursoring backward
builder.add(function testCursorBackward(test) {
  var client = this.client

  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/', '/comment/timestamp/999999')
    .setLimit(3)
    .scanBackward()
    .execute()
    .then(function (data) {
      return client.newQueryBuilder('comments')
        .setHashKey('postId', 'post1')
        .indexBetween('column', '/comment/', '/comment/timestamp/999999')
        .scanBackward()
        .setStartKey(data.LastEvaluatedKey)
        .execute()
    })
    .then(function (data) {
      test.equal(data.result.length, 1, "1 record should be returned")
      test.equal(data.result[0].comment, "HEYYOOOOO", "Row comment should be set")
    })
})

// test select attributes
builder.add(function testSelectAttributes(test) {
  var keyOffset = 1
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/', '/comment/timestamp/999999')
    .selectAttributes(['postId', 'comment'])
    .execute()
    .then(function (data) {
      test.equal(data.result.length, 4, "4 records should be returned")
      for (var i = 0; i < data.result.length; i++) {
        test.equal(data.result[i].comment, sortedRawData[i + keyOffset].comment, "Row comment should be set")
        test.equal(data.result[i].column, undefined, 'Column should not be set')
      }
    })
})

// test set existence
builder.add(function testSetExistence(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexEqual('column', '@')
    .selectAttributes(['postId', 'tags'])
    .execute()
    .then(function (data) {
      test.deepEqual(data.result[0].tags, ['foo', 'bar'], "post should have tags ['foo', 'bar']")
    })
})

// test scan backward
builder.add(function testScanBackward(test) {
  var keyOffset = 1

  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/', '/comment/timestamp/999999')
    .scanBackward()
    .execute()
    .then(function (data) {
      test.equal(data.result.length, 4, "4 records should be returned")
      for (var i = 0; i < data.result.length; i++) {
        test.deepEqual(data.result[i], sortedRawData[(data.result.length - 1 - i) + keyOffset],
                       "Row should be retrieved in the correct order")
      }
    })
})

// test count
builder.add(function testCount(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/timestamp/002123', '/comment/timestamp/999999')
    .getCount()
    .execute()
    .then(function (data) {
      test.equal(data.Count, 2, '"2" should be returned')
    })
})

// test count if it's zero
builder.add(function testCountIfZero(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'postDNE')
    .indexBetween('column', '/comment/timestamp/002123', '/comment/timestamp/999999')
    .getCount()
    .execute()
    .then(function (data) {
      test.equal(data.Count, 0, '"0" should be returned')
    })
})

builder.add(function testNext(test) {
  return this.client.newQueryBuilder('comments')
    .setHashKey('postId', 'post1')
    .indexBetween('column', '/comment/', '/comment/timestamp/999999')
    .setLimit(3)
    .execute()
    .then(function (data) {
      test.equal(3, data.Count)
      test.ok(data.hasNext())
      return data.next()
    })
    .then(function (data) {
      test.equal(1, data.Count)
      test.ok(!data.hasNext())
      return data.next()
    })
    .then(function (data) {
      test.fail('Expected error')
    })
    .fail(function (e) {
      if (e.message !== 'No more results') throw e
    })
})

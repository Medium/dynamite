// Copyright 2013 The Obvious Corporation.

var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var Q = require("kew")
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)
var tableName = "user"
var rawData = [{"userId": "a", "column": "@", "post": "3", "email": "1@medium.com"},
               {"userId": "b", "column": "@", "post": "0", "address": "800 Market St. SF, CA"},
               {"userId": "c", "column": "@", "post": "5", "email": "3@medium"},
               {"userId": "d", "column": "@", "post": "2", "twitter": "haha"},
               {"userId": "e", "column": "@", "post": "2", "twitter": "hoho"},
               {"userId": "f", "column": "@", "post": "4", "description": "Designer", "email": "h@w.com"},
               {"userId": "h", "column": "@", "post": "6", "tags": ['foo', 'bar']}];


// basic setup for the tests, creating record userA with range key @
exports.setUp = function (done) {
  this.db = utils.getMockDatabase()
  this.client = utils.getMockDatabaseClient()
  utils.createTable(this.db, tableName, "userId", "column")
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

/**
 * A helper function that runs a query and check the result.
 *
 * @param query {Query} The query that has been built and ready to execute.
 * @param expect {Array<Integer>} The expected returned results
 * @param test {Object} The test object from nodeunit.
 * @return {Q}
 */
var scanAndCheck = function (scan, expect, test) {
  return scan.execute()
    .then(function (data) {
      test.equal(data.result.length, expect.length, expect.length + " records should be returned")
      data.result.sort(function(a, b) {return (a.userId < b.userId) ? -1 : ((a.userId > b.userId) ? 1 : 0)})
      for (var i = 0; i < data.result.length; i++) {
        test.deepEqual(data.result[i], rawData[expect[i]], "Some records are wrong")
      }
    })
}

// test basic scan on the entire table
builder.add(function testScanAll(test) {
  var scan = this.client.newScanBuilder(tableName)
  return scanAndCheck(scan, [0, 1, 2, 3, 4, 5, 6], test)
})

// test filtering with post == 2
builder.add(function testFilterByEqual(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeEquals("post", 2)
  return scanAndCheck(scan, [3, 4], test)
})

// test filtering with post != 2
builder.add(function testFilterByNotEqual(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeNotEquals("post", 2)
  return scanAndCheck(scan, [0, 1, 2, 5, 6], test)
})

// test filtering with post <= 2
builder.add(function testFilterByLessThanEqual(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeLessThanEqual("post", 2)
  return scanAndCheck(scan, [1, 3, 4], test)
})

// test filtering with post < 2
builder.add(function testFilterByLessThan(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeLessThan("post", 2)
  return scanAndCheck(scan, [1], test)
})

// test filtering with post >= 2
builder.add(function testFilterByGreaterThanEqual(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeGreaterThanEqual("post", 2)
  return scanAndCheck(scan, [0, 2, 3, 4, 5, 6], test)
})

// test filtering with post > 2
builder.add(function testFilterByGreaterThan(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeGreaterThan("post", 2)
  return scanAndCheck(scan, [0, 2, 5, 6], test)
})

// test filtering with not null
builder.add(function testFilterByNotNull(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeNotNull("post")
  return scanAndCheck(scan, [0, 1, 2, 3, 4, 5, 6], test)
  scan = this.client.newScanBuilder(tableName)
           .filterAttributeNotNull("email")
  return scanAndCheck(scan, [0, 2, 5], test)
})

// test filtering with email 'CONTAINS' 'medium'
builder.add(function testFilterByContains(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeContains("email", "medium")
  return scanAndCheck(scan, [0, 2], test)
})

// test filters with tags 'CONTAINS' 'foo'
builder.add(function testFilterBySetContains(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeContains("tags", "foo")
  return scanAndCheck(scan, [6], test)
})

// test filtering with email 'NOT_CONTAINS' 'medium'
builder.add(function testFilterByNotContains(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeNotContains("email", "medium")
  return scanAndCheck(scan, [5], test, "testFilterByNotContains")
})

// test filters with tags 'NOT_CONTAINS' 'baz'
builder.add(function testFilterBySetNotContains(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeNotContains("tags", "baz")
  return scanAndCheck(scan, [6], test, "testFilterBySetNotContains")
})

// test filtering with twitter 'BEGIN_WITH' 'h'
builder.add(function testFilterByBeginWith(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeBeginsWith("twitter", "h")
  return scanAndCheck(scan, [3, 4], test, "testFilterByBeginWith")
})

// test filtering with post 'BETWEEN' 2 3
builder.add(function testFilterByBetween(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeBetween("post", 2, 3)
  return scanAndCheck(scan, [0, 3, 4], test, "testFilterByBetween")
})

// test filtering with post 'IN' 2 3
builder.add(function testFilterByIn(test) {
  var scan = this.client.newScanBuilder(tableName)
               .filterAttributeIn("post", [2, 3])
  return scanAndCheck(scan, [0, 3, 4], test, "testFilterByIn")
})

builder.add(function testNext(test) {
  var numInFirstScan = 0
  return this.client.newScanBuilder(tableName)
    .filterAttributeGreaterThan("post", 2)
    // The limit is *not* the number of records to return; instead it is
    // the number of records to scan. So the actual number of records returned
    // is not specified when a filter is given.
    .setLimit(4)
    .execute()
    .then(function (data) {
      numInFirstScan = data.Count
      test.ok(data.hasNext())
      return data.next()
    })
    .then(function (data) {
      test.equal(4, numInFirstScan + data.Count, 'Scan should return 4 records in total')
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

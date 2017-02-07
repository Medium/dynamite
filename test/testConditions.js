// Copyright 2016 A Medium Corporation
'use strict'

var utils = require('./utils/testUtils.js')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)

var onError = console.error.bind(console)
var tableName = 'user'
var rawData = [
  {userId: 'a', column: '@'},
  {userId: 'b', column: '@', blacklistedAt: '1'},
  {userId: 'c', column: '@', blacklistedAt: '1', unblacklistedAt: '2'},
  {userId: 'd', column: '@', blacklistedAt: '3', unblacklistedAt: '2'},
  {userId: 'e', column: '@', blacklistedAt: '4', unblacklistedAt: '4'}
]

var db
var client

exports.setUp = function (done) {
  db = utils.getMockDatabase()
  client = utils.getMockDatabaseClient()
  utils.ensureLocalDynamo()
  utils.createTable(db, tableName, 'userId', 'column')
    .thenBound(utils.initTable, null, {"db": db, "tableName": tableName, "data": rawData})
    .fail(onError)
    .fin(done)
}

exports.tearDown = function (done) {
  utils.deleteTable(db, tableName)
    .fin(done)
}

builder.add(function testEqualsAttribute(test) {
  var filter = client.newConditionBuilder()
    .filterAttributeEqualsAttribute('blacklistedAt', 'unblacklistedAt')
  return assertUserIds(test, ['e'], filter)
})

builder.add(function testNotEqualsAttribute(test) {
  var filter = client.newConditionBuilder()
    .filterAttributeNotEqualsAttribute('blacklistedAt', 'unblacklistedAt')
  return assertUserIds(test, ['a', 'b', 'c', 'd'], filter)
})

builder.add(function testLessThanAttribute(test) {
  var filter = client.newConditionBuilder()
    .filterAttributeLessThanAttribute('blacklistedAt', 'unblacklistedAt')
  return assertUserIds(test, ['c'], filter)
})

builder.add(function testLessThanEqualAttribute(test) {
  var filter = client.newConditionBuilder()
    .filterAttributeLessThanEqualAttribute('blacklistedAt', 'unblacklistedAt')
  return assertUserIds(test, ['c', 'e'], filter)
})

builder.add(function testGreaterThanAttribute(test) {
  var filter = client.newConditionBuilder()
    .filterAttributeGreaterThanAttribute('blacklistedAt', 'unblacklistedAt')
  return assertUserIds(test, ['d'], filter)
})

builder.add(function testGreaterThanEqualAttribute(test) {
  var filter = client.newConditionBuilder()
    .filterAttributeGreaterThanEqualAttribute('blacklistedAt', 'unblacklistedAt')
  return assertUserIds(test, ['d', 'e'], filter)
})

function assertUserIds(test, expectedUserIds, filter) {
  return client.newScanBuilder(tableName)
    .withFilter(filter)
    .setLimit(10)
    .execute()
    .then(function (data) {
      var userIds = data.result.map(function (r) {
        return r.userId
      })
      test.deepEqual(expectedUserIds, userIds.sort())
    })
}

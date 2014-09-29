// Copyright 2013 The Obvious Corporation.

var utils = require('./utils/testUtils.js')
var dynamite = require('../dynamite')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)
var Q = require('kew')

function onError(err) {
  console.error(err.stack)
}

var userData = [
  {'userId': 'userA', 'column': '@', 'age': '29'},
  {'userId': 'userB', 'column': '@', 'age': '44'},
  {'userId': 'userC', 'column': '@', 'age': '36'}
]

var phoneData = [
  {'userId': 'userA', 'column': 'phone1', 'number': '415-662-1234'},
  {'userId': 'userA', 'column': 'phone2', 'number': '650-143-8899'},
  {'userId': 'userB', 'column': 'phone1', 'number': '550-555-5555'}
]

// Generate many items that will exceed the batch get count limit
var manyData = []
for (var i = 0; i < 202; i++) {
  manyData.push({'hashKey': 'id' + i, 'column': '@', 'data': 'small'})
}

// Generate big items that will exceed amount allowed to be returned.
var muchoData = []
var junk = new Array(62000).join('.')
for (var i = 0; i < 101; i++) {
  muchoData.push({'hashKey': 'id' + i, 'column': '@', 'data': junk})
}

// basic setup for the tests, creating record userA with range key @
exports.setUp = function (done) {
  this.db = utils.getMockDatabase()
  this.client = utils.getMockDatabaseClient()

  var userTablePromise = utils.createTable(this.db, 'user', 'userId', 'column')
    .thenBound(utils.initTable, null, {db: this.db, tableName: 'user', data: userData})

  var phoneTablePromise = utils.createTable(this.db, 'phones', 'userId', 'column')
    .thenBound(utils.initTable, null, {db: this.db, tableName: 'phones', data: phoneData})

  var manyTablePromise = utils.createTable(this.db, 'pre_many', 'hashKey', 'column')
    .thenBound(utils.initTable, null, {db: this.db, tableName: 'pre_many', data: manyData})

  var muchoTablePromise = utils.createTable(this.db, 'mucho', 'hashKey', 'column')
    .thenBound(utils.initTable, null, {db: this.db, tableName: 'mucho', data: muchoData})

  Q.all([userTablePromise, phoneTablePromise, manyTablePromise, muchoTablePromise])
    .fail(onError)
    .fin(done)
}

exports.tearDown = function (done) {
  Q.all([
    utils.deleteTable(this.db, 'user'),
    utils.deleteTable(this.db, 'phones'),
    utils.deleteTable(this.db, 'pre_many'),
    utils.deleteTable(this.db, 'mucho')
  ])
  .then(function () {
    done()
  })
}

builder.add(function testBatchGet(test) {
  return this.client.newBatchGetBuilder()
    .requestItems('user', [{'userId': 'userA', 'column': '@'}, {'userId': 'userB', 'column': '@'}])
    .requestItems('phones', [{'userId': 'userA', 'column': 'phone1'}, {'userId': 'userB', 'column': 'phone1'}])
    .execute()
    .then(function (data) {
      var ages = data.result.user.map(function (user) { return user.age })
      test.deepEqual(ages, ['29', '44'])
      var phones = data.result.phones.map(function (phone) { return phone.number })
      test.deepEqual(phones, ['415-662-1234', '550-555-5555'])
    })
})

builder.add(function testEmptyBatch(test) {
  return this.client.newBatchGetBuilder()
    .requestItems('user', [{'userId': 'userE', 'column': '@'}])
    .execute()
    .then(function (data) {
      test.ok(Array.isArray(data.result.user), 'An array should be returned for requested tables')
      test.equal(0, data.result.user.length, 'No items should have been returned')
    })
})

builder.add(function testBatchGetMany(test) {
  return this.client.newBatchGetBuilder()
    .setPrefix('pre_')
    .requestItems('many', manyData.map(function (o) { return {'hashKey': o.hashKey, 'column': '@'}}))
    .execute()
    .then(function (data) {
      test.equal(202, data.result.many.length, 'All 202 items should be returned')
      test.equal(0, Object.keys(data.UnprocessedKeys).length, 'There should be no unprocessed keys')
    })
})

builder.add(function testBatchGetMucho(test) {
  return this.client.newBatchGetBuilder()
    .requestItems('mucho', muchoData.map(function (o) { return {'hashKey': o.hashKey, 'column': '@'}}))
    .execute()
    .then(function (data) {
      test.equal(101, data.result.mucho.length, 'All 101 items should be returned')
      test.equal(0, Object.keys(data.UnprocessedKeys), 'There should be no unprocessed keys')
    })
})

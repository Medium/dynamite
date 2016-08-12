// Copyright 2015 A Medium Corporation.

var typeUtil = require('../lib/typeUtil')
var localUpdater = require('../lib/localUpdater')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)
var Q = require('kew')

builder.add(function testDelete(test) {
  var data = typeUtil.packObjectOrArray({
    "userId": "userA",
    "column": "@",
    "age": 29,
    "someStringSet": ['a', 'b', 'c']
  })

  var updated = localUpdater.update(data, {
    'age': {
      Action: 'DELETE'
    }
  })

  test.equal(data.age.N, 29, 'age should not change')
  test.ok(updated.age === undefined, 'age should be undefined')
  return Q.resolve()
})

builder.add(function testDeleteFromSet(test) {
  var data = typeUtil.packObjectOrArray({
    "userId": "userA",
    "column": "@",
    "age": 29,
    "someStringSet": ['a', 'b', 'c']
  })

  var updated = localUpdater.update(data, {
    'someStringSet': {
      Action: 'DELETE',
      Value: typeUtil.valueToObject(['b','c','d'])
    }
  })

  test.deepEqual(data.someStringSet.SS, ['a', 'b', 'c'], 'someStringSet should not change')
  test.deepEqual(updated.someStringSet.SS, ['a'], 'someStringSet should equal [\'a\']')
  return Q.resolve()
})

builder.add(function testAddToSet(test) {
  var data = typeUtil.packObjectOrArray({
    "userId": "userA",
    "column": "@",
    "age": 29,
    "someStringSet": ['a', 'b', 'c']
  })

  var updated = localUpdater.update(data, {
    'someStringSet': {
      Action: 'ADD',
      Value: typeUtil.valueToObject(['b','c','d'])
    }
  })

  test.deepEqual(data.someStringSet.SS, ['a', 'b', 'c'], 'someStringSet should not change')
  test.deepEqual(updated.someStringSet.SS, ['a', 'b', 'c', 'd'], 'someStringSet should equal [\'a\', \'b\', \'c\', \'d\']')
  return Q.resolve()
})

builder.add(function testAddToEmptySet(test) {
  var data = typeUtil.packObjectOrArray({
    "userId": "userA",
    "column": "@",
    "age": 29
  })

  var updated = localUpdater.update(data, {
    'someStringSet': {
      Action: 'ADD',
      Value: typeUtil.valueToObject(['b','c','d'])
    }
  })

  test.ok(data.someStringSet === undefined, 'someStringSet should not change')
  test.deepEqual(updated.someStringSet.SS, ['b', 'c', 'd'], 'someStringSet should equal [\'a\', \'b\', \'c\', \'d\']')
  return Q.resolve()
})

builder.add(function testAddToNumber(test) {
  var data = typeUtil.packObjectOrArray({
    "userId": "userA",
    "column": "@",
    "age": 29
  })

  var updated = localUpdater.update(data, {
    'age': {
      Action: 'ADD',
      Value: typeUtil.valueToObject(1)
    }
  })

  test.deepEqual(data.age.N, 29, 'age should not change')
  test.deepEqual(updated.age.N, '30', 'age should equal 30')
  return Q.resolve()
})

builder.add(function testAddToEmptyNumber(test) {
  var data = typeUtil.packObjectOrArray({
    "userId": "userA",
    "column": "@"
  })

  var updated = localUpdater.update(data, {
    'age': {
      Action: 'ADD',
      Value: typeUtil.valueToObject(30)
    }
  })

  test.ok(data.age === undefined, 'age should not change')
  test.deepEqual(updated.age.N, '30', 'age should equal 30')
  return Q.resolve()
})

builder.add(function testPut(test) {
  var data = typeUtil.packObjectOrArray({
    "userId": "userA",
    "column": "@",
    "age": 29,
    "someStringSet": ['a', 'b', 'c']
  })

  var updated = localUpdater.update(data, {
    'someStringSet': {
      Action: 'PUT',
      Value: typeUtil.valueToObject(['b','c','d'])
    }
  })

  test.deepEqual(data.someStringSet.SS, ['a', 'b', 'c'], 'someStringSet should not change')
  test.deepEqual(updated.someStringSet.SS, ['b', 'c', 'd'], 'someStringSet should equal [\'b\', \'c\', \'d\']')
  return Q.resolve()
})

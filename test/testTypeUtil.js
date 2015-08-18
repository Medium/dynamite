// Copyright 2015 A Medium Corporation.

var typeUtil = require('../lib/typeUtil')
var nodeunitq = require('nodeunitq')
var builder = new nodeunitq.Builder(exports)
var Q = require('kew')

builder.add(function testAddToSet(test) {
  var set = typeUtil.valueToObject([1,2,3])
  test.equal(typeUtil.objectToType(set), 'NS')

  var additions = typeUtil.valueToObject([4])
  test.equal(typeUtil.objectToType(additions), 'NS')

  var modified = typeUtil.addToSet(set, additions)
  test.equal(typeUtil.objectToType(modified), 'NS')
  modified.NS.sort()

  test.deepEqual(set.NS, [1,2,3].map(String))
  test.deepEqual(modified.NS, [1,2,3,4].map(String))

  return Q.resolve()
})

builder.add(function testAddToNullSet(test) {
  var set = null

  var additions = typeUtil.valueToObject([4])
  test.equal(typeUtil.objectToType(additions), 'NS')

  var modified = typeUtil.addToSet(set, additions)
  test.equal(typeUtil.objectToType(modified), 'NS')
  modified.NS.sort()

  test.deepEqual(modified.NS, [4].map(String))

  return Q.resolve()
})

builder.add(function testDeleteFromSet(test) {
  var set = typeUtil.valueToObject([1,2,3])
  test.equal(typeUtil.objectToType(set), 'NS')

  var deletions = typeUtil.valueToObject([1, 4])
  test.equal(typeUtil.objectToType(deletions), 'NS')

  var modified = typeUtil.deleteFromSet(set, deletions)
  test.equal(typeUtil.objectToType(modified), 'NS')
  modified.NS.sort()

  test.deepEqual(modified.NS, [2, 3].map(String))

  return Q.resolve()
})

builder.add(function testObjectIsNonEmptySet(test) {
  test.ok(!typeUtil.objectIsNonEmptySet())
  test.ok(!typeUtil.objectIsNonEmptySet(null))
  test.ok(!typeUtil.objectIsNonEmptySet({}))
  test.ok(!typeUtil.objectIsNonEmptySet(typeUtil.valueToObject(4)))
  test.ok(!typeUtil.objectIsNonEmptySet(typeUtil.valueToObject('4')))

  test.ok(typeUtil.objectIsNonEmptySet(typeUtil.valueToObject([4])))
  test.ok(typeUtil.objectIsNonEmptySet(typeUtil.valueToObject(['4'])))

  return Q.resolve()
})

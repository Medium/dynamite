// Copyright 2013 The Obvious Corporation

/**
 * @fileoverview Utility functions that convert plain javascript objects to
 *  Dynamo AttributeValue map (http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html)
 *  objects back and forth.
 */

var typ = require('typ')

/**
 * Convert Dynamo AttributeValue map object(s) to plain javascript object(s)
 *
 * @param {Object|Array.<Object>} object Dynamo AttributeValue map object(s)
 * @return {Object|Array.<Object>} plain javascript object(s)
 */
function unpackObjectOrArray(object) {
  if (typ.isNullish(object)) return object
  if (Array.isArray(object)) return object.map(unpackObjectOrArray)

  var item = {}
  for (var key in object) {
    item[key] = objectToValue(object[key])
  }
  return item
}

/**
 * Convert an object to a Dynamo AttributeValue map.
 *
 * @param {Object|Array.<Object>|undefined} object
 * @param {Object=} attributes an optional map of the attributes that need to convert.
 * @return {Object|Array.<Object>|undefined} The object in Dynamo AttributeValue map.
 */
function packObjectOrArray(object, attributes) {
  if (typ.isNullish(object)) return object
  if (Array.isArray(object)) {
    return object.map(function (obj) { return packObjectOrArray(obj, attributes) })
  }

  var newObj = {}
  for (var key in object) {
    if (attributes && !attributes[key]) continue
    newObj[key] = valueToObject(object[key])
  }
  return newObj
}

/**
 * @param {number|string|Array} value
 */
function valueToObject(value) {
  var type = typeof value

  if (Array.isArray(value)) {
    var firstItemType = typeof value[0]

    // check that all of the items are of the same type; that of the first element's
    for (var i = 0; i < value.length; i++) {
      if (typeof value[i] !== firstItemType) {
        throw new Error('Inconsistent types in set! Expecting all types to be the same as the first element\'s: ' + firstItemType)
      }
    }

    if (firstItemType === 'string') {
      return {SS: value}
    } else {
      if (firstItemType !== 'number') {
        throw new Error('Invalid dynamo set value. Type: ' + firstItemType + ', Value: ' + value[0])
      }

      var numArray = []
      for (var i = 0; i < value.length; i++) {
        numArray.push(String(value[i]))
      }

      return {NS: numArray}
    }
  } else if (type === 'string') {
    return {S: value}
  } else {
    if (type !== 'number') {
      // TODO(nick): I'm pretty sure this should be an error. But there is a bunch
      // of code relying on this behavior, so just log the error for now and
      // we'll track down the problems in the logs.
      console.error('Invalid dynamo value. Type: ' + type + ', Value: ' + value, new Error().stack)
    }

    return {N: String(value)}
  }
}

/**
 * Convert a Dynamo AttributeValue value to a javascript primitive value
 *
 * @param {Object} obj Dynamo AttributeValue value.
 * @return {string|number|Array} a javascript primitive value
 */
function objectToValue(obj) {
  if (obj.hasOwnProperty('SS')) {
    return obj.SS
  } else if (obj.hasOwnProperty('NS')) {
    var numArray = []
    for (var i = 0; i < obj.NS.length; i++) {
      numArray.push(Number(obj.NS[i]))
    }
    return numArray
  }

  return obj.hasOwnProperty('S') ? obj.S : Number(obj.N)
}

module.exports = {
  unpackObjectOrArray: unpackObjectOrArray,
  packObjectOrArray: packObjectOrArray,
  valueToObject: valueToObject,
  objectToValue: objectToValue
}

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
 * @param {Object|Array.<Object>} obj Dynamo AttributeValue map object(s)
 * @return {Object|Array.<Object>} plain javascript object(s)
 */
function unpackObjects(object) {
  if (typ.isNullish(object)) return object
  if (Array.isArray(object)) return object.map(unpackObjects)

  var item = {}
  for (var key in object) {
    item[key] = _objectToValue(object[key])
  }
  return item
}

/**
 * Convert an object to a Dynamo AttributeValue map.
 *
 * @param {Object|Array.<Object>} The data object
 * @param {Object=} attributes an optional map of the attributes that need to convert.
 * @return {Object|Array.<Object>} The object in Dynamo AttributeValue map.
 */
function packObjects(object, attributes) {
  if (typ.isNullish(object)) return object
  if (Array.isArray(object)) {
    var ret = []
    object.forEach(function (obj) { ret.push(packObjects(obj, attributes)) })
    return ret
  }

  var newObj = {}
  for (var key in object) {
    if (attributes && !attributes[key]) continue

    if (Array.isArray(object[key])) {
      // array field
      var firstType = typeof object[key][0]
      var consistentType = firstType
      object[key].map(function (attr) {
        if ((typeof attr) !== firstType) throw new Error('Array types must all be the same')
      })

      if (firstType === 'string') {
        newObj[key] = {
          SS: object[key]
        }
      } else if (firstType === 'number') {
        newObj[key] = {
          NS: object[key]
        }
      } else {
        throw new Error('Only arrays of strings and numbers are allowed. Unknown type: ' + firstType)
      }
    } else if (typeof object[key] === 'string') {
      // string field
      newObj[key] = {
        S: object[key]
      }
    } else if (typeof object[key] === 'number') {
      // number field
      newObj[key] = {
        N: object[key]
      }
    } else {
      throw new Error('Unknown object type at ' + key)
    }
  }
  return newObj
}

/**
 * Convert a Dynamo AttributeValue value to a javascript primitive value
 *
 * @param {Object} obj Dynamo AttributeValue value.
 * @return {string|number} a javascript primitive value
 */
function _objectToValue(obj) {
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
  unpackObjects: unpackObjects,
  packObjects: packObjects
}

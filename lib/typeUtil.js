// Copyright 2013 The Obvious Corporation

/**
 * @fileoverview Utility functions that convert plain javascript objects to
 *  Dynamo AttributeValue map (http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html)
 *  objects back and forth.
 */

var typ = require('typ')

/**
 * From http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
 * - B: A Binary data type.
 * - BOOL: A Boolean data type.
 * - BS: A Binary Set data type.
 * - L: A List of attribute values.
 * - M: A Map of attribute values.
 * - N: A Number data type.
 * - NS: A Number Set data type.
 * - NULL: A Null data type.
 * - S: A String data type.
 * - SS: A String Set data type.
 *
 * @typedef {{
 *   B: (string|undefined),
 *   BOOL: (boolean|undefined),
 *   BS: (Array.<string>|undefined),
 *   M: (Object|undefined),
 *   L: (Array|undefined),
 *   N: (string|undefined),
 *   NS: (Array.<string>|undefined),
 *   NULL: (null|undefined),
 *   S: (string|undefined),
 *   SS: (Array.<string>|undefined)
 * }}
 */
var AWSAttributeValue;

/**
 * Convert Dynamo AttributeValue map object(s) to plain javascript object(s)
 *
 * @param {Object.<string,AWSAttributeValue>|Array.<Object.<string,AWSAttributeValue>>} object Dynamo AttributeValue map object(s)
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
 * @return {Object.<string,AWSAttributeValue>|Array.<Object.<string,AWSAttributeValue>>|null} The object in Dynamo AttributeValue map.
 */
function packObjectOrArray(object, attributes) {
  if (typ.isNullish(object)) return null
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
 * Convert a javascript primitive value to an AWS AttributeValue
 *
 * @param {boolean|number|string|Array} value
 * @return {AWSAttributeValue|null}
 */
function valueToObject(value) {
  var type = typeof value

  switch (typeof value) {
  case 'string':
    return {S: value}
  case 'boolean':
    return {BOOL: Boolean(value)}
  case 'number':
    return {N: String(value)}
  default:
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
      } else if (firstItemType === 'number') {
        var numArray = []
        for (var i = 0; i < value.length; i++) {
          numArray.push(String(value[i]))
        }

        return {NS: numArray}
      } else {
        throw new Error('Invalid dynamo set value. Type: ' + firstItemType + ', Value: ' + value[0])
      }
    } else {
      // TODO(nick): I'm pretty sure this should be an error. But there is a bunch
      // of code relying on this behavior, so just log the error for now and
      // we'll track down the problems in the logs.
      console.error('Invalid dynamo value. Type: ' + type + ', Value: ' + value, new Error().stack)
      return null
    }
  }
}

/**
 * Get the type of an AWS AttributeValue
 * @param {!AWSAttributeValue} obj Dynamo AttributeValue.
 * @return {string}
 */
function objectToType(obj) {
  var objectType = Object.keys(obj)
  if (objectType.length != 1) {
    throw new Error('Expected only one key from Amazon object')
  }

  return objectType[0]
}

/**
 * Convert a Dynamo AttributeValue to a javascript primitive value
 *
 * @param {!AWSAttributeValue} obj
 * @return {string|number|Array.<string>|Array.<number>|boolean|Object} a javascript primitive value
 */
function objectToValue(obj) {
  switch (objectToType(obj)) {
    case 'SS':
      return (/** @type {Array.<string>} */(obj.SS))
    case 'S':
      return (/** @type {string} */(obj.S))
    case 'BOOL':
      return Boolean(obj.BOOL)
    case 'NS':
      return obj.NS.map(function (num) { return Number(num) })
    case 'N':
      return Number(obj.N)
    case 'M':
      var mapped = {};
      for (var k in obj.M) {
        mapped[k] = objectToValue(obj.M[k]);
      }
      return mapped;
    case 'L':
      return obj.L.map(objectToValue);
    default:
      throw new Error('Unexpected key: ' + objectToType(obj) + ' for attribute: ' + obj)
  }
}

/**
 * @param {!AWSAttributeValue} obj
 * @return {boolean}
 */
function objectIsEmpty(obj) {
  return !obj || Object.keys(obj).length === 0
}


/**
 * @param {!AWSAttributeValue} obj
 * @return {boolean}
 */
function objectIsNonEmptySet(obj) {
  if (objectIsEmpty(obj)) return false

  var type = objectToType(obj)
  if (type != 'NS' && type != 'SS') return false

  return Array.isArray(obj[type]) && obj[type].length > 0
}

/**
 * @param {!AWSAttributeValue} set
 * @param {!AWSAttributeValue} additions
 * @return {AWSAttributeValue}
 */
function addToSet(set, additions) {
  var type = objectToType(additions)
  if (objectIsEmpty(set)) {
    set = {}
    set[type] = []
  } else if (objectToType(set) === type) {
    set = clone(set)
  } else {
    throw new Error('Type mismatch: type of set should match type of additions')
  }

  for (var i = 0; i < additions[type].length; i++) {
    if (set[type].indexOf(additions[type][i]) == -1) {
      set[type].push(additions[type][i])
    }
  }

  return set
}

/**
 * @param {!AWSAttributeValue} set
 * @param {!AWSAttributeValue} deletions
 * @return {?AWSAttributeValue}
 */
function deleteFromSet(set, deletions) {
  var type = objectToType(deletions)
  if (objectIsEmpty(set)) {
    return null
  } else if (objectToType(set) !== type) {
    throw new Error('Type mismatch: type of set should match type of deletions')
  }

  set = clone(set)
  for (var i = 0; i < deletions[type].length; i++) {
    var idx = set[type].indexOf(deletions[type][i])
    if (idx != -1) {
      set[type].splice(idx, 1)
    }
  }

  if (set[type].length) {
    return set
  } else {
    return null
  }
}

/**
 * @param {!AWSAttributeValue} number
 * @param {!AWSAttributeValue} addition
 * @return {AWSAttributeValue}
 */
function addToNumber(number, addition) {
  if (objectIsEmpty(number)) {
    number = {'N': '0'}
  } else {
    number = clone(number)
  }

  if (objectToType(number) !== 'N' || objectToType(addition) !== 'N') {
    throw new Error('Type mismatch: number and addition should both be numeric types')
  }

  number.N = String(Number(number.N) + Number(addition.N))

  return number
}

/**
 * @param {!AWSAttributeValue} oldItem
 * @return {AWSAttributeValue}
 */
function clone(oldItem) {
  try {
    var objectType = objectToType(oldItem)

    var newItem = {}
    if (Array.isArray(oldItem[objectType])) {
      newItem[objectType] = oldItem[objectType].slice()
    } else {
      newItem[objectType] = oldItem[objectType]
    }
    return newItem
  } catch (e) {
    return {NULL:null}
  }
}

module.exports = {
  AWSAttributeValue: AWSAttributeValue,

  unpackObjectOrArray: unpackObjectOrArray,
  packObjectOrArray: packObjectOrArray,
  valueToObject: valueToObject,
  objectToType: objectToType,
  objectToValue: objectToValue,
  objectIsEmpty: objectIsEmpty,
  objectIsNonEmptySet: objectIsNonEmptySet,

  addToSet: addToSet,
  deleteFromSet: deleteFromSet,
  addToNumber: addToNumber,
  clone: clone
}

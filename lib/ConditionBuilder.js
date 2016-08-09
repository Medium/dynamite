var typ = require('typ')
var assert = require('assert')
var typeUtil = require('./typeUtil')

/**
 * @param {Object=} opt_conditions
 * @constructor
 */
function ConditionBuilder(opt_conditions) {
  this._conditions = opt_conditions || {}
}

/**
 * @param {!Object} field
 * @param {Array.<ConditionBuilder>} conditions
 */
ConditionBuilder.populateFieldFromConditionBuilderList = function (field, conditions) {
  if (conditions) {
    assert.ok(Array.isArray(conditions), 'Expected array')
    for (var i = 0; i < conditions.length; i++) {
      assert.ok(conditions[i] instanceof ConditionBuilder, 'Expected ConditionBuilder')
      for (var key in conditions[i]._conditions) {
        field[key] = conditions[i]._conditions[key]
      }
    }
  }
}

/**
 * Get the appropriate comparator function to compare the passed in values against
 * @return {function(Object): boolean}
 */
ConditionBuilder.prototype.buildFilterFn = function () {
  var filters = Object.keys(this._conditions).map(function (key) {
    var operator = this._conditions[key].ComparisonOperator

    var values = []
    if (this._conditions[key].AttributeValueList) {
      values = this._conditions[key].AttributeValueList.map(typeUtil.objectToValue)
    }

    switch (operator) {
    case 'BEGINS_WITH':
      return function(item) { return item[key].indexOf(values[0]) === 0 }
    case 'EQ':
      return function(item) { return item[key] == values[0] }
    case 'NE':
      return function(item) { return item[key] != values[0] }
    case 'LE':
      return function(item) { return item[key] <= values[0] }
    case 'LT':
      return function(item) { return item[key] < values[0] }
    case 'GE':
      return function(item) { return item[key] >= values[0] }
    case 'GT':
      return function(item) { return item[key] > values[0] }
    case 'BETWEEN':
      return function(item) { return item[key] >= values[0] && item[key] <= values[1] }
    case 'IN':
      return function(item) { return values.indexOf(item[key]) != -1 }
    case 'NOT_CONTAINS':
      return function(item) { return item[key].indexOf(values[0]) == -1 }
    case 'CONTAINS':
      return function(item) { return item[key].indexOf(values[0]) != -1 }
    case 'NULL':
      return function(item) { return !item.hasOwnProperty(key) }
    case 'NOT_NULL':
      return function(item) { return item.hasOwnProperty(key) }
    default:
      throw new Error('Invalid comparison operator \'' + operator + '\'')
    }
  }.bind(this))

  return function (item) {
    return filters.every(function (filter) { return filter(item) })
  }
}

ConditionBuilder.prototype.expectAttributeEqual =
ConditionBuilder.prototype.expectAttributeEquals =
ConditionBuilder.prototype.filterAttributeEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'EQ',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeNotEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'NE',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeLessThanEqual = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'LE',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeLessThan = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'LT',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeGreaterThanEqual = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'GE',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeGreaterThan = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'GT',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeNotNull = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'NOT_NULL'
  }
  return this
}

ConditionBuilder.prototype.expectAttributeAbsent =
ConditionBuilder.prototype.filterAttributeNull = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'NULL'
  }
  return this
}

ConditionBuilder.prototype.filterAttributeContains = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'CONTAINS',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeNotContains = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'NOT_CONTAINS',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeBeginsWith = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'BEGINS_WITH',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeBetween = function (key, val1, val2) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val1)) throw new Error("Val 1 must be defined")
  if (typ.isNullish(val2)) throw new Error("Val 2 must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'BETWEEN',
    AttributeValueList: [typeUtil.valueToObject(val1), typeUtil.valueToObject(val2)]
  }
  return this
}

ConditionBuilder.prototype.filterAttributeIn = function (key, vals) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(vals)) throw new Error("Vals must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'IN',
    AttributeValueList: vals.map(typeUtil.valueToObject)
  }
  return this
}

module.exports = ConditionBuilder

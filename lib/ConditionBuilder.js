var typ = require('typ')
var assert = require('assert')
var typeUtil = require('./typeUtil')

/** @constructor */
function ConditionBuilder() {
  this._conditions = {}
}

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

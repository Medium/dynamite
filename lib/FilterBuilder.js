var typ = require('typ')
var typeUtil = require('./typeUtil')

/** @constructor */
function FilterBuilder() {
  this._conditions = {}
}

FilterBuilder.prototype.populateObject = function (obj) {
  for (var key in this._conditions) {
    obj[key] = this._conditions[key]
  }
}

FilterBuilder.prototype.filterAttributeEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'EQ',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeNotEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'NE',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeLessThanEqual = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'LE',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeLessThan = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'LT',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeGreaterThanEqual = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'GE',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeGreaterThan = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'GT',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeNotNull = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'NOT_NULL'
  }
  return this
}

FilterBuilder.prototype.filterAttributeNull = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'NULL'
  }
  return this
}

FilterBuilder.prototype.filterAttributeContains = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'CONTAINS',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeNotContains = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'NOT_CONTAINS',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeBeginsWith = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'BEGINS_WITH',
    AttributeValueList: [typeUtil.valueToObject(val)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeBetween = function (key, val1, val2) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val1)) throw new Error("Val 1 must be defined")
  if (typ.isNullish(val2)) throw new Error("Val 2 must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'BETWEEN',
    AttributeValueList: [typeUtil.valueToObject(val1), typeUtil.valueToObject(val2)]
  }
  return this
}

FilterBuilder.prototype.filterAttributeIn = function (key, vals) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(vals)) throw new Error("Vals must be defined")
  this._conditions[key] = {
    ComparisonOperator: 'IN',
    AttributeValueList: vals.map(typeUtil.valueToObject)
  }
  return this
}

module.exports = FilterBuilder

var typ = require('typ')

/** @constructor */
function ConditionBuilder() {
  this._attributes = {}
}

ConditionBuilder.prototype.expectAttributeAbsent = function (key) {
  if (typ.isNullish(key)) throw new Error("Invalid key")
  this._attributes[key] = {absent: true}
  return this
}

ConditionBuilder.prototype.expectAttributeEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Invalid key")
  if (typ.isNullish(val)) throw new Error("Invalid value")
  this._attributes[key] = {val: val}
  return this
}
ConditionBuilder.prototype.expectAttributeEqual = ConditionBuilder.prototype.expectAttributeEquals

module.exports = ConditionBuilder

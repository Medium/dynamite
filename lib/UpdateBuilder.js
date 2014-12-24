var common = require('./common')
var typ = require('typ')
var util = require('util')
var DynamoRequest = require('./DynamoRequest')
var Builder = require('./Builder')

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function UpdateBuilder(options) {
  Builder.call(this, options)

  /** @private {!Object.<?>} */
  this._attributes = {}

  /** @private {boolean} */
  this._enabledUpsert = false
}
util.inherits(UpdateBuilder, Builder)

UpdateBuilder.prototype.enableUpsert = function () {
  this._enabledUpsert = true
  return this
}

UpdateBuilder.prototype.putAttribute = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._attributes[key] = {val: val}
  return this
}

UpdateBuilder.prototype.addToAttribute = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._attributes[key] = {increment: val}
  return this
}

UpdateBuilder.prototype.deleteFromAttribute = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._attributes[key] = {del: val}
  return this
}

UpdateBuilder.prototype.deleteAttribute = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._attributes[key] = {del: true}
  return this
}

UpdateBuilder.prototype.execute = function () {
  var self = this
  var req = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setHashKey(this._hashKey, true)
    .setUpdates(this._attributes)
    .setExpected(this._conditions)
    .setReturnValues("ALL_NEW")

  if (this._rangeKey) req.setRangeKey(this._rangeKey, true)

  var queryData = req.build()

  if ((!this._conditions || !this._conditions.length) && !this._enabledUpsert) {
    console.warn("Update issued without conditions or .enableUpsert() called")
    console.trace()
  }
  return this.request("updateItem", queryData)
    .then(this.prepareOutput.bind(this))
    .failBound(this.convertErrors, null, {data: queryData, isWrite: true})
}

module.exports = UpdateBuilder

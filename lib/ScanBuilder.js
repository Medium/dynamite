var common = require('./common')
var typ = require('typ')

var Q = require('kew')

var DynamoRequest = require('./DynamoRequest')
var DynamoResponse = require('./DynamoResponse')
var Builder = require('./Builder')

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function ScanBuilder(options) {
  Builder.call(this, options)
  this._filters = {}
}
require('util').inherits(ScanBuilder, Builder)

ScanBuilder.prototype.filterAttributeEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['EQ', val]
  return this
}

ScanBuilder.prototype.filterAttributeNotEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['NE', val]
  return this
}

ScanBuilder.prototype.filterAttributeLessThanEqual = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['LE', val]
  return this
}

ScanBuilder.prototype.filterAttributeLessThan = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['LT', val]
  return this
}

ScanBuilder.prototype.filterAttributeGreaterThanEqual = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['GE', val]
  return this
}

ScanBuilder.prototype.filterAttributeGreaterThan = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['GT', val]
  return this
}

ScanBuilder.prototype.filterAttributeNotNull = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._filters[key] = ['NOT_NULL']
  return this
}

ScanBuilder.prototype.filterAttributeNull = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._filters[key] = ['NULL']
  return this
}

ScanBuilder.prototype.filterAttributeContains = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['CONTAINS', val]
  return this
}

ScanBuilder.prototype.filterAttributeNotContains = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['NOT_CONTAINS', val]
  return this
}

ScanBuilder.prototype.filterAttributeBeginsWith = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._filters[key] = ['BEGINS_WITH', val]
  return this
}

ScanBuilder.prototype.filterAttributeBetween = function (key, val1, val2) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val1)) throw new Error("Val 1 must be defined")
  if (typ.isNullish(val2)) throw new Error("Val 2 must be defined")
  this._filters[key] = ['BETWEEN', val1, val2]
  return this
}

ScanBuilder.prototype.filterAttributeIn = function (key, vals) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(vals)) throw new Error("Vals must be defined")
  this._filters[key] = ['IN'].concat(vals)
  return this
}

ScanBuilder.prototype.setStartKey = function (key) {
  this._startKey = key
  return this
}

/** @override */
ScanBuilder.prototype.prepareOutput = function (output) {
  return new DynamoResponse(
      this.getPrefix(), output, this._repeatWithStartKey.bind(this))
}

/**
 * @param {Object} nextKey
 * @return {Q.Promise.<DynamoResponse>}
 * @private
 */
ScanBuilder.prototype._repeatWithStartKey = function (nextKey) {
  return this.setStartKey(nextKey).execute()
}

ScanBuilder.prototype.execute = function () {
  var queryData = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setFilter(this._filters)
    .setLimit(this._limit)
    .setStartKey(this._startKey)
    .selectAttributes(this._attributes)
    .build()

  return this.request("scan", queryData)
    .then(this.prepareOutput.bind(this))
    .fail(this.emptyResults)
    .setContext({data: queryData, isWrite: false})
    .fail(this.convertErrors)
    .clearContext()
}

module.exports = ScanBuilder

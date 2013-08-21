var common = require('./common')
var typ = require('typ')
var DynamoRequest = require('./DynamoRequest')
var Builder = require('./Builder')

function QueryBuilder(options) {
  Builder.call(this, options)
  this._keyConditions = []
}
require('util').inherits(QueryBuilder, Builder)

QueryBuilder.prototype.setHashKey = function (name, val) {
  return this.indexEquals(name, val)
}

QueryBuilder.prototype.indexBeginsWith = function (name, prefix) {
  if (typ.isNullish(prefix)) throw new Error("Key must be defined")
  this._keyConditions.push({
    name: name,
    operator: 'BEGINS_WITH',
    attributes: [ prefix ]
  })
  return this
}

QueryBuilder.prototype.indexEquals = function (name, val) {
  if (typ.isNullish(val)) throw new Error("Invalid range key")
  this._keyConditions.push({
    name: name,
    operator: 'EQ',
    attributes: [ val ]
  })
  return this
}
QueryBuilder.prototype.indexEqual = QueryBuilder.prototype.indexEquals

QueryBuilder.prototype.indexLessThanEqual = function (name, val) {
  if (typ.isNullish(val)) throw new Error("Invalid range key")
  this._keyConditions.push({
    name: name,
    operator: 'LE',
    attributes: [ val ]
  })

  return this
}

QueryBuilder.prototype.indexLessThan = function (name, val) {
  if (typ.isNullish(val)) throw new Error("Invalid range key")
  this._keyConditions.push({
    name: name,
    operator: 'LT',
    attributes: [ val ]
  })
  return this
}

QueryBuilder.prototype.indexGreaterThanEqual = function (name, val) {
  if (typ.isNullish(val)) throw new Error("Invalid range key")
  this._keyConditions.push({
    name: name,
    operator: 'GE',
    attributes: [ val ]
  })
  return this
}
QueryBuilder.prototype.indexGreaterThanEquals = QueryBuilder.prototype.indexGreaterThanEqual

QueryBuilder.prototype.indexGreaterThan = function (name, val) {
  if (typ.isNullish(val)) throw new Error("Invalid range key")
  this._keyConditions.push({
    name: name,
    operator: 'GT',
    attributes: [ val ]
  })
  return this
}

QueryBuilder.prototype.indexBetween = function (name, val1, val2) {
  if (typ.isNullish(val1)) throw new Error("Invalid range key 1")
  if (typ.isNullish(val2)) throw new Error("Invalid range key 2")
  this._keyConditions.push({
    name: name,
    operator: 'BETWEEN',
    attributes: [ val1, val2 ]
  })
  return this
}

QueryBuilder.prototype.startKey = function (key) {
  this._startKey = key
  return this
}

QueryBuilder.prototype.execute = function () {
  var query = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setConsistent(this._isConsistent)
    .setKeyConditions(this._keyConditions)
    .setStartKey(this._startKey)
    .selectAttributes(this._attributes, true)
    .scanForward(this._shouldScanForward)
    .setLimit(this._limit)

  if (this._isCount) query.getCount()

  if (this._rangeKeyCondition) {
    query.setRangeKey.apply(query, this._rangeKeyCondition)
  }

  var queryData = query.build()

  return this.request("query", queryData)
    .then(this.prepareOutput)
    .fail(this.emptyResults)
    .setContext({attributes: queryData, isWrite: false})
    .fail(this.convertErrors)
    .clearContext()
}

module.exports = QueryBuilder

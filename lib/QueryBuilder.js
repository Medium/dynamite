var common = require('./common')
var typ = require('typ')
var DynamoRequest = require('./DynamoRequest')
var DynamoResponse = require('./DynamoResponse')
var Builder = require('./Builder')
var Q = require('kew')
var IndexNotExistError = require('./errors').IndexNotExistError

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function QueryBuilder(options) {
  Builder.call(this, options)

  /** @private {!Array.<{name: string, operator: string, attributes: !Array}>} */
  this._keyConditions = []
}
require('util').inherits(QueryBuilder, Builder)

/**
 * If this query runs on a local index or global index, set a
 * function that can generate an index name based on query
 * conditions.
 *
 * @param {Function(string, string): string} fn The generator function
 */
QueryBuilder.prototype.setIndexNameGenerator = function (fn) {
  this._indexNameGenerator = fn
}

QueryBuilder.prototype.setHashKey = function (name, val) {
  if (typ.isNullish(val)) throw new Error('Invalid hash key value: ' + val)
  this._hashKeyName = name
  // The condition on the hash key is always an 'EQ'
  this._keyConditions.push({
    name: name,
    operator: 'EQ',
    attributes: [ val ]
  })
  return this
}

QueryBuilder.prototype.indexBeginsWith = function (name, prefix) {
  this._rangeKeyName = name
  if (typ.isNullish(prefix)) throw new Error('Key must be defined')
  this._keyConditions.push({
    name: name,
    operator: 'BEGINS_WITH',
    attributes: [ prefix ]
  })
  return this
}

QueryBuilder.prototype.indexEquals = function (name, val) {
  if (typ.isNullish(val)) throw new Error('Invalid value for the range key: ' + val)
  this._rangeKeyName = name
  this._keyConditions.push({
    name: name,
    operator: 'EQ',
    attributes: [ val ]
  })
  return this
}
QueryBuilder.prototype.indexEqual = QueryBuilder.prototype.indexEquals

QueryBuilder.prototype.indexLessThanEqual = function (name, val) {
  if (typ.isNullish(val)) throw new Error('Invalid value for the range key: ' + val)
  this._rangeKeyName = name
  this._keyConditions.push({
    name: name,
    operator: 'LE',
    attributes: [ val ]
  })

  return this
}

QueryBuilder.prototype.indexLessThan = function (name, val) {
  if (typ.isNullish(val)) throw new Error('Invalid value for the range key: ' + val)
  this._rangeKeyName = name
  this._keyConditions.push({
    name: name,
    operator: 'LT',
    attributes: [ val ]
  })
  return this
}

QueryBuilder.prototype.indexGreaterThanEqual = function (name, val) {
  if (typ.isNullish(val)) throw new Error('Invalid value for the range key: ' + val)
  this._rangeKeyName = name
  this._keyConditions.push({
    name: name,
    operator: 'GE',
    attributes: [ val ]
  })
  return this
}
QueryBuilder.prototype.indexGreaterThanEquals = QueryBuilder.prototype.indexGreaterThanEqual

QueryBuilder.prototype.indexGreaterThan = function (name, val) {
  if (typ.isNullish(val)) throw new Error('Invalid value for the range key: ' + val)
  this._rangeKeyName = name
  this._keyConditions.push({
    name: name,
    operator: 'GT',
    attributes: [ val ]
  })
  return this
}

QueryBuilder.prototype.indexBetween = function (name, val1, val2) {
  if (typ.isNullish(val1)) throw new Error('Invalid lower bound for the range key: ' + val)
  if (typ.isNullish(val2)) throw new Error('Invalid upper bound for the range key: ' + val)
  this._rangeKeyName = name
  this._keyConditions.push({
    name: name,
    operator: 'BETWEEN',
    attributes: [ val1, val2 ]
  })
  return this
}

QueryBuilder.prototype.setStartKey = function (key) {
  this._startKey = key
  return this
}

/**
 * Set the index name of this query.
 *
 * @param {string} indexName
 */
QueryBuilder.prototype.setIndexName = function (indexName) {
  this._indexName = indexName
  return this
}

/** @override */
QueryBuilder.prototype.prepareOutput = function (output) {
  return new DynamoResponse(
      this.getPrefix(), output, this._repeatWithStartKey.bind(this))
}

/**
 * @param {Object} nextKey
 * @return {Q.Promise.<DynamoResponse>}
 * @private
 */
QueryBuilder.prototype._repeatWithStartKey = function (nextKey) {
  return this.setStartKey(nextKey).execute()
}

QueryBuilder.prototype.execute = function () {
  var query = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setConsistent(this._isConsistent)
    .setIndexName(this._indexName)
    .setKeyConditions(this._keyConditions)
    .setStartKey(this._startKey)
    .selectAttributes(this._attributes, true)
    .scanForward(this._shouldScanForward)
    .setLimit(this._limit)

  if (this._isCount) query.getCount()

  if (this._rangeKeyCondition) {
    query.setRangeKey.apply(query, this._rangeKeyCondition)
  }

  if (this._indexNameGenerator) {
    var indexName = this._indexNameGenerator(this._hashKeyName, this._rangeKeyName)
    if (!indexName) {
      throw new IndexNotExistError(this._hashKeyName, this._rangeKeyName)
    }
    this.setIndexName(indexName)
  }

  var queryData = query.build()

  return this.request('query', queryData)
    .then(this.prepareOutput.bind(this))
    .fail(this.emptyResults)
    .setContext({attributes: queryData, isWrite: false})
    .fail(this.convertErrors)
    .clearContext()
}

module.exports = QueryBuilder

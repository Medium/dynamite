var common = require('./common')
var typ = require('typ')
var ConditionBuilder = require('./ConditionBuilder')
var DynamoRequest = require('./DynamoRequest')
var DynamoResponse = require('./DynamoResponse')
var Builder = require('./Builder')
var Q = require('kew')
var util = require('util')
var IndexNotExistError = require('./errors').IndexNotExistError

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function QueryBuilder(options) {
  Builder.call(this, options)

  /** @private {!ConditionBuilder} */
  this._keyConditions = new ConditionBuilder()
}
util.inherits(QueryBuilder, Builder)

/**
 * If this query runs on a local index or global index, set a
 * function that can generate an index name based on query
 * conditions.
 *
 * @param {function(string, string): string} fn The generator function
 */
QueryBuilder.prototype.setIndexNameGenerator = function (fn) {
  this._indexNameGenerator = fn
  return this
}

QueryBuilder.prototype.setHashKey = function (name, val) {
  this._hashKeyName = name
  this._keyConditions.filterAttributeEquals(name, val)
  return this
}

QueryBuilder.prototype.setIndexRangeKeyWithoutCondition = function (name) {
  this._rangeKeyName = name
  return this
}

QueryBuilder.prototype.indexBeginsWith = function (name, prefix) {
  this._rangeKeyName = name
  this._keyConditions.filterAttributeBeginsWith(name, prefix)
  return this
}

QueryBuilder.prototype.indexEqual =
QueryBuilder.prototype.indexEquals = function (name, val) {
  this._rangeKeyName = name
  this._keyConditions.filterAttributeEquals(name, val)
  return this
}

QueryBuilder.prototype.indexLessThanEqual =
QueryBuilder.prototype.indexLessThanEquals = function (name, val) {
  this._rangeKeyName = name
  this._keyConditions.filterAttributeLessThanEqual(name, val)
  return this
}

QueryBuilder.prototype.indexLessThan = function (name, val) {
  this._rangeKeyName = name
  this._keyConditions.filterAttributeLessThan(name, val)
  return this
}

QueryBuilder.prototype.indexGreaterThanEqual =
QueryBuilder.prototype.indexGreaterThanEquals = function (name, val) {
  this._rangeKeyName = name
  this._keyConditions.filterAttributeGreaterThanEqual(name, val)
  return this
}

QueryBuilder.prototype.indexGreaterThan = function (name, val) {
  this._rangeKeyName = name
  this._keyConditions.filterAttributeGreaterThan(name, val)
  return this
}

QueryBuilder.prototype.indexBetween = function (name, val1, val2) {
  this._rangeKeyName = name
  this._keyConditions.filterAttributeBetween(name, val1, val2)
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
 * @param {?number} opt_limit The number of items to check
 * @return {Q.Promise.<DynamoResponse>}
 * @private
 */
QueryBuilder.prototype._repeatWithStartKey = function (nextKey, opt_limit) {
  if (!typ.isNullish(opt_limit)) {
    this.setLimit(opt_limit)
  }
  return this.setStartKey(nextKey).execute()
}

QueryBuilder.prototype.execute = function () {
  var query = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setQueryFilter(this._filters)
    .setConsistent(this._isConsistent)
    .setIndexName(this._indexName)
    .setKeyConditions([this._keyConditions])
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
    query.setIndexName(indexName)
  }

  var queryData = query.build()

  return this.request('query', queryData)
    .then(this.prepareOutput.bind(this))
    .fail(this.emptyResults)
    .failBound(this.convertErrors, null, {attributes: queryData, isWrite: false})
}

module.exports = QueryBuilder

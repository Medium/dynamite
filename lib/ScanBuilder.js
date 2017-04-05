var typ = require('typ')
var DynamoRequest = require('./DynamoRequest')
var DynamoResponse = require('./DynamoResponse')
var Builder = require('./Builder')
var IndexNotExistError = require('./errors').IndexNotExistError

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function ScanBuilder(options) {
  Builder.call(this, options)
}
require('util').inherits(ScanBuilder, Builder)

/**
 * If this query runs on a local index or global index, set a
 * function that can generate an index name based on query
 * conditions.
 *
 * @param {function(string, string): string} fn The generator function
 */
ScanBuilder.prototype.setIndexNameGenerator = function (fn) {
  this._indexNameGenerator = fn
  return this
}


ScanBuilder.prototype.setStartKey = function (key) {
  this._startKey = key
  return this
}

/**
 * @param {number} segment
 * @param {number} totalSegments
 * @return {ScanBuilder}
 */
ScanBuilder.prototype.setParallelScan = function (segment, totalSegments) {
  this._segment = segment
  this._totalSegments = totalSegments
  return this
}

/** @override */
ScanBuilder.prototype.prepareOutput = function (output) {
  return new DynamoResponse(
      this.getPrefix(), output, this._repeatWithStartKey.bind(this))
}

/**
 * @param {Object} nextKey
 * @param {?number} opt_limit The number of items to check
 * @return {Q.Promise.<DynamoResponse>}
 * @private
 */
ScanBuilder.prototype._repeatWithStartKey = function (nextKey, opt_limit) {
  if (!typ.isNullish(opt_limit)) {
    this.setLimit(opt_limit)
  }
  return this.setStartKey(nextKey).execute()
}

ScanBuilder.prototype.execute = function () {
  var query = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setScanFilter(this._filters)
    .setLimit(this._limit)
    .setStartKey(this._startKey)
    .selectAttributes(this._attributes)
    .setParallelScan(this._segment, this._totalSegments)

  if (this._indexNameGenerator) {
    var rangeKeyName = this._rangeKey ? this._rangeKey.name : ''
    var indexName = this._indexNameGenerator(this._hashKey.name, rangeKeyName)
    if (!indexName) {
      throw new IndexNotExistError(this._hashKey.name, this._rangeKey.name)
    }
    query.setIndexName(indexName)
  }

  var queryData = query.build()

  return this.request("scan", queryData)
    .then(this.prepareOutput.bind(this))
    .fail(this.emptyResults)
    .failBound(this.convertErrors, null, {data: queryData, isWrite: false})
}

module.exports = ScanBuilder

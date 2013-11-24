var common = require('./common')
var DynamoRequest = require('./DynamoRequest')
var Builder = require('./Builder')
var Q = require('kew')

/** @const */
var BATCH_LIMIT = 100

function BatchGetItemBuilder(options) {
  Builder.call(this, options)
  this._tableAttributes = {}
  this._tableConsistentRead = {}
  this._tableKeys = {}
}
require('util').inherits(BatchGetItemBuilder, Builder)


/**
 * Specify what attribute to get from a certain table.
 *
 * @param {string} table The name of the table to configure
 * @param {Array.<string>} attributes A list of attribute to get from the given table.
 * @return {BatchGetItemBuilder}
 */
BatchGetItemBuilder.prototype.setAttributesForTable = function (table, attributes) {
  this.queryData.setBatchRequestItems(this._tablePrefix, o)
  return this
}


/**
 * Specify if we should do consistent read on a certain table.
 *
 * @param {string} table The name of the table to configure
 * @param {boolean} consistentRead Indicate if we need to do consistent read on the
 *                  given table.
 * @return {BatchGetItemBuilder}
 */
BatchGetItemBuilder.prototype.setConsistentReadForTable = function (table, consistentRead) {
  this._tableConsistentRead[table] = consistentRead
  return this
}


/**
 * Request items from a certain table.
 *
 * @param {string} table The name of the table to request items from
 * @param {Array.<Object>} keys A set of primary keys to fetch.
 * @return {BatchGetItemBuilder}
 */
BatchGetItemBuilder.prototype.requestItems = function (table, keys) {
  if (!this._tableKeys[table]) this._tableKeys[table] = []
  this._tableKeys[table].push.apply(this._tableKeys[table], keys)
  return this
}


/**
 * @inheritDoc
 */
BatchGetItemBuilder.prototype.execute = function () {
  var promises = []
  var count = 0
  var batchTableKeys = {}

  // Divde all the items to fetch into braches, BATCH_LIMIT (100) items each.
  for (var tableName in this._tableKeys) {
    var keys = this._tableKeys[tableName]
    batchTableKeys[tableName] = []
    for (var i = 0; i < keys.length; i++) {
      if (count === BATCH_LIMIT) {
        promises.push(this._getAllItems(this._buildDynamoRequest(batchTableKeys)))
        batchTableKeys = {}
        batchTableKeys[tableName] = []
        count = 0
      }
      count += 1
      batchTableKeys[tableName].push(keys[i])
    }
  }
  if (count > 0) promises.push(this._getAllItems(this._buildDynamoRequest(batchTableKeys)))
  builder = this

  return Q.all(promises)
    .then(function (batches) {
      var all = batches[0]
      for (var i = 1; i < batches.length; i++) builder._mergeTwoBatches(all, batches[i])
      return all
    })
    .then(this.prepareOutput.bind(this))
    .fail(this.emptyResults)
}


/**
 * Return an object that represents the request data, for the purpose of
 * logging/debugging.
 *
 * @return {Object} Information about this request
 */
BatchGetItemBuilder.prototype.toObject = function () {
  return {
    options: this._options,
    attributes: this._tableAttributes,
    consistent: this._tableConsistentRead,
    items: this._tableKeys
  }
}


BatchGetItemBuilder.prototype._buildDynamoRequest = function (keys) {
  return new DynamoRequest(this.getOptions())
    .setBatchTableAttributes(this._tablePrefix, this._tableAttributes)
    .setBatchTableConsistent(this._tablePrefix, this._tableConsistentRead)
    .setBatchRequestItems(this._tablePrefix, keys)
    .returnConsumedCapacity()
    .build()
}


/**
 * Merge two batches of responses into one batch.
 *
 * @param{Object} batch One batch data returned from Dynamo.
 * @param{Object} anotherBatch Another batch data returned from Dynamo
 */
BatchGetItemBuilder.prototype._mergeTwoBatches = function (batch, anotherBatch) {
  for (var tableName in anotherBatch.Responses)  {
    if (!(tableName in batch.Responses)) {
      batch.Responses[tableName] = {Items: [], ConsumedCapacityUnits: 0}
    }
    var items = batch.Responses[tableName]
    items.push.apply(items, anotherBatch.Responses[tableName])
    batch.Responses[tableName].ConsumedCapacityUnits +=
      anotherBatch.Responses[tableName].ConsumedCapacityUnits
  }
}


/**
 * Get all the items and handle the Dynamo API limit size limit.
 *
 * @param {Object.<string, Array.<Object>>} keys A map from table name to requested
 *        keys in Dynamo API format.
 * @return {Promise.<Object>} The object is a typical Dynamo response.
 */
BatchGetItemBuilder.prototype._getAllItems = function (queryData) {
  var builder = this

  return this.request("batchGetItem", queryData)
    .then(function (data) {
      var unprocessedKeys = data.UnprocessedKeys
      if (unprocessedKeys && Object.keys(unprocessedKeys).length > 0) {
        // If there are unprocessed keys, keep fetching and append the results to
        // the current results.
        return builder._getAllItems(
          new DynamoRequest(builder.getOptions())
            .setData({'RequestItems': unprocessedKeys})
            .returnConsumedCapacity()
            .build())
          .then(function (moreData) {
            data.UnprocessedKeys = {}
            builder._mergeTwoBatches(data, moreData)
            return data
          })
      } else {
        return data
      }
    })
    .setContext({data: queryData, isWrite: false})
    .fail(this.convertErrors)
    .clearContext()
}


module.exports = BatchGetItemBuilder
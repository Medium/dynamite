// Copyright 2013. The Obvious Corporation.

var typeUtil = require('./typeUtil')

/**
 * @param {?string} tablePrefix
 * @param {Object} output Response JSON
 * @param {?function(Object, number?): Promise} repeatWithStartKey Make the same query with a different start key.
 *     Only valid for Query/Scan results.
 * @constructor
 */
var DynamoResponse = function (tablePrefix, output, repeatWithStartKey) {
  /** @private {?function(Object, number?): Promise} */
  this._repeatWithStartKey = repeatWithStartKey

  this.ConsumedCapacityUnits = output.ConsumedCapacityUnits
  this.Count = output.Count
  this.ProcessingStartedAt = output.ProcessingStartedAt
  this.RequestLatencyMs = output.RequestLatencyMs
  this.ByteLength = output.ByteLength

  this.LastEvaluatedKey = undefined
  if (output.LastEvaluatedKey) {
    this.LastEvaluatedKey = typeUtil.unpackObjectOrArray(output.LastEvaluatedKey)
  }

  // For batchGet
  this.UnprocessedKeys = undefined

  var table
  if (output.UnprocessedKeys) {
    var unprocessed = {}
    for (table in output.UnprocessedKeys) {
      unprocessed[table] = typeUtil.unpackObjectOrArray(output.UnprocessedKeys[table].Keys)
    }
    this.UnprocessedKeys = unprocessed
  }

  this.ConsumedCapacity = undefined
  if (output.ConsumedCapacity) {
    if (!Array.isArray(output.ConsumedCapacity)) {
      output.ConsumedCapacity = [output.ConsumedCapacity]
    }
    var capacity = {}
    output.ConsumedCapacity.forEach(function (outputCapacity) {
      capacity[getOriginalTableName(tablePrefix, outputCapacity.TableName)] = outputCapacity.CapacityUnits
    })
    this.ConsumedCapacity = capacity
  }

  this.result = undefined

  // for Query and Scan, 'result' is {Array.<Object>}
  if (output.Items) {
    this.result = typeUtil.unpackObjectOrArray(output.Items)

  // for GetItem, 'result' is {Object}
  } else if (output.Item) {
    this.result = typeUtil.unpackObjectOrArray(output.Item)

  // for DeleteItem, PutItem and UpdateItem, 'result' is {Object}
  } else if (output.UpdatedAttributes || output.Attributes) {
    this.result = typeUtil.unpackObjectOrArray(output.UpdatedAttributes)
    this.previous = typeUtil.unpackObjectOrArray(output.Attributes)

  // for BatchGetItem, 'result' is {Object.<string, Object>}
  } else if (output.Responses) {
    this.result = {}
    for (table in output.Responses) {
      var origTableName = getOriginalTableName(tablePrefix, table)
      this.result[origTableName] = typeUtil.unpackObjectOrArray(output.Responses[table])
    }
  }
  return this
}

/**
 * @return {boolean} If the query or scan has more results.
 */
DynamoResponse.prototype.hasNext = function () {
  return !!(this.LastEvaluatedKey && this._repeatWithStartKey)
}

/**
 * @param {?number} opt_limit The number of items to check
 * @return {Promise.<DynamoResponse>}
 */
DynamoResponse.prototype.next = function (opt_limit) {
  if (!this.hasNext()) throw new Error('No more results')
  return this._repeatWithStartKey(/** @type {Object} */ (this.LastEvaluatedKey), opt_limit)
}

function getOriginalTableName (tablePrefix, tableName) {
  return tablePrefix ? tableName.substr(tablePrefix.length) : tableName
}

module.exports = DynamoResponse

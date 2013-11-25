// Copyright 2013. The Obvious Corporation.

var typeUtil = require('./typeUtil')

/**
 * @param {?string} tablePrefix
 * @param {Object} output Response JSON
 * @constructor
 */
var DynamoResponse = function (tablePrefix, output) {
  this.ConsumedCapacityUnits = output.ConsumedCapacityUnits
  this.LastEvaluatedKey = output.LastEvaluatedKey
  this.Count = output.Count

  // For batchGet
  this.UnprocessedKeys = undefined
  if (output.UnprocessedKeys) {
    var unprocessed = {}
    for (var table in output.UnprocessedKeys) {
      unprocessed[table] = typeUtil.unpackObjects(output.UnprocessedKeys[table].Keys)
    }
    this.UnprocessedKeys = unprocessed
  }

  this.ConsumedCapacity = undefined
  if (output.ConsumedCapacity) {
    var capacity = {}
    for (var i = 0; i < output.ConsumedCapacity.length; i++) {
      capacity[output.ConsumedCapacity[i].TableName] = output.ConsumedCapacity[i].CapacityUnits
    }
    this.ConsumedCapacity = capacity
  }

  this.result = undefined
  if (output.Items) {
    this.result = typeUtil.unpackObjects(output.Items)
  } else if (output.Item) {
    this.result = typeUtil.unpackObjects([output.Item])[0]
  } else if (output.Attributes) {
    this.result = typeUtil.unpackObjects([output.Attributes])[0]
  } else if (output.Responses) {
    var result = {}
    for (var table in output.Responses) {
      var origTableName = tablePrefix ? table.substr(tablePrefix.length) : table
      result[origTableName] = typeUtil.unpackObjects(output.Responses[table])
    }
    this.result = result
  }
  return this
}

module.exports = DynamoResponse

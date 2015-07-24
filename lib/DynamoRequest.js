var ConditionBuilder = require('./ConditionBuilder')
var assert = require('assert')
var typeUtil = require('./typeUtil')

/**
 * @param {Object} options
 * @constructor
 */
function DynamoRequest(options) {
  this._options = options || {}
  this.data = {}
}

DynamoRequest.prototype.setData = function (data) {
  this.data = data
  return this
}


DynamoRequest.prototype.setTable = function (prefix, table) {
  this.data.TableName = (prefix ? prefix : '') + table
  return this
}

DynamoRequest.prototype.setItem = function (item) {
  if (item) {
    this.data.Item = {}
    for (var key in item) {
      this.data.Item[key] = typeUtil.valueToObject(item[key])
    }
  }
  return this
}

DynamoRequest.prototype.setParallelScan = function (segment, totalSegments) {
  if (typeof segment != 'undefined' && totalSegments) {
    this.data.Segment = segment
    this.data.TotalSegments = totalSegments
  }
  return this
}

DynamoRequest.prototype.returnConsumedCapacity = function () {
  this.data.ReturnConsumedCapacity = 'TOTAL'
  return this
}


/**
 * @param {!Object.<{val: ?, del: (boolean|number|string|Array), increment: number}>} updates
 */
DynamoRequest.prototype.setUpdates = function (updates) {
  if (updates) {
    this.data.AttributeUpdates = {}
    for (var key in updates) {
      if (updates[key].del) {
        if (updates[key].del === true) {
          this.data.AttributeUpdates[key] = {Action: 'DELETE'}
        } else {
          this.data.AttributeUpdates[key] = {
            Value: typeUtil.valueToObject(/** @type {number|string|Array} */ (updates[key].del)),
            Action: 'DELETE'
          }
        }
      } else if (typeof updates[key].increment !== 'undefined') {
        this.data.AttributeUpdates[key] = {Value: typeUtil.valueToObject(updates[key].increment), Action: 'ADD'}
      } else {
        this.data.AttributeUpdates[key] = {Value: typeUtil.valueToObject(updates[key].val), Action: 'PUT'}
      }
    }
  }
  return this
}

DynamoRequest.prototype.setReturnValues = function (returnValues) {
  if (returnValues) {
    this.data.ReturnValues = returnValues
  }
  return this
}

/**
 * @param {Array.<ConditionBuilder>} conditions An array of conditions, possibly null to indicate
 *     no conditions.
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setKeyConditions = function (conditions) {
  this.data.KeyConditions = {}
  ConditionBuilder.populateFieldFromConditionBuilderList(this.data.KeyConditions, conditions)
  return this
}

/**
 * @param {Array.<ConditionBuilder>} conditions An array of conditions, possibly null to indicate
 *     no conditions.
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setQueryFilter = function (conditions) {
  this.data.QueryFilter = {}
  ConditionBuilder.populateFieldFromConditionBuilderList(this.data.QueryFilter, conditions)
  return this
}

/**
 * @param {Array.<ConditionBuilder>} conditions An array of conditions, possibly null to indicate
 *     no conditions.
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setScanFilter = function (conditions) {
  this.data.ScanFilter = {}
  ConditionBuilder.populateFieldFromConditionBuilderList(this.data.ScanFilter, conditions)
  return this
}

/**
 * @param {Array.<ConditionBuilder>} conditions An array of conditions, possibly null to indicate
 *     no conditions.
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setExpected = function (conditions) {
  this.data.Expected = {}
  ConditionBuilder.populateFieldFromConditionBuilderList(this.data.Expected, conditions)
  return this
}

DynamoRequest.prototype.setConsistent = function (isConsistent) {
  this.data.ConsistentRead = !!isConsistent
  return this
}

DynamoRequest.prototype.setLimit = function (limit) {
  if (limit) {
    this.data.Limit = limit
  }
  return this
}

DynamoRequest.prototype.setHashKey = function (key) {
  this.data.Key = {}
  if (!key) throw new Error('A hash key is required')

  this.data.Key[key.name] = typeUtil.valueToObject(key.val)

  return this
}

DynamoRequest.prototype.setRangeKey = function (key) {
  if (!this.data.Key) throw new Error('The hash key must be set first')
  if (!key) throw new Error('A range key is required')

  this.data.Key[key.name] = typeUtil.valueToObject(key.val)

  return this
}

DynamoRequest.prototype.setStartKey = function (key) {
  if (key) {
    this.data.ExclusiveStartKey = typeUtil.packObjectOrArray(key)
  }
  return this
}

DynamoRequest.prototype.selectAttributes = function (attributes, isDynamoRequest) {
  if (attributes) {
    if (isDynamoRequest) this.data.Select = 'SPECIFIC_ATTRIBUTES'
    this.data.AttributesToGet = attributes
  } else {
    if (isDynamoRequest) this.data.Select = 'ALL_ATTRIBUTES'
  }
  return this
}

DynamoRequest.prototype.setIndexName = function (indexName) {
  if(indexName) {
    this.data.IndexName = indexName
  }
  return this
}


DynamoRequest.prototype.setBatchTableAttributes = function (tablePrefix, attributes) {
  this._setPerTableValue(tablePrefix, attributes, 'AttributesToGet')
  return this
}


DynamoRequest.prototype.setBatchTableConsistent = function (tablePrefix, isConsistentValues) {
  this._setPerTableValue(tablePrefix, isConsistentValues, 'ConsistentRead')
  return this
}


DynamoRequest.prototype._setPerTableValue =  function (tablePrefix, values, propertyName) {
  if (values) {
    if (!this.data.RequestItems) this.data.RequestItems = {}
    for (var key in values) {
      var tableName = (tablePrefix || '') + key
      if (!this.data.RequestItems[tableName]) this.data.RequestItems[tableName] = {}
      this.data.RequestItems[tableName][propertyName] = values[key]
    }
  }
}


/**
 * Takes a map items to Dynamo request format. requestItems is an object containing an array
 * of Primary Keys for each table.  For example:
 *
 * { userTable : [{userId: '1234', column: '@'} ... ]}
 */
DynamoRequest.prototype.setBatchRequestItems = function (tablePrefix, requestItems) {
  if (!this.data.RequestItems) this.data.RequestItems = {}
  for (var tableName in requestItems) {
    if (!Array.isArray(requestItems[tableName])) {
      throw new Error('RequestedItems not an array, for table=' + tableName)
    }
    var tableNameWithPrefix = (tablePrefix || '') + tableName
    if (!this.data.RequestItems[tableNameWithPrefix]) this.data.RequestItems[tableNameWithPrefix] = {}
    if (!this.data.RequestItems[tableNameWithPrefix].Keys) this.data.RequestItems[tableNameWithPrefix].Keys = []
    for (var i = 0; i < requestItems[tableName].length; i++) {
      var keys = requestItems[tableName][i]
      var dynamoKeys = {}
      for (var key in keys) dynamoKeys[key] = typeUtil.valueToObject(keys[key])
      this.data.RequestItems[tableNameWithPrefix].Keys.push(dynamoKeys)
    }
  }
  return this
}


DynamoRequest.prototype.scanForward = function (isForward) {
  this.data.ScanIndexForward = typeof isForward === 'undefined' || isForward
  return this
}

DynamoRequest.prototype.getCount = function () {
  this.data.Select = "COUNT"
  return this
}

DynamoRequest.prototype.build = function () {
  return this.data
}

module.exports = DynamoRequest

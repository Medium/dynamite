var ConditionBuilder = require('./ConditionBuilder')
var typeUtil = require('./typeUtil')


/**
 * @param {Object} options
 * @constructor
 */
function DynamoRequest(options) {
  this._options = options || {}
  this.data = {_requestBuilder: this}

  this._nameMutex = {count: 0}
  this._keyConditionBuilder = null
  this._filterBuilder = null
}

DynamoRequest.prototype.setRequestItems = function (keys) {
  this.data.RequestItems = keys
  return this
}


DynamoRequest.prototype.setTable = function (prefix, table) {
  this.data.TableName = (prefix ? prefix : '') + table
  return this
}

/**
 * For putItem requests, the item we want to write to DynamoDB
 * @param {Object} item
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setItem = function (item) {
  if (item) {
    this.data.Item = typeUtil.packObjectOrArray(item)
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

/**
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.returnConsumedCapacity = function () {
  this.data.ReturnConsumedCapacity = 'TOTAL'
  return this
}


/**
 * @param {!Object} attributeUpdates
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setUpdates = function (attributeUpdates) {
  // TODO(nick): Switch this to use UpdateExpression
  if (attributeUpdates) {
    this.data.AttributeUpdates = attributeUpdates
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
  if (conditions) {
    this._keyConditionBuilder = ConditionBuilder.populateExpressionField(
        this.data, 'KeyConditionExpression', conditions, this._nameMutex)
  }
  return this
}

/**
 * @param {Array.<ConditionBuilder>} conditions An array of conditions, possibly null to indicate
 *     no conditions.
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setQueryFilter = function (conditions) {
  if (conditions) {
    this._filterBuilder = ConditionBuilder.populateExpressionField(
        this.data, 'FilterExpression', conditions, this._nameMutex)
  }
  return this
}

/**
 * @param {Array.<ConditionBuilder>} conditions An array of conditions, possibly null to indicate
 *     no conditions.
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setScanFilter = function (conditions) {
  if (conditions) {
    this._filterBuilder = ConditionBuilder.populateExpressionField(
        this.data, 'FilterExpression', conditions, this._nameMutex)
  }
  return this
}

/**
 * @param {Array.<ConditionBuilder>} conditions An array of conditions, possibly null to indicate
 *     no conditions.
 * @return {DynamoRequest}
 */
DynamoRequest.prototype.setExpected = function (conditions) {
  // TODO(nick): Switch this to use ConditionExpression
  this.data.Expected = {}
  ConditionBuilder.populateFieldFromConditionBuilderList(this.data.Expected, conditions)
  return this
}

DynamoRequest.prototype.setConsistent = function (isConsistent) {
  this.data.ConsistentRead = !!isConsistent
  return this
}

/**
 * For query and scan requests, the number of items to iterate over (before the filter)
 * @param {number} limit
 * @return {DynamoRequest}
 */
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

DynamoRequest.prototype.selectAttributes = function (attributes) {
  if (attributes) {
    if (!this.data.ExpressionAttributeNames) {
      this.data.ExpressionAttributeNames = {}
    }
    typeUtil.extendAttributeNames(this.data.ExpressionAttributeNames, typeUtil.buildAttributeNames(attributes))

    this.data.ProjectionExpression = attributes.map(function (attr) {
      return typeUtil.getAttributeAlias(attr)
    }).join(',')
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
  if (this.data.ExpressionAttributeNames &&
      !Object.keys(this.data.ExpressionAttributeNames).length) {
    delete this.data.ExpressionAttributeNames
  }

  if (this.data.ExpressionAttributeValues &&
      !Object.keys(this.data.ExpressionAttributeValues).length) {
    delete this.data.ExpressionAttributeValues
  }

  return this.data
}

module.exports = DynamoRequest

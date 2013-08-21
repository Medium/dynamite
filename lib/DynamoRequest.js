var ConditionBuilder = require('./ConditionBuilder')
var assert = require('assert')

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
      this.data.Item[key] = valueToObject(item[key])
    }
  }
  return this
}

DynamoRequest.prototype.returnConsumedCapacity = function () {
  this.data.ReturnConsumedCapacity = 'TOTAL'
  return this
}

DynamoRequest.prototype.setKeyConditions = function (keyConditions) {
  this.data.KeyConditions = {}
  for (var i = 0; i < keyConditions.length; i++) {
    var keyCondition = keyConditions[i]
    var conditionData = this.data.KeyConditions[keyCondition.name] = {
      AttributeValueList: [],
      ComparisonOperator: keyCondition.operator
    }
    for (var j = 0; j < keyCondition.attributes.length; j++) {
      conditionData.AttributeValueList.push(valueToObject(keyCondition.attributes[j]))
    }
  }
  return this
}

DynamoRequest.prototype.setUpdates = function (updates) {
  if (updates) {
    this.data.AttributeUpdates = {}
    for (var key in updates) {
      if (updates[key].del) {
        this.data.AttributeUpdates[key] = {Action: 'DELETE'}
      } else if (typeof updates[key].increment !== 'undefined') {
        this.data.AttributeUpdates[key] = {Value: valueToObject(updates[key].increment), Action: 'ADD'}
      } else {
        this.data.AttributeUpdates[key] = {Value: valueToObject(updates[key].val), Action: 'PUT'}
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

DynamoRequest.prototype.setFilter = function (filter) {
  if (filter) {
    this.data.ScanFilter = {}
    for (var key in filter) {
      if (filter[key][0] === 'BETWEEN') {
        this.data.ScanFilter[key] = {AttributeValueList:[valueToObject(filter[key][1]), valueToObject(filter[key][2])], ComparisonOperator: filter[key][0]}
      } else if (filter[key][0] === 'IN') {
        var values = []
        for (var i = 1; i < filter[key].length; i += 1) {
          values.push(valueToObject(filter[key][i]))
        }
        this.data.ScanFilter[key] = {AttributeValueList: values, ComparisonOperator: filter[key][0]}
      } else {
        this.data.ScanFilter[key] = {AttributeValueList:[valueToObject(filter[key][1])], ComparisonOperator: filter[key][0]}
      }
    }
  }
  return this
}

/**
 * @param {Array.<ConditionBuilder>} conditions An array of conditions, possibly null to indicate
 *     no conditions.
 */
DynamoRequest.prototype.setExpected = function (conditions) {
  this.data.Expected = {}
  if (conditions) {
    assert.ok(Array.isArray(conditions), 'Expected array')
    for (var i = 0; i < conditions.length; i += 1) {
      assert.ok(conditions[i] instanceof ConditionBuilder, 'Expected ConditionBuilder')
      for (var key in conditions[i]._attributes) {
        var attr = conditions[i]._attributes[key]
        if (attr.absent) {
          this.data.Expected[key] = {Exists: false}
        } else {
          this.data.Expected[key] = {Value: valueToObject(attr.val)}
        }
      }
    }
  }
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

  this.data.Key[key.name] = valueToObject(key.val)

  return this
}

DynamoRequest.prototype.setRangeKey = function (key) {
  if (!this.data.Key) throw new Error('The hash key must be set first')
  if (!key) throw new Error('A range key is required')

  this.data.Key[key.name] = valueToObject(key.val)

  return this
}

DynamoRequest.prototype.setStartKey = function (key) {
  if (key) {
    this.data.ExclusiveStartKey = key
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
      for (var key in keys) dynamoKeys[key] = valueToObject(keys[key])
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

/**
 * @param {number|string|Array} value
 */
function valueToObject(value) {
  var type = typeof value

  if (Array.isArray(value)) {
    var firstItemType = typeof value[0]

    // check that all of the items are of the same type; that of the first element's
    for (var i = 0; i < value.length; i++) {
      if (typeof value[i] !== firstItemType) {
        throw new Error('Inconsistent types in set! Expecting all types to be the same as the first element\'s: ' + firstItemType)
      }
    }

    if (firstItemType === 'string') {
      return {SS: value}
    } else {
      if (firstItemType !== 'number') {
        throw new Error('Invalid dynamo set value. Type: ' + firstItemType + ', Value: ' + value[0])
      }

      var numArray = []
      for (var i = 0; i < value.length; i++) {
        numArray.push(String(value[i]))
      }

      return {NS: numArray}
    }
  } else if (type === 'string') {
    return {S: value}
  } else {
    if (type !== 'number') {
      // TODO(nick): I'm pretty sure this should be an error. But there is a bunch
      // of code relying on this behavior, so just log the error for now and
      // we'll track down the problems in the logs.
      //
      // Xiao: Nick, agreed. Examples of places that depend on this behavior:
      // (1) ScanBuilder.filterAttributeNull
      // (2) ScanBuilder.filterAttributeNotNull
      console.error('Invalid dynamo value. Type: ' + type + ', Value: ' + value)
    }

    return {N: String(value)}
  }
}

module.exports = DynamoRequest

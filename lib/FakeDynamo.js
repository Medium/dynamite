var Q = require('kew')
var typ = require('typ')
var typeUtil = require('../lib/typeUtil')

var util = require('util')
var inspect = function (o) { return util.inspect(o, {depth: null, colors: true})}

/** @const */
var MAX_GET_BATCH_ITEM_SIZE = 100

/**
 * @param {string} name
 * @param {string} message
 * @constructor
 * @extends {Error}
 */
function DynamoError(name, message) {
  this.name = name
  this.code = name
  this.message = message
}
util.inherits(DynamoError, Error)

/**
 * Fake table which can be used to store mock data and run queries
 *
 * @constructor
 * @param {string} name table name
 */
function FakeTable(name) {
  this.name = name
  this.primaryKey = {
    hash: null,
    range: null
  }
  this.data = {}

  /** @private {number} */
  this._maxResultSetSize = MAX_GET_BATCH_ITEM_SIZE
}

/**
 * Set a hard upper limit at the amount of results that queries and scans on this table can return.
 * @param {number} maxResultSetSize
 */
FakeTable.prototype.setMaxResultSetSize = function (maxResultSetSize) {
  this._maxResultSetSize = maxResultSetSize
}

/**
 * Set data for the mock table
 *
 * @param {!Object} data
 * @return {FakeTable} the mock table instance
 */
FakeTable.prototype.setData = function(data) {
  this.data = data
  return this
}

/**
 * Get all data from the mock table
 *
 * @return {Object} data
 */
FakeTable.prototype.getData = function(data) {
  return this.data
}

/**
 * Set the hash attribute from the primary key for this table
 *
 * @param {string} name the name of the attribute
 * @param {string} type the type of the attribute ('S', 'N', etc.)
 * @return {FakeTable} the current FakeTable instance
 */
FakeTable.prototype.setHashKey = function(name, type) {
  this.primaryKey.hash = {
    name: name,
    type: type
  }

  return this
}

/**
 * Set the range attribute from the primary key for this table
 *
 * @param {string} name the name of the attribute
 * @param {string} type the type of the attribute ('S', 'N', etc.)
 * @return {FakeTable} the current FakeTable instance
 */
FakeTable.prototype.setRangeKey = function(name, type) {
  this.primaryKey.range = {
    name: name,
    type: type
  }

  return this
}

/**
 * Run a query against the mock data
 *
 * @param {Object} data
 * @return {Q.Promise}
 */
FakeTable.prototype.query = function(data) {
  var indexedKeyName
  if (!!data.IndexName) {
    // Global Secondary Index if hash key doesn't exist as either key condition
    if (!data.KeyConditions.hasOwnProperty(this.primaryKey.hash.name)) {
      return this._queryGlobalSecondaryIndex(data)
    }

    // Extract the range key for Local Secondary Indexes
    for (var key in data.KeyConditions) {
      if (key !== this.primaryKey.hash.name) {
        indexedKeyName = key
      }
    }
  } else { // no index specified, mock query original table
    indexedKeyName = this.primaryKey.range.name
  }

  // retrieve the hashkey
  var hash = data.HashKeyValue ||
             parseAmazonAttribute(data.KeyConditions[this.primaryKey.hash.name].AttributeValueList[0]).value

  var indexedKeyValues = !!data.IndexName ? this._getIndexedKeyValuesForHash(hash, indexedKeyName) : this._getRangeKeyValuesForHash(hash)

  // maybe add some stuff for secondary indices like
  // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
  // var indexName = data.IndexName || 'primary'

  // extract comparison operator for range key
  if (!!data.KeyConditions[indexedKeyName]) {
    var comp_op = data.KeyConditions[indexedKeyName].ComparisonOperator
    var comparator = this._extractComparisonOp(comp_op)
  }

  var results = []
  var isBackward = data.ScanIndexForward === false
  for (var i = 0; i < indexedKeyValues.length; i++) {
    var j = isBackward ? indexedKeyValues.length - 1 - i : i

    var argValues = []
    if (!!comparator) {
      var args = data.KeyConditions[indexedKeyName].AttributeValueList.map(function (attr) {
        var parsed = parseAmazonAttribute(attr)
        argValues.push(parsed.value)
        return parsed
      })
    }

    // todo(artem): if no range key is specified, then all of the items
    // with the same hash key need to be returned
    if (!comparator || comparator(indexedKeyValues[j], argValues)) {
      if (!!data.IndexName) {
        results.push.apply(results, this._getItemsByIndex({
          hash: hash,
          indexedKey: indexedKeyValues[j]
        }, indexedKeyName))
      } else {
        results.push(this._getItemByKey({
          hash: hash,
          range: indexedKeyValues[j]
        }))
      }
    }
  }

  return this._formatPagedResults(results, data)
}

/**
 * Query the mock data as if it respects Global Secondary Index.
 *
 * It does not matter, for the purposes of FakeDynamo, which KeyCondition is
 * the hash key or range key. We simply do a scan to find all matches for both
 * conditions.
 */
FakeTable.prototype._queryGlobalSecondaryIndex = function(data) {
  // store keys, values, and conditions from data for easy access in scan
  var keysValuesConditions = []
  for (var key in data.KeyConditions) {
    // do we need to check if comparator exists?
    var comparator = this._extractComparisonOp(data.KeyConditions[key].ComparisonOperator)

    // values always in array due to between operation
    var values = data.KeyConditions[key].AttributeValueList.map(function (attr) {
      return parseAmazonAttribute(attr).value
    })
    keysValuesConditions[key] = {"values": values, "comparator": comparator}
  }

  var results = []
  for (var primary_key in this.data) {
    for (var range_key in this.data[primary_key]) {
      var currentRow = this.data[primary_key][range_key]

      // both key constraints must be satisfied to match
      var isMatch = true
      for (var key in keysValuesConditions) {
        var comparator = keysValuesConditions[key].comparator
        var values = keysValuesConditions[key].values
        if (!comparator(currentRow[key], values)) {
          isMatch = false
          break
        }
      }

      if (isMatch) {
        results.push(currentRow)
      }
    }
  }
  return this._formatPagedResults(results, data)
}

/**
 * Run a scan against the mock data
 *
 * @param {Object} data
 * @return {Q.Promise}
 */
FakeTable.prototype.scan = function(data) {
  return this._formatPagedResults(this._sortDataByPrimaryKey(), data)
}


/**
 * Filter and format the results of a query or scan.
 * @param {Array} results
 * @param {Object} queryData
 * @return {Q.Promise.<Object>}
 */
FakeTable.prototype._formatPagedResults = function (results, queryData) {
  var filteredResults = this._filterByExclusiveStartKey(results, queryData)

  var limit = Math.min(queryData.Limit || Infinity, this._maxResultSetSize)
  var ret = {}

  if (filteredResults.length > limit) {
    ret.LastEvaluatedKey = this._createLastEvaluatedKey(filteredResults[limit - 1])
    filteredResults.length = limit
  }

  // If the client only requires the count, return the only the count
  if (queryData.Select === 'COUNT') {
    ret.Count = filteredResults.length
    return Q.resolve(ret)
  }

  var attributes = this._getAttributesFromData(queryData)
  ret.Items = typeUtil.packObjectOrArray(filteredResults, attributes)
  ret.ConsumedCapacity = this._getCapacityBlob(ret.Items.length)
  return Q.resolve(ret)
}


/**
 * @param {Array} sortedDbData
 * @param {Object} queryData
 * @return {Array} A filtered data array.
 */
FakeTable.prototype._filterByExclusiveStartKey = function (sortedDbData, queryData) {
  var hashKeyName = this.primaryKey.hash.name
  var rangeKeyName = this.primaryKey.range.name
  var exclusiveStartKey = queryData.ExclusiveStartKey ?
      typeUtil.unpackObjectOrArray(queryData.ExclusiveStartKey) : undefined

  // TODO(nick): Fix this to work for backward queries/scans.
  var results = []
  for (var i = 0; i < sortedDbData.length; i++) {
    if (exclusiveStartKey) {
      if (sortedDbData[i][hashKeyName] < exclusiveStartKey[hashKeyName] ||
          (sortedDbData[i][hashKeyName] === exclusiveStartKey[hashKeyName] &&
          sortedDbData[i][rangeKeyName] <= exclusiveStartKey[rangeKeyName])) {
        continue
      }
    }
    results.push(sortedDbData[i])
  }
  return results
}


/**
 * @param {Object} item
 * @return {Object|undefined}
 */
FakeTable.prototype._createLastEvaluatedKey = function (item) {
  var primaryKeyAttribute = {}
  primaryKeyAttribute[this.primaryKey.hash.name] = true
  primaryKeyAttribute[this.primaryKey.range.name] = true
  return typeUtil.packObjectOrArray(item, primaryKeyAttribute)
}


/**
 * Get the appropriate comparator function to compare the passed in values against
 * @param {string} operator
 * @return {function(string, Array): boolean}
 */
FakeTable.prototype._extractComparisonOp = function(operator) {
  switch (operator) {
  case 'BEGINS_WITH':
    return function(keyA, keyB) { return keyA.indexOf(keyB[0]) === 0 }
  case 'EQ':
    return function(keyA, keyB) { return keyA == keyB[0] }
  case 'LE':
    return function(keyA, keyB) { return keyA <= keyB[0] }
  case 'LT':
    return function(keyA, keyB) { return keyA < keyB[0] }
  case 'GE':
    return function(keyA, keyB) { return keyA >= keyB[0] }
  case 'GT':
    return function(keyA, keyB) { return keyA > keyB[0] }
  case 'BETWEEN':
    return function(keyA, keyB) { return keyA >= keyB[0] && keyA <= keyB[1] }
  default:
    throw new Error('Invalid comparison operator \'' + operator + '\'')
  }
}

/**
 * Put an item into the mock data
 *
 * @param {Object} data
 * @return {Q.Promise}
 */
FakeTable.prototype.putItem = function(data) {
  var key = this._extractKey(data)
  if (data.Expected) {
    var item = this._getItemByKey(key)
    this._checkExpected(data.Expected, item)
  }

  // create the item to store
  var obj = {}
  for (var field in data.Item) {
    obj[field] = parseAmazonAttribute(data.Item[field]).value
  }

  // store the item
  this._putItemAtKey(key, obj)


  // done (ALL_NEW only returns ConsumedCapacity)
  return Q.resolve({
    ConsumedCapacity: this._getCapacityBlob(1)
  })
}

/**
 * Update an item in the mock data
 *
 * @param {Object} data
 * @return {Q.Promise}
 */
FakeTable.prototype.updateItem = function(data) {
  var key = this._extractKey(data)
  var item = this._getItemByKey(key)

  // run the conditional if it exists
  if (data.Expected) {
    this._checkExpected(data.Expected, item)
  }

  // if the item doesn't exist, create a temp item which we may save later
  if (!item) {
    item = {}
    item[this.primaryKey.hash.name] = key.hash
    if (this.primaryKey.range.name) {
      item[this.primaryKey.range.name] = key.range
    }
  }

  // whether we should save the item later. if the item exists, all updates
  // should save, otherwise we only save on PUT or ADD
  var shouldSave = !! item

  for (var field in data.AttributeUpdates) {
    var update = data.AttributeUpdates[field]

    if (update.Action === 'DELETE') {
      // From the dynamo docs:
      //
      // "If no value is specified, the attribute and its value are removed
      // from the item. The data type of the specified value must match the
      // existing value's data type.
      //
      // If a set of values is specified, then those values are subtracted
      // from the old set. For example, if the attribute value was the set
      // [a,b,c] and the DELETE action specified [a,c], then the final
      // attribute value would be [b]. Specifying an empty set is an error."
      var attribute = parseAmazonAttribute(update.Value)
      if (attribute.value === undefined) {
        // delete a field if it exists
        delete item[field]
      } else if (attribute.type.length > 1) {
        // remove from array
        for (var i = 0; i < attribute.value.length; i++) {
          var valueToDelete = attribute.value[i]
          var idx = item[field].indexOf(valueToDelete)
          if (idx != -1) {
            item[field].splice(idx, 1)
          }
        }
        if (item[field].length === 0) {
          delete item[field]
        }
      }
    } else if (update.Action === 'PUT') {
      var attribute = parseAmazonAttribute(update.Value)
      if (attribute.value !== undefined) {
        // set the value of a field
        shouldSave = true
        item[field] = attribute.value
      }
    } else if (update.Action === 'ADD') {
      // increment a field
      shouldSave = true
      var attribute = parseAmazonAttribute(update.Value)
      if (attribute.type.length > 1) {
        // append to an array
        if (typ.isNullish(item[field])) item[field] = []
        if (!Array.isArray(item[field])) throw new Error('Trying to append to a non-array')

        for (var i = 0; i < attribute.value.length; i++) {
          var valueToAdd = attribute.value[i]
          if (item[field].indexOf(valueToAdd) == -1) {
            item[field].push(valueToAdd)
          }
        }

      } else if (attribute.type == 'N') {
        // increment a number
        item[field] = typ.isNullish(item[field]) ? attribute.value : item[field] + attribute.value

      } else {
        throw new Error('Trying to ADD to a field which isnt an array or number')
      }
    }
  }

  if (shouldSave) {
    // store the item
    this._putItemAtKey(key, item)

    // done (ALL_NEW only returns ConsumedCapacity)
    return Q.resolve({
      Attributes: typeUtil.packObjectOrArray(item),
      ConsumedCapacity: this._getCapacityBlob(1)
    })
  } else {
    // done (ALL_NEW only returns ConsumedCapacity)
    return Q.resolve({
      Attributes: {},
      ConsumedCapacity: this._getCapacityBlob(1)
    })
  }
}

/**
 * Delete an item from the mock data
 *
 * @param {Object} data
 * @return {Q.Promise}
 */
FakeTable.prototype.deleteItem = function(data) {
  var key = this._extractKey(data)
  var item = this._getItemByKey(key)
  if (data.Expected) {
    this._checkExpected(data.Expected, item)
  }
  this._putItemAtKey(key, undefined)
  return Q.resolve({
    ConsumedCapacity: this._getCapacityBlob(1)
  })
}

/**
 * Get an item from the mock data
 *
 * @param {Object} data
 * @return {Q.Promise}
 */
FakeTable.prototype.getItem = function(data) {
  var key = this._extractKey(data)
  var item = this._getItemByKey(key)
  var attributes = this._getAttributesFromData(data)

  return Q.resolve({
    Item: typeUtil.packObjectOrArray(item, attributes),
    ConsumedCapacity: this._getCapacityBlob(1)
  })
}

/**
 * Get a JSON blob for the consumed capacity data.
 */
FakeTable.prototype._getCapacityBlob = function(n) {
  return {
    CapacityUnits: n,
    TableName: this.name
  }
}

/**
 * Retrieve list of selected attributes from request data.
 * If data.AttributesToGet is falsy, that implies that all attributes
 * should be returned.
 *
 * @param {Object} data
 * @return {Object|undefined} map of attribute names to a boolean true value
 */
FakeTable.prototype._getAttributesFromData = function(data) {
  if (data.AttributesToGet) {
    var attributes = {}
    for (var i = 0; i < data.AttributesToGet.length; i++) {
      attributes[data.AttributesToGet[i]] = true
    }
    return attributes
  } else return undefined // Todo (gianni): this seems a fragile indicator to me
}

/**
 * For mutating operations, check if the conditional is valid
 *
 * @param {Object} expected expected data
 * @param {Object|undefined} currentData current data for the data being changed
 */
FakeTable.prototype._checkExpected = function(expected, currentData) {
  for (var key in expected) {
    if (!typ.isNullish(expected[key].Exists) && !expected[key].Exists) {
      // if the field shouldn't exist
      if (currentData && !typ.isNullish(currentData[key])) {
        throw new DynamoError('ConditionalCheckFailedException', 'Attribute found when none expected.')
      }
    } else {
      // if the field should match a specific value
      if (!currentData) {
        throw new DynamoError('ConditionalCheckFailedException', 'Attribute missing when expected.')
      }
      var attribute = parseAmazonAttribute(expected[key].Value)
      if (currentData[key] !== attribute.value) {
        throw new DynamoError('ConditionalCheckFailedException', 'Values are different than expected.')
      }
    }
  }
}

/**
 * Get an item by its key
 *
 * @param {{hash:(number|string), range:(number|string)}} key
 * @return {Object|undefined} object
 */
FakeTable.prototype._getItemByKey = function(key) {
  if (!this.data[key.hash]) return undefined
  return this.primaryKey.range ? this.data[key.hash][key.range] : this.data[key.hash]
}

/**
 * Gets an item by the Range Key (LSI)
 */
FakeTable.prototype._getItemsByIndex = function(key, indexedKey) {
  if (!this.data[key.hash]) return undefined
  var items = []
  for (var rangeKey in this.data[key.hash]) {
    if (this.data[key.hash][rangeKey][indexedKey] === key.indexedKey) {
      items.push(this.data[key.hash][rangeKey])
    }
  }
  return items
}

/**
 * Put an item at its key
 *
 * @param {{hash:(number|string), range:(number|string)}} key
 * @param {*} obj
 */
FakeTable.prototype._putItemAtKey = function(key, obj) {
  if (this.primaryKey.range) {
    if (!this.data[key.hash]) this.data[key.hash] = {}
    if (!obj) {
      delete this.data[key.hash][key.range]
    } else {
      this.data[key.hash][key.range] = obj
    }
  } else {
    if (!obj) {
      delete this.data[key.hash]
    } else {
      this.data[key.hash] = obj
    }
  }
}

/**
 * Retrieve all range keys for a specified hash key
 *
 * @param {string} hash
 * @return {Array.<string>}
 */
FakeTable.prototype._getRangeKeyValuesForHash = function(hash) {
  if (!this.data[hash]) return []
  return Object.keys(this.data[hash]).sort()
}

/**
 * Retrieve all attribute values for a specified hash key and attribute
 *
 * @param {string} hash
 * @return {Array.<string>}
 */
FakeTable.prototype._getIndexedKeyValuesForHash = function(hash, attr) {
  if (!this.data[hash]) return []
  if (!attr || attr === this.primaryKey.range.name) {
    return this._getRangeKeyValuesForHash(hash)
  }
  var keys = []
  for (var key in this.data[hash]) {
    var indexedKey = this.data[hash][key][attr]
    if (keys.indexOf(indexedKey) < 0) {
      keys.push(indexedKey)
    }
  }
  return keys
}

/**
 * Extract a range key from request data
 *
 * @param {Object} data request data
 * @return {{hash:(number|string), range:(number|string)}}
 */
FakeTable.prototype._extractKey = function(data) {
  var key = {}

  if (data.Key) {
    key.hash = data.Key[this.primaryKey.hash.name][this.primaryKey.hash.type]
    if (this.primaryKey.range) {
      key.range = data.Key[this.primaryKey.range.name][this.primaryKey.range.type]
    }
  } else {
    key.hash = data.Item[this.primaryKey.hash.name][this.primaryKey.hash.type]
    if (this.primaryKey.range) {
      key.range = data.Item[this.primaryKey.range.name][this.primaryKey.range.type]
    }
  }

  return key
}

/**
 * Provides an array of the original data sorted by the Primary key, used as a Hash key here.
 */
FakeTable.prototype._sortDataByPrimaryKey = function() {
  var sortedData = []
  var hashKeys = Object.keys(this.data).sort()
  for (var i = 0; i < hashKeys.length; i++) {
    var rangeKeys = Object.keys(this.data[hashKeys[i]]).sort()
    for (var j = 0; j < rangeKeys.length; j++) {
      sortedData.push(this.data[hashKeys[i]][rangeKeys[j]])
    }
  }
  return sortedData
}

/**
 * Get a partial descirption of the table, format as specified by aws docs.
 * @return {Object} table description
 */
FakeTable.prototype.describeTable = function() {
  return {
    'Table': {
      'AttributeDefinitions': [
        {'AttributeName': this.primaryKey.hash.name, 'AttributeType': this.primaryKey.hash.type},
        {'AttributeName': this.primaryKey.range.name, 'AttributeType': this.primaryKey.range.type}
      ],
      'KeySchema': [
        {'AttributeName': this.primaryKey.hash.name, 'KeyType': 'HASH'},
        {'AttributeName': this.primaryKey.range.name, 'KeyType': 'RANGE'}
      ],
      'TableName': this.name,
      'TableStatus': 'ACTIVE'
    }
  }
}

/**
 * Create a Fake Dynamo client
 * @constructor
 */
function FakeDynamo() {
  this.tables = {}
  this.resetStats()
}


/**
 * Get stats counters
 */
FakeDynamo.prototype.getStats = function() {
  return this.stats
}

/**
 * Reset stats counters
 */
FakeDynamo.prototype.resetStats = function() {
  this.stats = {
    putItem: 0,
    getItem: 0,
    deleteItem: 0,
    updateItem: 0,
    query: 0,
    batchGetItem: 0,
    batchGetItemCount: []
  }
}

/**
 * Create a new table in the Fake Dynamo client
 *
 * @param {string} name table name
 * @return {FakeTable} a mock table
 */
FakeDynamo.prototype.createTable = function(name) {
  return this.tables[name] = new FakeTable(name)
}

/**
 * Get a mock table if it exists
 *
 * @param {string} name
 * @return {FakeTable} the mock table
 */
FakeDynamo.prototype.getTable = function(name) {
  if (!this.tables[name]) throw new Error('Table "' + name + '" has not been mocked!')
  return this.tables[name]
}

function performOp(operation) {
  return function (data, callback) {
    try {
      this.stats[operation] += 1
      this.getTable(data.TableName)[operation](data)
        .fail(function (e) {
          callback(e)
        })
        .then(function (data) {
          callback(null, data)
        })
    } catch (e) {
      callback(e)
    }
  }
}

FakeDynamo.prototype.putItem = performOp('putItem')
FakeDynamo.prototype.getItem = performOp('getItem')
FakeDynamo.prototype.deleteItem = performOp('deleteItem')
FakeDynamo.prototype.updateItem = performOp('updateItem')
FakeDynamo.prototype.query = performOp('query')
FakeDynamo.prototype.scan = performOp('scan')

/**
 * Respond a batchGetItem request.
 *
 * @param {Object} data The request data
 * @return {Q.Promise.<Object>} the fetched items in Dynamo format
 */
FakeDynamo.prototype.batchGetItem = function (data, callback) {
  var promises = []
  var unprocessedKeys = {}
  var resp = {Responses: {}, UnprocessedKeys: {}, ConsumedCapacity: []}
  this.stats.batchGetItem += 1

  try {
    // the number of fetched object across all tables.
    var count = 0
    for (var tableName in data.RequestItems) {
      var keys = data.RequestItems[tableName].Keys

      // Check for duplicates
      var keySet = {}
      keys.forEach(function (k) {
        keySet[JSON.stringify(k)] = true
      })
      if (Object.keys(keySet).length != keys.length) {
        throw new Error('ValidationException: Provided list of item keys contains duplicates')
      }

      var table = this.getTable(tableName)
      var limit = Math.min(table._maxResultSetSize, MAX_GET_BATCH_ITEM_SIZE)
      var countPerTable = 0
      for (var i = 0; i < keys.length; i++) {
        countPerTable++
        count++
        if (count <= limit) {
          promises.push(
            table.getItem({Item: keys[i], AttributesToGet: data.RequestItems[tableName].AttributesToGet})
              .then(function (data) {
                if (!(tableName in resp.Responses)) {
                  resp.Responses[tableName] = []
                }
                if (!typ.isNullish(data.Item)) {
                  resp.Responses[tableName].push(data.Item)
                }
              })
          )
        } else {
          if (!(tableName in resp.UnprocessedKeys)) {
            resp.UnprocessedKeys[tableName] = {Keys: []}
          }
          resp.UnprocessedKeys[tableName].Keys.push(keys[i])
        }
      }
      resp.ConsumedCapacity.push(table._getCapacityBlob(countPerTable))
    }
    this.stats.batchGetItemCount.push(count)

    return Q.all(promises)
      .fail(function (e) {
        callback(e)
      })
      .then(function () {
        callback(null, resp)
      })
  } catch (e) {
    callback(e)
  }
}

/**
 * Get the partial description of a table, format as specified by aws docs.
 * @param  {Object} tableData data related to the table
 * @return {Q.Promise.<Object>} description of the named table
 */
FakeDynamo.prototype.describeTable = function (tableData, callback) {
  var table = this.tables[tableData.TableName]
  if (!table) callback(new Error('No such table in FakeDynamo: ' + tableData.TableName))
  return callback(null, table.describeTable())
}

/**
 * Extract the data type from an Amazon attribute (format {type: val})
 *
 * @param {Object} attribute
 * @param {boolean=} allowEmpty
 * @return {{type:string, value:Object}}
 */
function parseAmazonAttribute(attribute, allowEmpty) {
  var val = {}
  for (var key in attribute) {
    val.type = key
    if (key === 'N')
      val.value = Number(attribute[key])
    else if (key === 'S' && !! allowEmpty || (typeof attribute[key] === 'string' && attribute[key].length)) {
      val.value = attribute[key]
    } else if (key === 'NS') {
      val.value = attribute[key].map(function (num) { return Number(num) })
    } else if (key === 'SS') {
      val.value = attribute[key]
    } else {
      throw new Error('Unexpected key: ' + key + ' for attribute: ' + attribute)
    }
  }
  return val
}

module.exports = FakeDynamo

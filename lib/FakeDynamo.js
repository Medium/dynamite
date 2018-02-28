var Q = require('kew')
var typ = require('typ')
var typeUtil = require('./typeUtil')
var localUpdater = require('./localUpdater')

var util = require('util')

/** @const */
var MAX_GET_BATCH_ITEM_SIZE = 100

/** @const */
var MAX_KEY_SIZE = 1024

function forEachKeyCondition(data, callback) {
  return data._requestBuilder._keyConditionBuilder.visitExpressionsPostOrder(function (expr) {
    if (expr.op && expr.args[0] && expr.args[0].name) {
      callback(expr, expr.args[0].name)
    }
  })
}

function getKeyConditionByName(data, name) {
  var result = null
  forEachKeyCondition(data, function (condition, key) {
    if (key == name) {
      result = condition
    }
  })
  return result
}

function forEachFilterCondition(data, callback) {
  if (!data._requestBuilder._filterBuilder) return

  return data._requestBuilder._filterBuilder.visitExpressionsPostOrder(function (expr) {
    if (expr.op && expr.args[0] && expr.args[0].name) {
      callback(expr, expr.args[0].name)
    }
  })
}

function getKeyConditionFn(data) {
  return data._requestBuilder._keyConditionBuilder.buildFilterFn()
}

function getFilterFn(data) {
  var filterBuilder = data._requestBuilder._filterBuilder
  return filterBuilder ? filterBuilder.buildFilterFn() : function() { return true }
}

function getConditionFn(data) {
  var builder = data._requestBuilder._conditionBuilder
  return builder ? builder.buildFilterFn() : function() { return true }
}

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

  this.gsiDefinitions = []

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
FakeTable.prototype.getData = function() {
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
 * Set GSI definitions for this table.
 *
 * @param {Array.<{hash:({name:(string), type:(string)}), range:({name:(string), type:(string)})}>} gsiDefinitions
 * @return {FakeTable} the current FakeTable instance
 */
FakeTable.prototype.setGsiDefinitions = function(gsiDefinitions) {
  this.gsiDefinitions = gsiDefinitions

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
  if (data.IndexName) {
    // Global Secondary Index if the index name has three or more
    // parts (separated by '-')
    var indexParts = data.IndexName.split('-')
    var probableGSIIndex = indexParts.length >= 3
    if (probableGSIIndex) {
      return this._queryGlobalSecondaryIndex(data)
    }

    // Extract the range key for Local Secondary Indexes
    forEachKeyCondition(data, function (condition, key) {
      if (key !== this.primaryKey.hash.name) {
        indexedKeyName = key
      }
    }.bind(this))
  } else { // no index specified, mock query original table
    indexedKeyName = this.primaryKey.range.name
  }

  this._validateFilterKeys(data, this.primaryKey.hash.name, indexedKeyName)

  // retrieve the hashkey
  var hash = data.HashKeyValue ||
             getKeyConditionByName(data, this.primaryKey.hash.name).args[1].evaluate()

  var indexedKeyValues = data.IndexName ? this._getIndexedKeyValuesForHash(hash, indexedKeyName) : this._getRangeKeyValuesForHash(hash)

  // maybe add some stuff for secondary indices like
  // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
  // var indexName = data.IndexName || 'primary'

  var keyConditionFn = getKeyConditionFn(data)
  var queryFilterFn = getFilterFn(data)

  var results = []
  var isBackward = data.ScanIndexForward === false
  for (var i = 0; i < indexedKeyValues.length; i++) {
    var j = isBackward ? indexedKeyValues.length - 1 - i : i
    var currentRows
    if (data.IndexName) {
      currentRows = this._getItemsByIndex({
        hash: hash,
        indexedKey: indexedKeyValues[j]
      }, indexedKeyName)
    } else {
      currentRows = [this._getItemByKey({
        hash: hash,
        range: indexedKeyValues[j]
      })]
    }

    currentRows.forEach(function (currentRow) {
      if (keyConditionFn(currentRow) && queryFilterFn(currentRow)) {
        // TODO(artem): if no range key is specified, then all of the items
        // with the same hash key need to be returned
        results.push(currentRow)
      }
    })
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
  var indexParts = data.IndexName.split('-')
  var gsis = this.gsiDefinitions.filter(function (gsi) {
    return indexParts.indexOf(gsi.hash.name) >= 0 &&
      indexParts.indexOf(gsi.range.name) >= 0
  })

  if (gsis.length == 0) {
    throw new Error('No gsi found for ' + data.IndexName)
  }

  // We should only find 1 gsi, so use the first one
  this._validateFilterKeys(data, gsis[0].hash.name, gsis[0].range.name)

  // store keys, values, and conditions from data for easy access in scan
  var keyConditionFn = getKeyConditionFn(data)
  var queryFilterFn = getFilterFn(data)

  var results = []
  for (var primary_key in this.data) {
    for (var range_key in this.data[primary_key]) {
      var currentRow = this.data[primary_key][range_key]

      if (keyConditionFn(currentRow) && queryFilterFn(currentRow)) {
        results.push(currentRow)
      }
    }
  }

  // sort by the range key of the GSI (if there is any)
  var sortByKey
  forEachKeyCondition(data, function (condition, key) {
    if (!sortByKey && condition.op != 'EQ') {
      sortByKey = key
    }
  })
  if (sortByKey) {
    results.sort(function (a, b) {
      return data.ScanIndexForward ?
        a[sortByKey] - b[sortByKey] :
        b[sortByKey] - a[sortByKey]
    })
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
  var sortedData = this._sortDataByPrimaryKey()
  var scanFilterFn = getFilterFn(data)

  var results = []
  for (var primary_key in sortedData) {
    var currentRow = sortedData[primary_key]
    if (scanFilterFn(currentRow)) {
      results.push(currentRow)
    }
  }

  return this._formatPagedResults(results, data)
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
  if (!exclusiveStartKey) {
    return sortedDbData
  }

  var startHashKey = exclusiveStartKey[hashKeyName]
  var startRangeKey = exclusiveStartKey[rangeKeyName]
  var matchIndex = -1

  sortedDbData.forEach(function (item, i) {
    var itemHashKey = item[hashKeyName]
    var itemRangeKey = item[rangeKeyName]
    if (itemHashKey == startHashKey && itemRangeKey == startRangeKey) {
      matchIndex = i
    }
  })

  return sortedDbData.slice(matchIndex + 1)
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
 * Put an item into the mock data
 *
 * @param {Object} data
 * @return {Q.Promise}
 */
FakeTable.prototype.putItem = function(data) {
  var key = this._extractKey(data)

  var item = this._getItemByKey(key)
  this._checkExpected(getConditionFn(data), item)

  // create the item to store
  var obj = {}
  for (var field in data.Item) {
    obj[field] = typeUtil.objectToValue(data.Item[field])
  }

  // store the item
  this._putItemAtKey(key, obj)


  // done (ALL_OLD only returns ConsumedCapacity)
  return Q.resolve({
    Attributes: typeUtil.packObjectOrArray(item),
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
  this._checkExpected(getConditionFn(data), item)

  var itemExists = !!item

  // if the item doesn't exist, create a temp item which we may save later
  if (!itemExists) {
    item = {}
    item[this.primaryKey.hash.name] = key.hash
    if (this.primaryKey.range.name) {
      item[this.primaryKey.range.name] = key.range
    }
  }

  var oldItem = typeUtil.packObjectOrArray(item)
  var newItem = localUpdater.update(oldItem, data._requestBuilder._updateExpressionBuilder._attributes)

  // store the item
  this._putItemAtKey(key, typeUtil.unpackObjectOrArray(newItem))

  // done (ALL_OLD only returns ConsumedCapacity)
  return Q.resolve({
    Attributes: itemExists ? oldItem : null,
    ConsumedCapacity: this._getCapacityBlob(1)
  })
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
  this._checkExpected(getConditionFn(data), item)
  this._putItemAtKey(key, undefined)

  // done (ALL_OLD only returns ConsumedCapacity)
  return Q.resolve({
    Attributes: typeUtil.packObjectOrArray(item),
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
 * @param {function(Object)} filterFn A filter function that returns false if the item is filtered, true if allowed.
 * @param {Object|undefined} currentData current data for the data being changed
 */
FakeTable.prototype._checkExpected = function(filterFn, currentData) {
  if (!filterFn(currentData)) {
    throw new DynamoError('ConditionalCheckFailedException', 'Values are different than expected.')
  }
}

/**
 * Throw an error if a key is too big.
 *
 * @param {{hash:(number|string), range:(number|string)}} key
 */
FakeTable.prototype._validateKeySize = function (key) {
  var size = Buffer.byteLength(String(key.hash), 'utf8')
  if (this.primaryKey.range) {
    size += Buffer.byteLength(String(key.range), 'utf8')
  }
  if (size > MAX_KEY_SIZE) {
    throw new DynamoError('ValidationException',
        'One or more parameter values were invalid: ' +
        'Aggregated size of all range keys has exceeded the size limit of 1024 bytes')
  }
}

/**
 * Throw an error if a filter expression uses a primary key
 *
 * @param {Object} data
 * @param {string} partitionKey
 * @param {string} sortKey
 */
FakeTable.prototype._validateFilterKeys = function (data, partitionKey, sortKey) {
  forEachFilterCondition(data, function (condition, key) {
    if (key === partitionKey || key === sortKey) {
      throw new DynamoError(
        'ValidationException',
        'Filter Expression can only contain non-primary key attributes: Primary key attribute: ' + key
      )
    }
  })
}

/**
 * Get an item by its key
 *
 * @param {{hash:(number|string), range:(number|string)}} key
 * @return {Object|undefined} object
 */
FakeTable.prototype._getItemByKey = function(key) {
  this._validateKeySize(key)

  if (!this.data[key.hash]) return undefined
  return this.primaryKey.range ? this.data[key.hash][key.range] : this.data[key.hash]
}

/**
 * Gets an item by the Range Key (LSI)
 */
FakeTable.prototype._getItemsByIndex = function(key, indexedKey) {
  this._validateKeySize(key)

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
  this._validateKeySize(key)

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
  var keys = Object.keys(this.data[hash])
  this._sortKeys(keys)
  return keys
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
  this._sortKeys(keys)
  return keys
}

/**
 * Sorts the array of keys in place. This method assumes that all the keys are of the same type.
 */
FakeTable.prototype._sortKeys = function (keys) {
  if (keys.length > 0 && !isNaN(keys[0])) {
    keys.sort(function(a, b) { return a - b })
  } else {
    keys.sort()
  }
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
      'TableStatus': 'ACTIVE',
      'ProvisionedThroughput': {
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
      }
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

  this.isFakeDynamo = true
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
        throw new DynamoError('ValidationException', 'Provided list of item keys contains duplicates')
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

module.exports = FakeDynamo

var Q = require('kew')
var typ = require('typ')

var util = require('util')
var inspect = function (o) { return util.inspect(o, {depth: null, colors: true})}

/** @const */
var MAX_GET_BATCH_ITEM_SIZE = 100

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
}

/**
 * Set data for the mock table
 *
 * @param {Object} data
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
 * @return {Promise}
 */
FakeTable.prototype.query = function(data) {
  var hash = data.HashKeyValue || parseAmazonAttribute(
    data.KeyConditions[this.primaryKey.hash.name].AttributeValueList[0]).value

  var rangeKeys = this._getRangeKeysForHash(hash)
  var limit = data.Limit || -1

  if (data.ExclusiveStartKey) {
    var index = rangeKeys.indexOf(data.ExclusiveStartKey[this.primaryKey.range.name])
    rangeKeys.splice(0, index + 1) // take off all the lesser range keys, and the start key
  }

  // maybe add some stuff for secondary indices like
  // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
  // var indexName = data.IndexName || 'primary'

  var attributes = this._getAttributesFromData(data)
  if (!!data.KeyConditions[this.primaryKey.range.name]) {
    var comp_op = data.KeyConditions[this.primaryKey.range.name].ComparisonOperator
    var comparator = this._extractComparisonOp(comp_op)
  }

  var results = []
  var isBackward = data.ScanIndexForward === false
  for (var i = 0; i < rangeKeys.length  && (limit === -1 || results.length < limit); i++) {
    var j = isBackward ? rangeKeys.length - 1 - i : i

    if (!!comparator) {
      var argValues = []
      var args = data.KeyConditions[this.primaryKey.range.name].AttributeValueList.map(function (attr) {
        var parsed = parseAmazonAttribute(attr)
        argValues.push(parsed.value)
        return parsed
      })
    }

    // todo(artem): if no range key is specified, then all of the items
    // with the same hash key need to be returned
    if (!comparator || comparator(rangeKeys[j], argValues)) {
      results.push(this._getItemByKey({
        hash: hash,
        range: rangeKeys[j]
      }))
    }
  }

  if (data.Select === 'COUNT') {
    return Q.resolve({
      Count: results.length,
      ConsumedCapacityUnits: 1
    })
  } else {
    return Q.resolve({
      Items: this._packObjects(attributes, results),
      ConsumedCapacityUnits: 1
    })
  }
}

/**
 * get the appropriate comparator function forto compare the passed in values against
 * @param {string} operator
 * @return {function(Array.<string>): boolean}
 */
FakeTable.prototype._extractComparisonOp = function(operator) {
  switch (operator) {
  case 'BEGINS_WITH':
    return function(key) { return key.indexOf(arguments[1][0]) === 0 }
  case 'EQ':
    return function(key) { return key == arguments[1][0] }
  case 'LE':
    return function(key) { return key <= arguments[1][0] }
  case 'LT':
    return function(key) { return key < arguments[1][0] }
  case 'GE':
    return function(key) { return key >= arguments[1][0] }
  case 'GT':
    return function(key) { return key > arguments[1][0] }
  case 'BETWEEN':
    return function(key) { return key >= arguments[1][0] && key <= arguments[1][1] }
  default:
    throw new Error('Invalid comparison operator \'' + data.RangeKeyCondition.ComparisonOperator + '\'')
  }
}

/**
 * Put an item into the mock data
 *
 * @param {Object} data
 * @return {Promise}
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

  // done (ALL_NEW only returns ConsumedCapacityUnits)
  return Q.resolve({
    ConsumedCapacityUnits: 1
  })
}

/**
 * Update an item in the mock data
 *
 * @param {Object} data
 * @return {Promise}
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

    // done (ALL_NEW only returns ConsumedCapacityUnits)
    return Q.resolve({
      Attributes: this._packObjects(undefined, item),
      ConsumedCapacityUnits: 1
    })
  } else {
    // done (ALL_NEW only returns ConsumedCapacityUnits)
    return Q.resolve({
      Attributes: {},
      ConsumedCapacityUnits: 1
    })
  }
}

/**
 * Delete an item from the mock data
 *
 * @param {Object} data
 * @return {Promise}
 */
FakeTable.prototype.deleteItem = function(data) {
  var key = this._extractKey(data)
  var item = this._getItemByKey(key)
  if (data.Expected) {
    this._checkExpected(data.Expected, item)
  }
  this._putItemAtKey(key, undefined)
  return Q.resolve({
    ConsumedCapacityUnits: 1
  })
}

/**
 * Delete an item from the mock data
 *
 * @param {Object} data
 * @return {Promise}
 */
FakeTable.prototype.getItem = function(data) {
  var key = this._extractKey(data)
  var item = this._getItemByKey(key)
  var attributes = this._getAttributesFromData(data)

  return Q.resolve({
    Item: this._packObjects(attributes, item),
    ConsumedCapacityUnits: 1
  })
}

/**
 * Retrieve list of selected attributes from request data.
 * If data.AttributesToGet is falsy, that implies that all attributes
 * should be returned.
 *
 * @param {Object} data
 * @return {Object} map of attribute names to a boolean true value
 */
FakeTable.prototype._getAttributesFromData = function(data) {
  if (data.AttributesToGet) {
    var attributes = {}
    for (var i = 0; i < data.AttributesToGet.length; i++) {
      attributes[data.AttributesToGet[i]] = true
    }
    return attributes
  } else return undefined
}

/**
 * Pack objects into the expected response format from Dynamo (which
 * uses field types as keys in a map of key to value)
 *
 * @param {Object|Array.<Object>} object
 * @return {Object|Array.<Object>} serialized object
 */
FakeTable.prototype._packObjects = function(attributes, object) {
  if (typ.isNullish(object)) return object
  if (Array.isArray(object)) {
    return object.map(this._packObjects.bind(this, attributes))
  }

  var newObj = {}
  for (var key in object) {
    if (attributes && !attributes[key]) continue

    if (Array.isArray(object[key])) {
      // array field
      var firstType = typeof object[key][0]
      var consistentType = firstType
      object[key].map(function (attr) {
        if ((typeof attr) !== firstType) throw new Error('Array types must all be the same')
      })

      if (firstType === 'string') {
        newObj[key] = {
          SS: object[key]
        }
      } else if (firstType === 'number') {
        newObj[key] = {
          NS: object[key]
        }
      } else {
        throw new Error('Only arrays of strings and numbers are allowed. Unknown type: ' + firstType)
      }
    } else if (typeof object[key] === 'string') {
      // string field
      newObj[key] = {
        S: object[key]
      }
    } else if (typeof object[key] === 'number') {
      // number field
      newObj[key] = {
        N: object[key]
      }
    } else {
      throw new Error('Unknown object type at ' + key)
    }
  }
  return newObj
}

/**
 * For mutating operations, check if the conditional is valid
 *
 * @param {Object} expected expected data
 * @param {Object} currentData current data for the data being changed
 */
FakeTable.prototype._checkExpected = function(expected, currentData) {
  for (var key in expected) {
    if (!typ.isNullish(expected[key].Exists) && !expected[key].Exists) {
      // if the field shouldn't exist
      if (currentData && !typ.isNullish(currentData[key])) throw new Error('conditional request failed')
    } else {
      // if the field should match a specific value
      if (!currentData) throw new Error('conditional request failed')
      var attribute = parseAmazonAttribute(expected[key].Value)
      if (currentData[key] !== attribute.value) throw new Error('conditional request failed')
    }
  }
}

/**
 * Get an item by its key
 *
 * @param {{hash:(number|string), range:(number|string)}} key
 * @return {Object} object
 */
FakeTable.prototype._getItemByKey = function(key) {
  if (!this.data[key.hash]) return undefined
  return this.primaryKey.range ? this.data[key.hash][key.range] : this.data[key.hash]
}

/**
 * Put an item at its key
 *
 * @param {{hash:(number|string), range:(number|string)}} key
 * @return {Object} object
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
FakeTable.prototype._getRangeKeysForHash = function(hash) {
  if (!this.data[hash]) return []
  return Object.keys(this.data[hash]).sort()
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
 * Create a Fake Dynamo client
 */
function FakeDynamo() {
  this.tables = {}
  this.resetStats()
}


/**
 * Reset stats counters
 *
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

/**
 * Respond a batchGetItem request.
 *
 * @param {Object} data The request data
 * @return {Promise.<Object>} the fetched items in Dynamo format
 */
FakeDynamo.prototype.batchGetItem = function (data, callback) {
  var promises = []
  var unprocessedKeys = {}
  var resp = {Responses: {}, UnprocessedKeys: {}}
  this.stats.batchGetItem += 1

  try {
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

      for (var i = 0; i < keys.length; i++) {
        count++
        if (count <= MAX_GET_BATCH_ITEM_SIZE) {
          promises.push(
            this.getTable(tableName)
              .getItem({Item: keys[i], AttributesToGet: data.RequestItems[tableName].AttributesToGet})
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
 * Extract the data type from an Amazon attribute (format {type: val})
 *
 * @param {Object} attribute
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

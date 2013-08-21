var util = require('util')
var Q = require('kew')
var errors = require('./errors')

function Builder(options) {
  this._options = options || {}
}

Builder.prototype.setHashKey = function (keyName, keyVal) {
  this._hashKey = {
    name: keyName,
    val: keyVal
  }
  return this
}

Builder.prototype.setRangeKey = function (keyName, keyVal) {
  this._rangeKey = {
    name: keyName,
    val: keyVal
  }
  return this
}

Builder.prototype.getRetryHandler = function () {
  return this._options.retryHandler
}

Builder.prototype.getOptions = function () {
  return this._options
}

Builder.prototype.setDatabase = function (db) {
  this._db = db
  return this
}

Builder.prototype.setConsistent = function (isConsistent) {
  this._isConsistent = isConsistent
  return this
}

Builder.prototype.consistentRead = function () {
  return this.setConsistent(true)
}

Builder.prototype.setPrefix = function (prefix) {
  this._tablePrefix = prefix
  return this
}

Builder.prototype.setTable = function (table) {
  this._table = table
  return this
}

Builder.prototype.setLimit = function (limit) {
  this._limit = limit
  return this
}

Builder.prototype.scanForward = function () {
  this._shouldScanForward = true
  return this
}

Builder.prototype.scanBackward = function () {
  this._shouldScanForward = false
  return this
}

Builder.prototype.getCount = function () {
  this._isCount = true
  return this
}

Builder.prototype.withCondition = function (condition) {
  if (!this._conditions) this._conditions = []
  if (condition) this._conditions.push(condition)
  return this
}

Builder.prototype.selectAttributes = function (attributes) {
  if (!attributes) return this
  if (!Array.isArray(attributes)) attributes = Array.prototype.slice.call(arguments, 0)
  this._attributes = attributes
  return this
}

Builder.prototype.emptyResults = function (e) {
  if (e.message === 'Requested resource not found') return {results:[]}
  throw e
}

Builder.prototype.emptyResult = function (e) {
  if (e.message === 'Requested resource not found') return {results:null}
  throw e
}

Builder.prototype.request = function (method, data) {
  if (this._options.logQueries) {
    this.logQuery(method, data)
  }

  var defer = Q.defer()
  var req = this._db[method](data, defer.makeNodeResolver())

  var retryHandler = this.getRetryHandler()
  var table = this._table
  if (retryHandler) {
    req.on('retry', function () {
      retryHandler(method, table)
    })
  }
  return defer.promise
}

Builder.prototype.logQuery = function (method, data) {
  var cyanBold, cyan, reset, logline;
  cyanBold = '\u001b[1;36m';
  cyan     = '\u001b[0;36m';
  reset    = '\u001b[0m';
  console.info(cyanBold + method + cyan)
  console.info(util.inspect(data, {depth: null}))
  console.info(reset)
}

Builder.prototype.prepareOutput = function (output) {
  var data = {}

  if (output.ConsumedCapacityUnits) data.ConsumedCapacityUnits = output.ConsumedCapacityUnits
  if (output.ConsumedCapacityUnits !== undefined) data.ConsumedCapacityUnits = output.ConsumedCapacityUnits
  if (output.LastEvaluatedKey) data.LastEvaluatedKey = output.LastEvaluatedKey
  if (output.Count !== undefined) data.Count = output.Count

  // For batchGet
  if (output.UnprocessedKeys) {
    var unprocessed = {}
    for (var table in output.UnprocessedKeys) {
      unprocessed[table] = _unpackObjects(output.UnprocessedKeys[table].Keys)
    }
    data.UnprocessedKeys = unprocessed
  }
  if (output.ConsumedCapacity) {
    var capacity = {}
    for (var i = 0; i < output.ConsumedCapacity.length; i++) {
      capacity[output.ConsumedCapacity[i].TableName] = output.ConsumedCapacity[i].CapacityUnits
    }
    data.ConsumedCapacity = capacity
  }

  if (output.Items) data.result = _unpackObjects(output.Items)
  else if (output.Item) data.result = _unpackObjects([output.Item])[0]
  else if (output.Attributes) data.result = _unpackObjects([output.Attributes])[0]
  else if (output.Responses) {
    var result = {}
    for (var table in output.Responses) {
      var origTableName = this._tablePrefix ? table.substr(this._tablePrefix.length) : table
      result[origTableName] = _unpackObjects(output.Responses[table])
    }
    data.result = result
  }
  return data
}

Builder.prototype.convertErrors = function (e, context) {
  var data = context.data
  var isWrite = !!context.isWrite

  if (isConditionalError(e)) throw new errors.ConditionalError(data, e.message)
  else if (isProvisioningError(e)) throw new errors.ProvisioningError(data, e.message, isWrite)
  else if (isValidationError(e)) throw new errors.ValidationError(data, e.message, isWrite)
  else throw e
}

function objectToValue(obj) {
  if (obj.hasOwnProperty('SS')) {
    return obj.SS
  } else if (obj.hasOwnProperty('NS')) {
    var numArray = []
    for (var i = 0; i < obj.NS.length; i++) {
      numArray.push(Number(obj.NS[i]))
    }
    return numArray
  }

  return obj.hasOwnProperty('S') ? obj.S : Number(obj.N)
}

function isConditionalError(e) {
  return e.message && e.message.indexOf('conditional') !== -1
}

function isProvisioningError(e) {
  return e.message && e.message.indexOf('provisioning') !== -1
}

function isValidationError(e) {
  return e.message && e.message.indexOf('ValidationException') !== -1
}

function _unpackObjects(objs) {
  var output = []
  for (var i = 0; i < objs.length; i += 1) {
    var item = {}
    for (var key in objs[i]) {
      item[key] = objectToValue(objs[i][key])
    }
    output.push(item)
  }
  return output
}

module.exports = Builder

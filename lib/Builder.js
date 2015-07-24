var util = require('util')
var Q = require('kew')
var errors = require('./errors')
var typeUtil = require('./typeUtil')
var DynamoResponse = require('./DynamoResponse')

/**
 * @constructor
 * @param {Object} options
 */
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

Builder.prototype.getPrefix = function () {
  return this._tablePrefix
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

Builder.prototype.withFilter = function (filter) {
  if (!this._filters) this._filters = []
  if (filter) this._filters.push(filter)
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

/** @this {*} */
Builder.prototype.emptyResults = function (e) {
  if (e.message === 'Requested resource not found') return {results:[]}
  throw e
}

/** @this {*} */
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
  return new DynamoResponse(this._tablePrefix, output, null)
}

Builder.prototype.convertErrors = function (context, err) {
  // Errors in Dynamo response are JSON objects like this:
  // {
  //   "message":"Attribute found when none expected.",
  //   "code":"ConditionalCheckFailedException",
  //   "name":"ConditionalCheckFailedException",
  //   "statusCode":400,
  //   "retryable":false
  // }
  //
  // More at http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ErrorHandling.html
  //
  // To be more reliable, we check both err.name and err.code.
  // Dynamo doc specifies the value of "code", so the error object
  // should have "code" assigned. The node.js SDK assigns "code"
  // to "name". "name" is the standard attribute of javascript Error
  // object, so we double-check it.
  var data = context.data
  var isWrite = !!context.isWrite

  switch (err.code || err.name) {
    case 'ConditionalCheckFailedException':
      throw new errors.ConditionalError(data, err.message)
    case 'ProvisionedThroughputExceededException':
      throw new errors.ProvisioningError(data, err.message, isWrite)
    case 'ValidationException':
      throw new errors.ValidationError(data, err.message, isWrite)
    default:
      throw err
  }
}

module.exports = Builder

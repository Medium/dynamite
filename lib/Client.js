var assert = require('assert')
var AWS = require('aws-sdk')

var AWSName = require('./common').AWSName
var ConditionBuilder = require('./ConditionBuilder')
var FilterBuilder = require('./FilterBuilder')

var DeleteItemBuilder = require('./DeleteItemBuilder')
var DescribeTableBuilder = require('./DescribeTableBuilder')
var BatchGetItemBuilder = require('./BatchGetItemBuilder')
var GetItemBuilder = require('./GetItemBuilder')
var PutItemBuilder = require('./PutItemBuilder')
var QueryBuilder = require('./QueryBuilder')
var ScanBuilder = require('./ScanBuilder')
var UpdateBuilder = require('./UpdateBuilder')
var errors = require('./errors')

/**
 * Creates an instance of Client which can be used to access Dynamo.
 *
 * The must-have information in 'options' are:
 *  - region
 *  - accessKeyId
 *  - secretAccessKey
 *
 * If region does not present, we try to infer it from other keys, e.g., 'host',
 * which is mainly for the backward compatibility with some old code that use
 * Dynamite.
 *
 * Dynamite current supports only the 2011 version of the API. It does not
 * concern the user of Dynamite though, because we do not expose the low level
 * APIs to Dynamite users.
 *
 * @constructor
 * @param {{dbClient:Object, host:string, region:string,
 *          accessKeyId:string, secretAccessKey:string, prefix: string,
 *          logQueries: boolean, retryHandler: Function}}
 *     options map which can be used to either configure accesss to Amazon's DynamoDB
 *     service using a host/region, accessKeyId, and secrectAccessKey or can provide
 *     a dbClient object which implements the interface per the AWS SDK for node.js.
 */
function Client(options) {
  this._prefix = options.prefix || ''

  this._commonOptions = {
    // whether to log out queries
    logQueries: !!options.logQueries
  }
  if (options.dbClient) {
    this.db = options.dbClient
  } else {
    if (!('region' in options)) {
      // If the options do not contain a 'region' key, we will try to refer
      // it from the values of other keys, e.g., 'host'.
      //
      // 'region' is what we need to initialize a database instance in the
      // AWS SDK.
      if ('endpoint' in options) {
        var endpoint = options['endpoint']
        for (var i = 0; i < AWSName.REGION.length; i++) {
          if (endpoint.indexOf(AWSName.REGION[i]) >= 0) {
            options['region'] = AWSName.REGION[i]
            break
          }
        }
      }
    }

    options['apiVersion'] = AWSName.API_VERSION_2012
    AWS.config.update(options)
    this.db = new AWS.DynamoDB()
  }
}

Client.prototype.describeTable = function (table) {
  return new DescribeTableBuilder(this._commonOptions)
    .setDatabase(this.db)
    .setPrefix(this._prefix)
    .setTable(table)
}

Client.prototype.newQueryBuilder = function (table) {
  return new QueryBuilder(this._commonOptions)
    .setDatabase(this.db)
    .setPrefix(this._prefix)
    .setTable(table)
}

Client.prototype.newBatchGetBuilder = function () {
  return new BatchGetItemBuilder(this._commonOptions)
    .setDatabase(this.db)
    .setPrefix(this._prefix)
}

Client.prototype.newUpdateBuilder = function (table) {
  assert.equal(arguments.length, 1, "newUpdateBuilder(table) only takes table name as an arg")
  return new UpdateBuilder(this._commonOptions)
    .setDatabase(this.db)
    .setPrefix(this._prefix)
    .setTable(table)
}

Client.prototype.newScanBuilder = function (table) {
  assert.equal(arguments.length, 1, "newScanBuilder(table) only takes table name as an arg")
  return new ScanBuilder(this._commonOptions)
    .setDatabase(this.db)
    .setPrefix(this._prefix)
    .setTable(table)
}

Client.prototype.getItem = function (table) {
  assert.equal(arguments.length, 1, "getItem(table) only takes table name as an arg")
  return new GetItemBuilder(this._commonOptions)
    .setDatabase(this.db)
    .setPrefix(this._prefix)
    .setTable(table)
}

Client.prototype.deleteItem = function (table) {
  assert.equal(arguments.length, 1, "deleteItem(table) only takes table name as an arg")
  return new DeleteItemBuilder(this._commonOptions)
    .setDatabase(this.db)
    .setPrefix(this._prefix)
    .setTable(table)
}

Client.prototype.putItem = function (table, item) {
  assert.equal(arguments.length, 2, "putItem(table, item) did not have 2 arguments")
  return new PutItemBuilder(this._commonOptions)
    .setDatabase(this.db)
    .setPrefix(this._prefix)
    .setTable(table)
    .setItem(item)
}

Client.prototype.newFilterBuilder = function () {
  return new FilterBuilder()
}

Client.prototype.newConditionBuilder = function () {
  return new ConditionBuilder()
}


/**
 * Returns a condition builder that guarantees that an item matches an expected
 * state. Keys that have null, undefined, or empty string values are expected
 * to not be present in the item being queried.
 *
 * @param {Object} obj A map of keys to verify.
 * @return {ConditionBuilder}
 */
Client.prototype.conditions = function (obj) {
  var conditionBuilder = this.newConditionBuilder()
  for (var key in obj) {
    if (typeof obj[key] === 'undefined' || obj[key] === null || obj[key] === '') {
      conditionBuilder.expectAttributeAbsent(key)
    } else {
      conditionBuilder.expectAttributeEquals(key, obj[key])
    }
  }
  return conditionBuilder
}


/**
 * Returns true if error is a Dynamo error indicating the table is throttled
 * @param {Error} e
 * @return {boolean}
 */
Client.isProvisioningError = function (e) {
  return e instanceof errors.ProvisioningError
}


/**
 * Returns true if error is a Dynamo error indicating the table is throttled
 * @param {Error} e
 * @return {boolean}
 */
Client.prototype.isProvisioningError = Client.isProvisioningError


/**
 * Returns true if error is a Dynamo error indicating a validation error
 * @param {Error} e
 * @return {boolean}
 */
Client.isValidationError = function (e) {
  return e instanceof errors.ValidationError
}


/**
 * Returns true if error is a Dynamo error indicating a validation error
 * @param {Error} e
 * @return {boolean}
 */
Client.prototype.isValidationError = Client.isValidationError


/**
 * Returns true if error is a Dynamo error indicating a condition wasn't met.
 * @param {Error} e
 * @return {boolean}
 */
Client.isConditionalError = function (e) {
  return e instanceof errors.ConditionalError
}


/**
 * Returns true if error is a Dynamo error indicating a condition wasn't met.
 * @param {Error} e
 * @return {boolean}
 */
Client.prototype.isConditionalError = Client.isConditionalError


/**
 * Returns false if the error indicates a condition failed, otherwise the error
 * is re-thrown.
 * @param {Error} e
 * @return {boolean}
 */
Client.throwUnlessConditionalError = function (e) {
  if (!Client.isConditionalError(e)) throw e
  return false
}


/**
 * Returns false if the error indicates a condition failed, otherwise the error
 * is re-thrown. It is safe to use this without binding it.
 * @param {Error} e
 * @return {boolean}
 */
Client.prototype.throwUnlessConditionalError = Client.throwUnlessConditionalError

module.exports = Client

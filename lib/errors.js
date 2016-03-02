// Copyright 2013. The Obvious Corporation.

var util = require('util')

/**
 * @param {Object} data
 * @param {string} msg
 * @constructor
 * @extends {Error}
 */
function ConditionalError(data, msg) {
  Error.captureStackTrace(this)
  this.data = data
  this.message = msg
}
util.inherits(ConditionalError, Error)
ConditionalError.prototype.type = 'ConditionalError'


/**
 * @param {Object} data
 * @param {string} msg
 * @param {boolean} isWrite
 * @constructor
 * @extends {Error}
 */
function ProvisioningError(data, msg, isWrite) {
  Error.captureStackTrace(this)
  this.data = data
  this.message = msg
  this.isWrite = isWrite
}
util.inherits(ProvisioningError, Error)
ProvisioningError.prototype.type = 'ProvisioningError'


/**
 * @param {Object} data
 * @param {string} msg
 * @param {boolean} isWrite
 * @constructor
 * @extends {Error}
 */
function ValidationError(data, msg, isWrite) {
  Error.captureStackTrace(this)
  this.data = data
  this.message = msg + '. Response follows: ' + data
  this.isWrite = isWrite
}
util.inherits(ValidationError, Error)
ValidationError.prototype.type = 'ValidationError'

/**
 * @param {string} hashKeyName
 * @param {string} rangeKeyName
 * @constructor
 * @extends {Error}
 */
function IndexNotExistError(hashKeyName, rangeKeyName) {
  Error.captureStackTrace(this)
  this.hashKeyName = hashKeyName
  this.rangeKeyName = rangeKeyName
}
util.inherits(IndexNotExistError, Error)
IndexNotExistError.prototype.type = 'IndexNotExistError'

/**
 * @param {string} returnValues
 * @constructor
 * @extends {Error}
 */
function InvalidReturnValuesError(returnValues) {
  Error.captureStackTrace(this)
  this.returnValues = returnValues
}
util.inherits(InvalidReturnValuesError, Error)
InvalidReturnValuesError.prototype.type = 'InvalidReturnValuesError'

module.exports = {
  ConditionalError: ConditionalError,
  ProvisioningError: ProvisioningError,
  ValidationError: ValidationError,
  IndexNotExistError: IndexNotExistError,
  InvalidReturnValuesError: InvalidReturnValuesError
}

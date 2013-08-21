// Copyright 2013. The Obvious Corporation.

var util = require('util')

/** @constructor */
function ConditionalError(data, msg) {
  this.data = data
  this.message = msg
}
util.inherits(ConditionalError, Error)
ConditionalError.prototype.type = 'ConditionalError'

/** @constructor */
function ProvisioningError(data, msg, isWrite) {
  this.data = data
  this.message = msg
  this.isWrite = isWrite
}
util.inherits(ProvisioningError, Error)
ProvisioningError.prototype.type = 'ProvisioningError'

/** @constructor */
function ValidationError(data, msg, isWrite) {
  this.data = data
  this.message = msg + ' Response follows:' + data
  this.isWrite = isWrite
}
util.inherits(ValidationError, Error)
ValidationError.prototype.type = 'ValidationError'

module.exports = {
  ConditionalError: ConditionalError,
  ProvisioningError: ProvisioningError,
  ValidationError: ValidationError
}

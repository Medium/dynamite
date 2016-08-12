var typ = require('typ')
var DynamoRequest = require('./DynamoRequest')
var DynamoResponse = require('./DynamoResponse')
var Builder = require('./Builder')
var typeUtil = require('./typeUtil')
var errors = require('./errors')

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function PutItemBuilder(options) {
  Builder.call(this, options)

  /** @private {string} */
  this._returnValues = PutItemBuilder.RETURN_VALUES.ALL_OLD
}
require('util').inherits(PutItemBuilder, Builder)

PutItemBuilder.RETURN_VALUES = {
  NONE: 'NONE',
  ALL_OLD: 'ALL_OLD',
  UPDATED_OLD: 'UPDATED_OLD',
  ALL_NEW: 'ALL_NEW',
  UPDATED_NEW: 'UPDATED_NEW'
}

PutItemBuilder.prototype.setReturnValues = function (val) {
  if (!PutItemBuilder.RETURN_VALUES[val]) {
    throw new errors.InvalidReturnValuesError(val)
  }

  this._returnValues = val
  return this
}

PutItemBuilder.prototype.setItem = function (item) {
  for (var key in item) {
    if (typ.isNullish(item[key])) {
      throw new Error("Field '" + key + "' on item must not be null or undefined")
    }
  }
  this._item = item
  return this
}

/** @override */
PutItemBuilder.prototype.prepareOutput = function (output) {
  if (this._returnValues !== 'NONE') {
    output.UpdatedAttributes = typeUtil.packObjectOrArray(this._item)
  }
  return new DynamoResponse(this.getPrefix(), output, null)
}

PutItemBuilder.prototype.execute = function () {
  var queryData = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setItem(this._item)
    .setExpected(this._conditions)
    .setReturnValues(this._returnValues)
    .build()

  return this.request("putItem", queryData)
    .then(this.prepareOutput.bind(this))
    .failBound(this.convertErrors, null, {data: queryData, isWrite: true})
}

module.exports = PutItemBuilder

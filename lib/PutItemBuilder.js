var common = require('./common')
var typ = require('typ')
var DynamoRequest = require('./DynamoRequest')
var DynamoResponse = require('./DynamoResponse')
var Builder = require('./Builder')
var typeUtil = require('./typeUtil')

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function PutItemBuilder(options) {
  Builder.call(this, options)
}
require('util').inherits(PutItemBuilder, Builder)

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
  output.UpdatedAttributes = typeUtil.packObjectOrArray(this._item)
  return new DynamoResponse(this.getPrefix(), output, null)
}

PutItemBuilder.prototype.execute = function () {
  var queryData = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setItem(this._item)
    .setExpected(this._conditions)
    .setReturnValues('ALL_OLD')
    .build()

  return this.request("putItem", queryData)
    .then(this.prepareOutput.bind(this))
    .failBound(this.convertErrors, null, {data: queryData, isWrite: true})
}

module.exports = PutItemBuilder

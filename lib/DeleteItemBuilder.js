var common = require('./common')
var DynamoRequest = require('./DynamoRequest')
var DynamoResponse = require('./DynamoResponse')
var Builder = require('./Builder')

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function DeleteItemBuilder(options) {
  Builder.call(this, options)
}
require('util').inherits(DeleteItemBuilder, Builder)

/** @override */
DeleteItemBuilder.prototype.prepareOutput = function (output) {
  output.UpdatedAttributes = null
  return new DynamoResponse(this.getPrefix(), output, null)
}

DeleteItemBuilder.prototype.execute = function () {
  var req = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setHashKey(this._hashKey, true)
    .setExpected(this._conditions)
    .setReturnValues('ALL_OLD')

  if (this._rangeKey) req.setRangeKey(this._rangeKey, true)

  var queryData = req.build()

  return this.request("deleteItem", queryData)
    .then(this.prepareOutput.bind(this))
    .failBound(this.convertErrors, null, {data: queryData, isWrite: true})
}

module.exports = DeleteItemBuilder

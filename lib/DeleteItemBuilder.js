var common = require('./common')
var DynamoRequest = require('./DynamoRequest')
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

DeleteItemBuilder.prototype.execute = function () {
  var queryData = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setHashKey(this._hashKey, true)
    .setRangeKey(this._rangeKey, true)
    .setExpected(this._conditions)
    .build()

  return this.request("deleteItem", queryData)
    .then(this.prepareOutput.bind(this))
    .setContext({data: queryData, isWrite: true})
    .fail(this.convertErrors)
    .clearContext()
}

module.exports = DeleteItemBuilder

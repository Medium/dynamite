var common = require('./common')
var DynamoRequest = require('./DynamoRequest')
var Builder = require('./Builder')

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function GetItemBuilder(options) {
  Builder.call(this, options)
}
require('util').inherits(GetItemBuilder, Builder)

GetItemBuilder.prototype.execute = function () {
  var queryData = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setConsistent(this._isConsistent)
    .setHashKey(this._hashKey, true);

  if (this.__rangeKey)
    queryData.setRangeKey(this._rangeKey, true)

  queryData = queryData.selectAttributes(this._attributes).build()

  return this.request("getItem", queryData)
    .then(this.prepareOutput.bind(this))
    .fail(this.emptyResult)
    .setContext({data: queryData, isWrite: false})
    .fail(this.convertErrors)
    .clearContext()
}

module.exports = GetItemBuilder

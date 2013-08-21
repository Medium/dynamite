var common = require('./common')
var DynamoRequest = require('./DynamoRequest')
var Builder = require('./Builder')

function GetItemBuilder(options) {
  Builder.call(this, options)
}
require('util').inherits(GetItemBuilder, Builder)

GetItemBuilder.prototype.execute = function () {
  var queryData = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setConsistent(this._isConsistent)
    .setHashKey(this._hashKey, true)
    .setRangeKey(this._rangeKey, true)
    .selectAttributes(this._attributes)
    .build()

  return this.request("getItem", queryData)
    .then(this.prepareOutput)
    .fail(this.emptyResult)
    .setContext({data: queryData, isWrite: false})
    .fail(this.convertErrors)
    .clearContext()
}

module.exports = GetItemBuilder
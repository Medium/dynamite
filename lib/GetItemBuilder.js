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
  var req = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setConsistent(this._isConsistent)
    .setHashKey(this._hashKey, true)
    .selectAttributes(this._attributes)

  if (this._rangeKey) req.setRangeKey(this._rangeKey, true)

  var queryData = req.build()

  return this.request("getItem", queryData)
    .then(this.prepareOutput.bind(this))
    .fail(this.emptyResult)
    .failBound(this.convertErrors, null, {data: queryData, isWrite: false})
}

module.exports = GetItemBuilder

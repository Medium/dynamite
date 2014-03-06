var common = require('./common')
var typ = require('typ')
var DynamoRequest = require('./DynamoRequest')
var Builder = require('./Builder')

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

PutItemBuilder.prototype.execute = function () {
  var queryData = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .returnConsumedCapacity()
    .setItem(this._item)
    .setExpected(this._conditions)
    .build()

  return this.request("putItem", queryData)
    .then(returnTrue)
    .setContext({data: queryData, isWrite: true})
    .fail(this.convertErrors)
    .clearContext()
}

/**
 * Always return true
 * @return {boolean} true!
 */
function returnTrue() {
  return true
}

module.exports = PutItemBuilder

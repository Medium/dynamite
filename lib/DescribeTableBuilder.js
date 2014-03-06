var common = require('./common')
var DynamoRequest = require('./DynamoRequest')
var Builder = require('./Builder')

/**
 * @param {Object} options
 * @constructor
 * @extends {Builder}
 */
function DescribeTableBuilder(options) {
  Builder.call(this, options)
}
require('util').inherits(DescribeTableBuilder, Builder)

DescribeTableBuilder.prototype.execute = function () {
  var queryData = new DynamoRequest(this.getOptions())
    .setTable(this._tablePrefix, this._table)
    .build()

  return this.request("describeTable", queryData)
    .setContext({data: queryData, isWrite: false})
    .fail(this.convertErrors)
    .clearContext()
}

module.exports = DescribeTableBuilder

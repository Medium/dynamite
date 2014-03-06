
// Require an event emitter, because some of these apis return emitters.
var EventEmitter = require('events').EventEmitter

var awsResponse = {
  CapacityUnits: 0,
  UnprocessedKeys: []
}

var queryResponse = {
  TableName: null,
  AttributesToGet: [],
  Limit: 1,
  ConsistentRead: true,
  Count: true,
  HashKeyValue: {
    S: '',
    N: 1,
    B: 'x',
    SS: [],
    NS: [],
    BS: []
  },
  RangeKeyCondition: {
    AttributeValueList: [],
    ComparisonOperator: null
  },
  ScanIndexForward: true,
  ExclusiveStartKey: {
    HashKeyElement: {S: ''},
    RangeKeyElement: {S: ''}
  }
}

/** @constructor */
function DynamoDB() {}

module.exports = {
  /** @constructor */
  DynamoDB: DynamoDB,

  config: {
    update: function (options) {}
  }
}

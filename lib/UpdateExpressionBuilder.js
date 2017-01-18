var typeUtil = require('./typeUtil')

// The UpdateExpression API has different names for all the action types, but they're
// semantically the same as the old actions.
function getActionName(attr) {
  switch (attr.Action) {
    case 'PUT':
      return 'SET'
    case 'ADD':
      return 'ADD'
    case 'DELETE':
      return attr.Value ? 'DELETE' : 'REMOVE'
    default:
      throw new Error('Unrecognized action ' + attr.Action)
  }
}

/**
 * Translates old AttributeValueUpdate format into the new UpdateExpression format.
 *
 * @param {Object} attributes
 * @constructor
 */
function UpdateExpressionBuilder(attributes) {
  this._attributes = attributes
  this._uniqueName = ''
}

/**
 * @param {Object} data
 * @param {Array<UpdateExpressionBuilder>} attributes
 * @param {{count: number}} nameMutex
 * @return {UpdateExpressionBuilder}
 */
UpdateExpressionBuilder.populateUpdateExpression = function (data, attributes, nameMutex) {
  var builder = new UpdateExpressionBuilder(attributes)
  builder._assignUniqueNames(nameMutex)

  if (!data.ExpressionAttributeNames) {
    data.ExpressionAttributeNames = {}
  }
  typeUtil.extendAttributeNames(data.ExpressionAttributeNames, builder.buildAttributeNames())

  if (!data.ExpressionAttributeValues) {
    data.ExpressionAttributeValues = {}
  }
  typeUtil.extendAttributeValues(data.ExpressionAttributeValues, builder.buildAttributeValues())

  data.UpdateExpression = builder.buildExpression()
  return builder
}

/**
 * @param {Object} nameMutex
 */
UpdateExpressionBuilder.prototype._assignUniqueNames = function (nameMutex) {
  if (!nameMutex.count) {
    nameMutex.count = 1
  }
  this._uniqueName = 'U' + nameMutex.count++
}

/** @return {Object} */
UpdateExpressionBuilder.prototype.buildAttributeNames = function () {
  return typeUtil.buildAttributeNames(Object.keys(this._attributes))
}

/** @return {Object} */
UpdateExpressionBuilder.prototype.buildAttributeValues = function () {
  var result = {}
  Object.keys(this._attributes).map(function (key) {
    var attr = this._attributes[key]
    var value = attr.Value
    if (value) {
      result[this._getValueAlias(key)] = value
    }
  }, this)
  return result
}

/**
 * @see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ExpressionPlaceholders.html#ExpressionAttributeValues
 */
UpdateExpressionBuilder.prototype._getValueAlias = function (key) {
  if (!this._uniqueName) throw new Error('Names have not been assigned yet')
  return ':V' + this._uniqueName + 'X' + key
}

/**
 * @return {string} String suitable for UpdateExpression
 * @see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html
 */
UpdateExpressionBuilder.prototype.buildExpression = function () {
  var keysByAction = {}
  Object.keys(this._attributes).map(function (key) {
    var attr = this._attributes[key]
    var action = getActionName(attr)
    if (!keysByAction[action]) {
      keysByAction[action] = []
    }
    keysByAction[action].push(key)
  }, this)

  var groups = []
  Object.keys(keysByAction).map(function (action) {
    var keys = keysByAction[action]
    groups.push(
      action + ' ' +
      keys.map(function (key) {
        var attrAlias = typeUtil.getAttributeAlias(key)
        var valueAlias = this._getValueAlias(key)
        if (action == 'REMOVE') {
          return attrAlias
        } else if (action == 'SET') {
          return attrAlias + ' = ' + valueAlias
        } else if (action == 'ADD' || action == 'DELETE') {
          return attrAlias + ' ' + valueAlias
        } else {
          throw new Error('Unrecognized action ' + action)
        }
      }, this).join(','))
  }, this)

  return groups.join(' ')
}

module.exports = UpdateExpressionBuilder

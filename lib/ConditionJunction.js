'use strict'

var typeUtil = require('./typeUtil')

/**
 * @param {string} operator
 * @param {Array<ConditionJunction|ConditionBuilder>} conditions
 * @constructor
 */
function ConditionJunction(operator, conditions) {
  if (operator != 'AND' && operator != 'OR' && operator != 'NOT') {
    throw new Error('Unknown operator ' + operator)
  }

  if (operator == 'NOT' && conditions.length != 1) {
    throw new Error('Exactly one condition expected for NOT: ' + conditions.length)

  }

  this._operator = operator
  this._conditions = conditions
}

/**
 * @param {Object} nameMutex
 */
ConditionJunction.prototype.assignUniqueNames = function (nameMutex) {
  this._conditions.map(function (condition) {
    condition.assignUniqueNames(nameMutex)
  }, this)
}

/** @return {Object} */
ConditionJunction.prototype.buildAttributeNames = function () {
  var result = {}
  this._conditions.map(function (condition) {
    typeUtil.extendAttributeNames(result, condition.buildAttributeNames())
  }, this)
  return result
}

/** @return {Object} */
ConditionJunction.prototype.buildAttributeValues = function () {
  var result = {}
  this._conditions.map(function (condition) {
    typeUtil.extendAttributeValues(result, condition.buildAttributeValues())
  }, this)
  return result
}

/** @return {string} */
ConditionJunction.prototype.buildExpression = function () {
  if (!this._conditions.length) {
    return ''
  }

  if (this._operator == 'NOT') {
    return '(NOT ' + this._conditions[0].buildExpression() + ')'
  }

  if (this._conditions.length == 1) {
    return this._conditions[0].buildExpression()
  }

  var exprs = this._conditions.map(function (condition) {
    return condition.buildExpression()
  }, this)
  return '(' + exprs.join(' ' + this._operator + ' ') + ')'
}

/**
 * Iterate through all the conditions
 * @param {function(Object, string)} callback
 */
ConditionJunction.prototype.forEachCondition = function (callback) {
  this._conditions.map(function (condition) {
    condition.forEachCondition(callback)
  })
}

/**
 * Get the appropriate comparator function to compare the passed in values against
 * @return {function(Object): boolean}
 */
ConditionJunction.prototype.buildFilterFn = function () {
  var filters = this._conditions.map(function (condition) {
    return condition.buildFilterFn()
  }, this)

  if (this._operator == 'AND') {
    return function (item) {
      return filters.every(function (filter) { return filter(item) })
    }
  } else if (this._operator == 'OR') {
    return function (item) {
      return filters.some(function (filter) { return filter(item) })
    }
  } else {
    if (this._operator != 'NOT') throw new Error('Unexpected operator ' + this._operator)
    return function (item) {
      return !filters[0](item)
    }
  }
}

module.exports = ConditionJunction

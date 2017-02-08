var assert = require('assert')
var typ = require('typ')
var typeUtil = require('./typeUtil')
var util = require('util')

/**
 * @constructor
 */
function ConditionBuilder() {
  this._exprs = []
}

/**
 * Creates a new Conditional expression with an operator (AND, EQ, etc)
 * @param {Op} op One of the ops defined in the Op enum.
 * @param {Array<ConditionExpr>} args The arguments to the op.
 * @return {ConditionExpr}
 */
ConditionBuilder.prototype._op = function (op, args) {
  if (!op) throw new Error('Missing op')
  return new ConditionExprOp(op, args)
}

/**
 * Creates a new Conditional expression that evaluates to an attribute name
 * @param {string} name An attribute name
 * @return {ConditionExpr}
 */
ConditionBuilder.prototype._attr = function (name) {
  return new ConditionExprAttr(name)
}

/**
 * Creates a new Conditional expression that evaluates to a value
 * @param {*} val Any literal value representable in a dynamodb expression
 * @return {ConditionExpr}
 */
ConditionBuilder.prototype._val = function (val) {
  return new ConditionExprVal(val)
}

/**
 * Returns this condition builder as a ConditionExpr, the internal representation
 * of a condition.
 * @return {ConditionExpr}
 */
ConditionBuilder.prototype._asExpr = function () {
  return this._op(Op.AND, this._exprs)
}

/**
 * @param {Array.<ConditionBuilder>} conditions
 * @return {ConditionBuilder}
 */
ConditionBuilder.andConditions = function (conditions) {
  ConditionBuilder.validateConditions(conditions)
  var builder = new ConditionBuilder()
  builder._exprs.push(builder._op(Op.AND, conditions.map(function (c) {
    return c._asExpr()
  })))
  return builder
}

/**
 * @param {Array.<ConditionBuilder>} conditions
 * @return {ConditionBuilder}
 */
ConditionBuilder.orConditions = function (conditions) {
  ConditionBuilder.validateConditions(conditions)
  var builder = new ConditionBuilder()
  builder._exprs.push(builder._op('OR', conditions.map(function (c) {
    return c._asExpr()
  })))
  return builder
}

/**
 * @param {ConditionBuilder} condition
 * @return {ConditionBuilder}
 */
ConditionBuilder.notCondition = function (condition) {
  var conditions = [condition]
  ConditionBuilder.validateConditions(conditions)
  var builder = new ConditionBuilder()
  builder._exprs.push(builder._op('NOT', [condition._asExpr()]))
  return builder
}

ConditionBuilder.validateConditions = function (conditions) {
  assert.ok(Array.isArray(conditions), 'Expected array')
  for (var i = 0; i < conditions.length; i++) {
      var condition = conditions[i]
    assert.ok(condition instanceof ConditionBuilder,
              'Expected ConditionBuilder')
  }
}

/**
 * @param {Object} data
 * @param {string} fieldName The fieldName on `data`
 * @param {Array<ConditionBuilder>} conditions
 * @param {{count: number}} nameMutex
 * @return {ConditionBuilder}
 */
ConditionBuilder.populateExpressionField = function (data, fieldName, conditions, nameMutex) {
  ConditionBuilder.validateConditions(conditions)

  var junction = ConditionBuilder.andConditions(conditions)
  junction.assignUniqueNames(nameMutex)

  if (!data.ExpressionAttributeNames) {
    data.ExpressionAttributeNames = {}
  }
  typeUtil.extendAttributeNames(data.ExpressionAttributeNames, junction.buildAttributeNames())

  if (!data.ExpressionAttributeValues) {
    data.ExpressionAttributeValues = {}
  }
  typeUtil.extendAttributeValues(data.ExpressionAttributeValues, junction.buildAttributeValues())

  data[fieldName] = junction.buildExpression()
  return junction
}

/**
 * @param {Object} nameMutex
 */
ConditionBuilder.prototype.assignUniqueNames = function (nameMutex) {
  this._asExpr().assignUniqueNames(nameMutex)
}

/** @return {Object} */
ConditionBuilder.prototype.buildAttributeNames = function () {
  var result = []
  this._asExpr().appendAttributeNames(result)
  return typeUtil.buildAttributeNames(result)
}

/** @return {Object} */
ConditionBuilder.prototype.buildAttributeValues = function () {
  var result = {}
  this._asExpr().appendAttributeValues(result)
  return result
}

/**
 * @return {string} String suitable for FilterExpression and KeyExpression
 * @see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
 */
ConditionBuilder.prototype.buildExpression = function () {
  return this._asExpr().buildExpression()
}

/**
 * Iterate through all the expressions. Intended for FakeDynamo
 * @param {function(Object)} callback
 */
ConditionBuilder.prototype.visitExpressionsPostOrder = function (callback) {
  this._exprs.forEach(function (expr) {
    expr.visitPostOrder(callback)
  })
}

/**
 * Get the appropriate comparator function to compare the passed in values against
 * @return {function(Object): boolean}
 */
ConditionBuilder.prototype.buildFilterFn = function () {
  var builder = this
  return function (item) {
    return !!builder._asExpr().evaluate(item)
  }
}

ConditionBuilder.prototype.expectAttributeEqual =
ConditionBuilder.prototype.expectAttributeEquals =
ConditionBuilder.prototype.filterAttributeEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('EQ', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeEqualsAttribute = function (key1, key2) {
  if (typ.isNullish(key1)) throw new Error("Key1 must be defined")
  if (typ.isNullish(key2)) throw new Error("Key2 must be defined")
  this._exprs.push(this._op('EQ', [this._attr(key1), this._attr(key2)]))
  return this
}

ConditionBuilder.prototype.filterAttributeNotEquals = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('NE', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeNotEqualsAttribute = function (key1, key2) {
  if (typ.isNullish(key1)) throw new Error("Key1 must be defined")
  if (typ.isNullish(key2)) throw new Error("Key2 must be defined")
  this._exprs.push(this._op('NE', [this._attr(key1), this._attr(key2)]))
  return this
}

ConditionBuilder.prototype.filterAttributeLessThanEqual = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('LE', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeLessThanEqualAttribute = function (key1, key2) {
  if (typ.isNullish(key1)) throw new Error("Key1 must be defined")
  if (typ.isNullish(key2)) throw new Error("Key2 must be defined")
  this._exprs.push(this._op('LE', [this._attr(key1), this._attr(key2)]))
  return this
}

ConditionBuilder.prototype.filterAttributeLessThan = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('LT', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeLessThanAttribute = function (key1, key2) {
  if (typ.isNullish(key1)) throw new Error("Key1 must be defined")
  if (typ.isNullish(key2)) throw new Error("Key2 must be defined")
  this._exprs.push(this._op('LT', [this._attr(key1), this._attr(key2)]))
  return this
}

ConditionBuilder.prototype.filterAttributeGreaterThanEqual = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('GE', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeGreaterThanEqualAttribute = function (key1, key2) {
  if (typ.isNullish(key1)) throw new Error("Key1 must be defined")
  if (typ.isNullish(key2)) throw new Error("Key2 must be defined")
  this._exprs.push(this._op('GE', [this._attr(key1), this._attr(key2)]))
  return this
}

ConditionBuilder.prototype.filterAttributeGreaterThan = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('GT', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeGreaterThanAttribute = function (key1, key2) {
  if (typ.isNullish(key1)) throw new Error("Key1 must be defined")
  if (typ.isNullish(key2)) throw new Error("Key2 must be defined")
  this._exprs.push(this._op('GT', [this._attr(key1), this._attr(key2)]))
  return this
}

ConditionBuilder.prototype.filterAttributeNotNull = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._exprs.push(this._op('NOT_NULL', [this._attr(key)]))
  return this
}

ConditionBuilder.prototype.expectAttributeAbsent =
ConditionBuilder.prototype.filterAttributeNull = function (key) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  this._exprs.push(this._op('NULL', [this._attr(key)]))
  return this
}

ConditionBuilder.prototype.filterAttributeContains = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('CONTAINS', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeNotContains = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('NOT_CONTAINS', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeBeginsWith = function (key, val) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val)) throw new Error("Val must be defined")
  this._exprs.push(this._op('BEGINS_WITH', [this._attr(key), this._val(val)]))
  return this
}

ConditionBuilder.prototype.filterAttributeBetween = function (key, val1, val2) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(val1)) throw new Error("Val 1 must be defined")
  if (typ.isNullish(val2)) throw new Error("Val 2 must be defined")
  this._exprs.push(this._op('BETWEEN', [this._attr(key), this._val(val1), this._val(val2)]))
  return this
}

ConditionBuilder.prototype.filterAttributeIn = function (key, vals) {
  if (typ.isNullish(key)) throw new Error("Key must be defined")
  if (typ.isNullish(vals)) throw new Error("Vals must be defined")
  this._exprs.push(this._op('IN', [this._attr(key)].concat(vals.map(this._val.bind(this)))))
  return this
}

/**
 * Represents an abstract conditional expression internally.
 *
 * May contain zero or more arguments, which are also ConditionExprs.
 *
 * @param {Array<ConditionExpr>} args
 * @constructor
 */
function ConditionExpr(args) {
  this.args = args || []
  this.uniqueName = ''
}

/**
 * Iterate through all the expressions
 * @param {function(Object)} callback
 */
ConditionExpr.prototype.visitPostOrder = function (callback) {
  this.args.forEach(function (arg) {
    arg.visitPostOrder(callback)
  })
  callback(this)
}

/**
 * @param {Object} nameMutex
 */
ConditionExpr.prototype.assignUniqueNames = function (nameMutex) {
  if (!nameMutex.count) {
    nameMutex.count = 1
  }
  this.visitPostOrder(function (expr) {
    expr.uniqueName = 'C' + nameMutex.count++
  })
}

/**
 * Appends the attribute names to the given array.
 */
ConditionExpr.prototype.appendAttributeNames = function (result) {
  this.args.forEach(function (arg) {
    arg.appendAttributeNames(result)
  })
}

/**
 * Appends the attribute values to the given object.
 */
ConditionExpr.prototype.appendAttributeValues = function (result) {
  this.args.forEach(function (arg) {
    arg.appendAttributeValues(result)
  })
}

/**
 * Builds an expression for consumption by the AWS expression API
 */
ConditionExpr.prototype.buildExpression = function () {
  throw new Error('Expected buildExpression impl')
}

/**
 * Client-side evaluation.
 */
ConditionExpr.prototype.evaluate = function () {
  throw new Error('Expected evaluate impl')
}

/**
 * Represents a conditional expression with 1 or more arguments.
 * @param {Op} op One of the ops defined in the Op enum.
 * @param {Array<ConditionExpr>} args The arguments to the op.
 * @constructor
 */
function ConditionExprOp(op, args) {
  ConditionExpr.call(this, args)
  this.op = op
}
util.inherits(ConditionExprOp, ConditionExpr)

/**
 * Builds an expression for arg i, asserting that the arg exists
 */
ConditionExprOp.prototype.buildArgExpr = function (i) {
  if (i >= this.args.length) throw new Error('Operator ' + this.op + ' expected arg at position ' + i)
  return this.args[i].buildExpression()
}

/** @override */
ConditionExprOp.prototype.buildExpression = function () {
  var operator = this.op
  switch (operator) {
    case Op.NOT:
      return '(NOT ' + this.buildArgExpr(0) + ')'
    case Op.AND:
      if (this.args.length == 0) return ''
      if (this.args.length == 1) return this.buildArgExpr(0)

      var andExprs = this.args.map(function (arg) {
        return arg.buildExpression()
      }, this)
      return '(' + andExprs.join(' AND ') + ')'
    case Op.OR:
      if (this.args.length == 0) return ''
      if (this.args.length == 1) return this.buildArgExpr(0)

      var orExprs = this.args.map(function (arg) {
        return arg.buildExpression()
      }, this)
      return '(' + orExprs.join(' OR ') + ')'
    case Op.BEGINS_WITH:
      return 'begins_with(' + this.buildArgExpr(0) + ', ' + this.buildArgExpr(1) + ')'
    case Op.EQ:
      return '(' + this.buildArgExpr(0) + ' = ' + this.buildArgExpr(1) + ')'
    case Op.NE:
      return '(' + this.buildArgExpr(0) + ' <> ' + this.buildArgExpr(1) + ')'
    case Op.LE:
      return '(' + this.buildArgExpr(0) + ' <= ' + this.buildArgExpr(1) + ')'
    case Op.LT:
      return '(' + this.buildArgExpr(0) + ' < ' + this.buildArgExpr(1) + ')'
    case Op.GE:
      return '(' + this.buildArgExpr(0) + ' >= ' + this.buildArgExpr(1) + ')'
    case Op.GT:
      return '(' + this.buildArgExpr(0) + ' > ' + this.buildArgExpr(1) + ')'
    case Op.BETWEEN:
      return '(' + this.buildArgExpr(0) + ' BETWEEN ' + this.buildArgExpr(1) + ' AND ' + this.buildArgExpr(2) + ')'
    case Op.IN:
      var values = this.args.slice(1).map(function (arg) {
        return arg.buildExpression()
      }, this)
      return '(' + this.buildArgExpr(0) + ' IN (' + values + '))'
    case Op.NOT_CONTAINS:
      return '(attribute_exists(' + this.buildArgExpr(0) + ') AND NOT contains(' + this.buildArgExpr(0) + ', ' + this.buildArgExpr(1) + '))'
    case Op.CONTAINS:
      return 'contains(' + this.buildArgExpr(0) + ', ' + this.buildArgExpr(1) + ')'
    case Op.NULL:
      return 'attribute_not_exists(' + this.buildArgExpr(0) + ')'
    case Op.NOT_NULL:
      return 'attribute_exists(' + this.buildArgExpr(0) + ')'
    default:
      throw new Error('Invalid comparison operator \'' + operator + '\'')
  }
}

/**
 * Evaluate the value for arg i, asserting that the arg exists
 */
ConditionExprOp.prototype.evalArg = function (i, item) {
  if (i >= this.args.length) throw new Error('Operator ' + this.op + ' expected arg at position ' + i)
  return this.args[i].evaluate(item)
}

/**
 * Evaluate the value for arg i, asserting that the arg exists
 */
ConditionExprOp.prototype.evalArgs = function (item) {
  return this.args.map(function (arg) {
    return arg.evaluate(item)
  })
}

/** @override */
ConditionExprOp.prototype.evaluate = function (item) {
  var argExprs = this.args
  var operator = this.op
  switch (operator) {
    case Op.NOT:
      return !this.evalArg(0, item)
    case Op.AND:
      return this.evalArgs(item).every(function (val) {
        return val
      })
    case Op.OR:
      return this.evalArgs(item).some(function (val) {
        return val
      })
    case Op.BEGINS_WITH:
      return item && this.evalArg(0, item).indexOf(this.evalArg(1, item)) === 0
    case Op.EQ:
      return item && this.evalArg(0, item) == this.evalArg(1, item)
    case Op.NE:
      return item && this.evalArg(0, item) != this.evalArg(1, item)
    case Op.LE:
      return item && this.evalArg(0, item) <= this.evalArg(1, item)
    case Op.LT:
      return item && this.evalArg(0, item) < this.evalArg(1, item)
    case Op.GE:
      return item && this.evalArg(0, item) >= this.evalArg(1, item)
    case Op.GT:
      return this.evalArg(0, item) > this.evalArg(1, item)
    case Op.BETWEEN:
      return item && this.evalArg(0, item) >= this.evalArg(1, item) && this.evalArg(0, item) <= this.evalArg(2, item)
    case Op.IN:
      return item && this.evalArgs(item).slice(1).indexOf(this.evalArg(0, item)) != -1
    case Op.NOT_CONTAINS:
      return item && this.evalArg(0, item).indexOf(this.evalArg(1, item)) == -1
    case Op.CONTAINS:
      return item && this.evalArg(0, item).indexOf(this.evalArg(1, item)) != -1
    case Op.NULL:
      return !item || !item.hasOwnProperty(argExprs[0].name)
    case Op.NOT_NULL:
      return item && item.hasOwnProperty(argExprs[0].name)
    default:
      throw new Error('Invalid comparison operator \'' + operator + '\'')
  }

}

/**
 * Represents a value in a conditional expression.
 * @param {*} val Any literal value representable in a dynamodb expression
 * @constructor
 */
function ConditionExprVal(val) {
  ConditionExpr.call(this, [])
  this.val = typeUtil.valueToObject(val)
}
util.inherits(ConditionExprVal, ConditionExpr)

/** @override */
ConditionExprVal.prototype.buildExpression = function () {
  return ':V' + this.uniqueName
}

/** @override */
ConditionExprVal.prototype.appendAttributeValues = function (result) {
  result[this.buildExpression()] = this.val
}

/** @override */
ConditionExprVal.prototype.evaluate = function () {
  return typeUtil.objectToValue(this.val)
}

/**
 * Represents an item attribute in a conditional expression.
 * @param {string} name An attribute name
 * @constructor
 */
function ConditionExprAttr(name) {
  ConditionExpr.call(this, [])
  this.name = name
}
util.inherits(ConditionExprAttr, ConditionExpr)

/** @override */
ConditionExprAttr.prototype.buildExpression = function () {
  return typeUtil.getAttributeAlias(this.name)
}

/** @override */
ConditionExprAttr.prototype.evaluate = function (item) {
  return item[this.name]
}

/** @override */
ConditionExprAttr.prototype.appendAttributeNames = function (result) {
  result.push(this.name)
}

/**
 * An enum of all the possible Ops in our ConditionExpr syntax tree.
 */
var Op = {
  NOT: 'NOT',
  AND: 'AND',
  OR: 'OR',
  BEGINS_WITH: 'BEGINS_WITH',
  EQ: 'EQ',
  NE: 'NE',
  LE: 'LE',
  LT: 'LT',
  GE: 'GE',
  GT: 'GT',
  BETWEEN: 'BETWEEN',
  IN: 'IN',
  NOT_CONTAINS: 'NOT_CONTAINS',
  CONTAINS: 'CONTAINS',
  NULL: 'NULL',
  NOT_NULL: 'NOT_NULL'
}

module.exports = ConditionBuilder

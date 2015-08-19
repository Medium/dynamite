// Copyright 2015 A Medium Corporation.

var typ = require('typ')
var typeUtil = require('./typeUtil')

/**
 * @param {Object} oldItem
 * @return {Object}
 */
function _cloneObject (oldItem) {
  var newItem = {}
  for (var key in oldItem) {
    if (oldItem.hasOwnProperty(key)) {
      newItem[key] = oldItem[key]
    }
  }

  return newItem
}

/**
 * @param {Object} item
 * @param {string} field
 * @param {Object} update
 */
function _processDeleteAction (item, field, update) {
  // From the dynamo docs:
  //
  // "If no value is specified, the attribute and its value are removed
  // from the item. The data type of the specified value must match the
  // existing value's data type.
  //
  // If a set of values is specified, then those values are subtracted
  // from the old set. For example, if the attribute value was the set
  // [a,b,c] and the DELETE action specified [a,c], then the final
  // attribute value would be [b]. Specifying an empty set is an error."
  if (typeUtil.objectIsEmpty(update.Value)) {
    // delete a field if it exists
    delete item[field]

  } else if (typeUtil.objectIsNonEmptySet(update.Value)) {
    // delete the items from the set if they exist
    item[field] = typeUtil.deleteFromSet(item[field], update.Value)
    if (!item[field]) delete item[field]

  } else {
    throw new Error('Trying to DELETE to a specified field from a non-set')
  }
}

/**
 * @param {Object} item
 * @param {string} field
 * @param {Object} update
 */
function _processPutAction (item, field, update) {
  // Attribute values cannot be null. String and Binary type attributes must have
  // lengths greater than zero. Set type attributes must not be empty. Requests
  // with empty values will be rejected with a ValidationException exception.

  if (!typeUtil.objectIsEmpty(update.Value)) {
    // set the value of a field
    item[field] = update.Value

  } else {
    throw new Error('Trying to PUT a field with an empty value')
  }
}

/**
 * @param {Object} item
 * @param {string} field
 * @param {Object} update
 */
function _processAddAction (item, field, update) {
  if (typeUtil.objectIsNonEmptySet(update.Value)) {
    // append to an array
    item[field] = typeUtil.addToSet(item[field], update.Value)

  } else if (typeUtil.objectToType(update.Value) == 'N') {
    // increment a number
    item[field] = typeUtil.addToNumber(item[field], update.Value)

  } else {
    throw new Error('Trying to ADD to a field which isnt an array or number')
  }
}

/**
 * @param {Object} oldItem
 * @param {Object} updates
 * @return {Object}
 */
function update (oldItem, updates) {
  if (!oldItem) {
    throw new Error('oldItem should not be falsy')
  }
  var newItem = _cloneObject(oldItem)

  for (var field in updates) {
    var update = updates[field]

    // See http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-AttributeUpdates

    if (update.Action === 'DELETE') {
      _processDeleteAction(newItem, field, update)
    } else if (update.Action === 'PUT') {
      _processPutAction(newItem, field, update)
    } else if (update.Action === 'ADD') {
      _processAddAction(newItem, field, update)
    }
  }

  return newItem
}

module.exports = {
  update: update
}

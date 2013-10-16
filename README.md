# Dynamite

Dynamite is a promise-based DynamoDB client. It was created to address performance issues in our previous DynamoDB client. Dynamite will almost always comply with the latest DynamoDB spec on Amazon.

Dynamite is based upon, and has an API compatibility with, an older in-house library called [Commutator](https://github.com/Obvious/commutator). Commutator details many of the underlying components in Dynamite and is thus a solid read to understand how Dynamite functions.

## Installation

    $ npm install dynamite


## Running Tests

Currently, Dynamite runs tests against the [Fake
Dynamo](https://github.com/ananthakumaran/fake_dynamo) Ruby gem. To install the gem:

	$ gem install fake_dynamo

To run the service, with an optional log level of `log` (show everything) on port 4567, run the following in a different terminal window or as a background process:

	$ fake_dynamo -l log -p 4567

Ensure that all of the required node modules are installed in the Dynamite directory by first running:

	$ npm install

The tests will be run against the `fake_dynamo` service running on port `4567`. Currently, there is no way to change the port without modifying the connection code in `test/utils/TestUtils.js`. To run the tests:

	$ npm test

## Creating a Client

	var Dynamite = require('dynamite')

	var options = {
		  region: 'us-east-1'
		, accessKeyId: 'xxx'
		, secretAccessKey: 'xxx'
		}

	var client = new Dynamite.Client(options)

Options requires all of:

* region
* accessKeyId
* secretAccessKey

If a `region` key is not provided in the `options` hash but a `endpoint` key is present, Dynamite will try to infer the region from the `host` key.

Options can also optionally take a hash with a key `dbClient` which points to an object that implements the AWS SDK interface for node.js.

#### Optional Options Keys

* `sslEnabled`: a boolean to turn ssl on or off for the connection.
* `endPoint`: the address of the DynamoDB instance to try to communicate with.
* `retryHandler`: a `function(method, table)` that will be triggered if Dynamite needs to retry a command.

### Foreword: Kew and You

All functions return [Kew](https://github.com/Obvious/kew) promises on `execute()`. These functions will all then take the form:

	client.fn(params)
		.execute()
		.then(function(){
			// handle success
		})
		.fail(function(e) {
			// handle failure
		})
		.fin(function() {
			// when all is said and done
		})

Therefore, these docs will focus more on function signatures and assume that the developer using those functions will comply with the Kew API in turn.

## Tables

### Creating a Table

Table creation is part of the database's concerns and thus doesn't have its own pretty API built into Dynamite. A snippet successfully creating a table that is compliant with the 2012 DynamoDB spec can be found in `test/utils/TestUtils.js`.


### Describing a Table

Tables can have descriptions. Retrieve them with:

	client.describeTable('table-name')

## Conditions

Conditions ensure that certain properties of the item are either absent or equal to a certain value before allowing whatever operation to which they were supplied to mutate the item. They become very useful when items should only be updated if they are missing a field or are of the wrong value. There currently exist two kinds of conditions: `expectAttributeEquals` and `expectAttributeAbsent`. Every operation has particular behaviors when conditions are or are not met.

Adding conditions to an operation is fairly trivial:

    var conditions = client.newConditionBuilder()
        .expectAttributeEquals('age', 29)

    client.fn('some-table')
        .withCondition(conditions)
        .execute()
        .then(function () {
            // handle the operation output
        })

There is also a helper method for building conditions from a JSON object.

    var conditions = client.conditions({age: 29})
    client.fn('some-table')
        .withCondition(conditions)
        .execute()

If a condition fails, the promise will be rejected with a conditional error,
which you can detect with the `isConditionalError` method

    client.fn('some-table')
        .withCondition(client.conditions({age: 29})
        .execute()
        .fail(function (e) {
          if (!client.isConditionalError(e)) {
            throw new Error('Unexpected age; conditional check failed')
          } else {
            throw e
          }
        })

Catching all conditional errors is a common idiom, so there is a 
`throwUnlessConditionalError` helper method for this case.

    client.fn('some-table')
        .withCondition(client.conditions({age: 29})
        .execute()
        .fail(client.throwUnlessConditionalError)

## Getting an Item From a Table

	client.getItem('user-table')
	    .setHashKey('userId', 'userA')
	    .setRangeKey('column', '@')
		.execute()
		.then(function(data) {
			// data.result: the resulting object
		})

If an item does not exist, `data.result` will be `undefined`.

### Getting Select Attributes

	client.getItem('user-table')
		.setHashKey('userId', 'userA')
	    .setRangeKey('column', '@')
	    .selectAttributes(['userId', 'column'])
		.execute()
		.then(function(data) {
			// data.result: the resulting object
			//              only the attributes passed into selectAttributes()
			//              appear as keys in data.result
		})



### Batch Get

The batch get API allows you to request multiple items with specific primary keys, from different
tables, in a single fetch.

    client.newBatchGetBuilder()
        .requestItems('user', [{'userId': 'userA', 'column': '@'}, {'userId': 'userB', 'column': '@'}])
        .requestItems('phones', [{'userId': 'userA', 'column': 'phone1'}, {'userId': 'userB', 'column': 'phone1'}])
        .execute()

`requestItems` can be called multiple times, with a table name and an array of objects representing
primary keys, in the form `{hashKey: 123, rangeKey: 456}`.



## Putting an Item Into a Table

Items are handled as JavaScript Objects by the client. These are then converted into an AWS specific format and sent off. The only accepted types of data that can be stored in DynamoDB are Strings, Numbers, and Sets (Arrays). Sets can contain either only Numbers or Strings.

	client.putItem('user-table', {
		userId: 'userA'
	  , column: '@'
	  , age: 30
	  , company: 'Medium'
	  , nickNames: ['Ev', 'Evan']
	  , postIds: [1, 2, 3]
	})

### Overrides

If an item with the same hash and range keys as the one that is being inserted, the old item will be replaced with the item that is being put in its place.

    // initialData = [{userId: 'userA', column: '@', age: 27]

    client.putItem('user-table', {
		userId: 'userA'
	  , column: '@'
	  , height: 72
	})

If the item above were to be retrieved from the table `user-table`, then age would be undefined and a new key `height` would be available.

### Conditional Writes

#### expectAttributeEquals

The item will only be replaced if the field `field` in the item is equal to the param `value`. If the item does not exist in the table, or the condition is not met, the request will fail.

#### expectAttributeAbsent

The item will only be replaced if the field `field` is not set in the item in the table. If the item does not exist in the table, then the item will be written to the table. If the field `field` exists for the item in the table, the request will fail.


## Deleting Items From a Table

If the hash key and range key match an item, it will be deleted. Upon success, the function returns the previous attributes and values of the deleted item.

    client.deleteItem('user-table')
        .setHashKey('userId', 'userA')
        .setRangeKey('column', '@')
        .execute()
        .then(function (data) {
            // data.result will contain the origin item attributes and their corresponding values
        })

### Conditional Deletes

#### expectAttributeEquals

If an item does not exist, then the request will fail. Otherwise, if the condition is met, the item will be deleted.

#### expectAttributeAbsent

If an item does not exist, then the request will fail. Otherwise, if the condition is met, the item will be deleted.


## Updating an Item

There are three methods available to modify columns for items: `putAttribute(field, value)`, `deleteAttribute(field)`, and `addToAttribute(field, value)`.

If an item does not exist, the update query will create the item and update its attributes accordingly.

If a value is updated on an attribute that does not exist, the attribute will be added to the item and set to the `value` passed to `putsAttribute(field, value)`. If an attribute does not exist and it's value is incremented, that attribute will be added to the item and it's value will be set to the `value` passed to `addToAttribute(field, value)`. If an attribute is deleted and it does not exist, the operation becomes a nonsense operation and has no effect on the item.

Putting empty attributes causes the whole update query to fail.

    // initialData = [{userId: 'userA', column: '@', age: 27, weight: 180]

    client.newUpdateBuilder('user-table')
        .setHashKey('userId', 'userA')
        .setRangeKey('column', '@')
        .enableUpsert()
        .putAttribute('age', 30)
        .addToAttribute('age', 1)
        .deleteAttribute('weight')
        .putAttribute('height', 72)
        .execute()
        .then(function (data) {
            // data.result == {userId: 'userA', column: '@', age: 31, height: 72}
        })

#### Conditional Updates

Conditions should be added with `withCondition` before any update commands.

##### expectAttributeEquals

If the item does not exist, the update query will fail.

##### expectAttributeAbsent

If the item does not exist, the update query will create the item and update its attributes accordingly.

## Querying a Table

Amazon features [extensive documentation](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html) describing querying and scanning in great detail.

A Query operation searches only primary key attribute values and supports a subset of comparison operators on key attribute values to refine the search process. A query returns all of the item data for the matching primary keys (all of each item's attributes) up to 1 MB of data per query operation. A Query operation always returns results, but can return empty results.

A Query operation seeks the specified composite primary key, or range of keys, until one of the following events occur:

+ The result set is exhausted.
+ The number of items retrieved reaches the value of the Limit parameter, if specified.
+ The amount of data retrieved reaches the maximum result set size limit of 1 MB.

### Usage

Our initial data set:

    [
      {"postId": "post1", "column": "@", "title": "This is my post", "content": "And here is some content!", "tags": ['foo', 'bar']},
      {"postId": "post1", "column": "/comment/timestamp/002123", "comment": "this is slightly later"},
      {"postId": "post1", "column": "/comment/timestamp/010000", "comment": "where am I?"},
      {"postId": "post1", "column": "/comment/timestamp/001111", "comment": "HEYYOOOOO"},
      {"postId": "post1", "column": "/comment/timestamp/001112", "comment": "what's up?"},
      {"postId": "post1", "column": "/canEdit/user/AAA", "userId": "AAA"}
    ]

Querying all items whose postId is `post1`:

    client.newQueryBuilder('comments-table')
        .setHashKey('postId', 'post1')
        .execute()
        .then(function (data) {
            // data.result is an array of posts whose hash key is `post1`
        })

There exist a variety of methods that refine and restrict the returned set of results that operate on the indexed range key, which in our sample case is `column`.

#### getCount()

Get the count of the number of items, not the actual items themselves.

    client.newQueryBuilder('comments-table')
        .setHashKey('postId', 'post1')
        .getCount()

#### scanForward()

Demand that items be returned in ascending ASCII or numerical value. This is the default.

#### scanBackward()

Demand that items be returned in descending ASCII or numerical value.

#### setStartKey(key)

Start the query at a specified hash key. Useful when your request is returned in chunks and subsequent chunks need to be retrieved after the current batch is processed.

When partial results are returned, the `LastEvaluatedKey` can be passed in as an argument to `setStartKey()` on the next query to get the next section of results.

#### setLimit(max)

Return at most `max` items. Note that if the response will be larger than 1mb, then at most 1mb of data is returned, and the next batch of items needs to be queried while specifying that the query start at the `LastEvaluatedKey`. That key is returned with the results of the current query.

#### indexBeginsWith(range_key, key_part)

Return only items where the range key begins with `key_part`. For instance, retrieve all comments for posts with a, in our case unique, hash key of `post1`.

    client.newQueryBuilder('comments-table')
        .setHashKey('postId', 'post1')
        .indexBeginsWith('column', '/comment/')

#### indexBetween(range_key, key_part_start, key_part_end)

Return only items whose range key is "between" the start and end keys. The range key will be compared to the start and end keys in a lexicographic manner. So 'b' is "between" 'a' and 'c'.

Retrieve all comments for posts with the hash key `post1` up until the `009999` timestamp:

    client.newQueryBuilder('comments-table')
        .setHashKey('postId', 'post1')
        .indexBetween('column', '/comment/', '/comment/timestamp/009999')

#### indexLessThan(range_key, value)
#### indexLessThanEqual(range_key, value)
#### indexGreaterThan(range_key, value)
#### indexGreaterThanEqual(range_key, value)
#### indexEqual(range_key, value)

Return all items whose range keys comply with the afore-listed operations.

#### selectAttributes(attributes[])

Returned items will be stripped of all attributes except their hash key, range key, and the provided array of strings `attributes`.

## Scanning A Table

Amazon features [extensive documentation](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html) describing querying and scanning in great detail.

A Scan operation examines every item in the table. You can specify filters to apply to the results to refine the values returned to you, after the scan has finished. Amazon DynamoDB puts a 1 MB limit on the scan (the limit applies before the results are filtered). A Scan can result in no table data meeting the filter criteria.

Scan supports a specific set of comparison operators. For information about each comparison operator available for scan operations, go to the API entry for Scan in the Amazon DynamoDB API Reference.

### Usage

Our initial data set:

    [
      {"userId": "c", "column": "@", "post": "3", "email": "1@medium.com"},
      {"userId": "b", "column": "@", "post": "0", "address": "800 Market St. SF, CA"},
      {"userId": "a", "column": "@", "post": "5", "email": "3@medium"},
      {"userId": "d", "column": "@", "post": "2", "twitter": "haha"},
      {"userId": "e", "column": "@", "post": "2", "twitter": "hoho"},
      {"userId": "f", "column": "@", "post": "4", "description": "Designer", "email": "h@w.com"},
      {"userId": "h", "column": "@", "post": "6", "tags": ['foo', 'bar']}
    ]

A simple scan looks like this:

    client.newScanBuilder('user-table')
        .execute()
        .then(function (data) {
            // data.result contains all of the users
        })

If your dataset contains more than 1 MB of data, the `data` that is returned will contain a `LastEvaluatedKey` key that will tell you what the last evaluated key for the scan was, so you can start the next `scan` there by passing the `LastEvaluatedKey` to `setStartKey(key)`.

#### .filterAttributeEquals(field, value)

Include items whose `field` equals `value`.

    client.newScanBuilder('user-table')
        .filterAttributeEquals('twitter', 'haha')
        .execute()
        .then(function (data) {
            // data.result #=> [{"userId": "d", "column": "@", "post": "2", "twitter": "haha"}]
        })

The other `filterAttribute*` functions are used in the exact same way.

#### .filterAttributeNotEquals(field, value)

Include items whose `field` does not equal `value`.

#### .filterAttributeLessThanEqual(field, value)

Include items whose `field` is less than or equal to `value`.

#### .filterAttributeLessThan(field, value)

Include items whose `field` is less than `value`.

#### .filterAttributeGreaterThanEqual(field, value)

Include items whose `field` is greater than or equal to `value`.

#### .filterAttributeGreaterThan(field, value)

Include items whose `field` is greater than `value`.

#### .filterAttributeNotNull(field)

Include items whose `field` is not `null`, or doesn't exist.

#### .filterAttributeContains(field, value)

Include items whose `field` contains `value`.

If an item's `field` attribute is a string, `filterAttributeContains` will search for `value` in that field's value. If an item's `field` attribute is a set, `filterAttributeContains` will search for `value` in that set.

#### .filterAttributeNotContains(field, value)

Include items whose `field` does not contain `value`. Essentially the inverse of `filterAttributeContains`.

#### .filterAttributeBeginsWith(field, value)

Include items whose `field` attribute begins with `value`.

#### .filterAttributeBetween(field, lower, upper)

Include items whose `field` attribute's value is between `lower` and `upper`, exclusive.

#### .filterAttributeIn(field, array_of_values)

Filter out rows where field is not one of the values in `array_of_values`.

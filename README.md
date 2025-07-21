# Yet another NoSQL/Key-Value-store library on top of SQLite for python

Yes

## How to install

This is the file: "pysqlitekv.py"

Just copy it directly to your code and import stuff from it

The async variant, "pysqlitekv_async.py", depends on "pysqlitekv.py" and the aiosqlite library

## API

Check out the "test_*.py" files for usage examples

### Data types

These are all the data types that are directly supported

- Booleans

- Integers (floats are converted directly)

- Strings

- Lists

- Hashmaps

- Any (any other object including custom classes and tuples)


### Standalone functions

The standalone functions are the corner stone of this library, even the async support depends on these


#### Init/connection functions

##### db_init()

```
db_init(filepath,confirm_only)
  returns bool, None or Connection
```

Takes a path to a file as an argument and initializes an SQL Table and returning (by default) a connection

The arg "confirm_only" (False by default), makes the function only return True if the table was created successfully in the requested filepath

##### db_getcon()

```
db_getcon(filepath)
  returns Connection
```

Takes a path to a file and returns a connection. It's just a fancy wrapper for sqlite3.connect()

##### db_getcur()

```
db_getcur(con_or_cur,begin_transaction)
  returns Cursor
```

Given a connection or a cursor, it returns a cursor

In case of giving it a cursor, the same cursor is returned

The arg "begin_transaction" (False by default) allows you to initiate a transaction on the new cursor


#### Main functions


##### db_post()

```
db_post(
    con_or_cur,
    key_name,
    value,
    page,
    force
  )
  returns bool
```

Writes a value to the database with a key name

If the key already exists, it will throw an error, unless "force" is used

##### db_get()

```
db_get(
    con_or_cur,
    key_name,
    page
  )
  returns None or Any
```

Pulls a value from a specific key from a database

##### db_delete()

```
db_delete(
    con_or_cur,
    key_name,
    page,
    return_val
  )
  returns bool, None or Any

```

Finds and deletes a value by its key

If "return_val" (False by default) is True, the now deleted value is returned

#### Lists

##### db_lpost()

```
db_lpost(
    con_or_cur,
    key_name,
    value,
    page,
    force
  )
  returns bool
```

Adds a value to an existing list

If the value itself is a list, the stored list is extended

If the stored value is not a list, you will need "force" to replace the value

##### db_lget()

```
db_lget(
    con_or_cur,
    key_name,
    target,
    page
  )
  returns None, list or Any
```

Pulls a specific index or slice from a list (using target)

Returns a list if it's a slice, returns Any or None if it's a specific index

##### db_ldelete()

```
db_ldelete(
    con_or_cur,
    key_name,
    target,
    page,
    return_val
  )
  returns bool
```

Deletes a specific index or slice from a list (using target, again)

By default it returns wether the targets were deleted (True) or not (False)

When using "return_val", the function returns all the deleted elements


#### Hashmaps

##### db_hupdate()

```
db_hupdate(
    con_or_cur,
    key_name,
    page,
    data_to_add,
    data_to_remove,
    return_val,
    force
  )
  returns bool or Mapping
```

Updates a key with a new dictionary to merge (data_to_add) and/or keys to delete (data_to_remove)

By default returns True if it managed to do everything right, False if it didn't

When "return_val" (False by default) is set to True, the removed values are returned

##### db_hget()

```
db_hget(
    con_or_cur,
    key_name,
    subkeys,
    aon,
  )
  returns Mapping
```

Given a key that SHOULD lead to a hashmap, and a list of subkeys, a Mapping based on that selection should be returned, otherwise an empty dict will

If the subkeys list is empty, you get NOTHING

The "aon" argument means "All Or Nothing", which means that if one of the keys in "subkeys" is not found, the function will return nothing

Check out the "test_hashmaps.py" file for more info

#### Other

##### db_custom()

```
db_custom(
    con_or_cur,
    key_name,
    custom_func,
    custom_func_params,
    res_write,
    res_return,
    page
  )
  returns bool, None or Any
```

Runs a custom function on a stored value

"custom_func" is the custom function

"custom_func_params" is the aditional argument for the custom function, it can be anything

"res_write" writes the result of the custom function to the keyname, replacing the original value. In case the cutom function returns None, the original value will not be replaced

"res_return" returns the result of the custom function

If "res_write" is True and "res_return" is False, db_custom() returns wether the stored value was modified or not

For more details, check out "test_customfun.py" and also check the "test_async.py"

##### db_len()

```
db_len(con_or_cur,key_name,page)
  returns int
```

Returns the length of the list or hashmap that correspond to that key

In case of failure, it returns -1

##### db_keys()

```
db_keys(con_or_cur,qtty,limit,page)
  returns List or Int
```

Returns all the keys in the database

The argument "qtty" (False by default) returns the ammount of keys instead of the list

The argument "limit", (0 by default) limits the ammount of results

##### db_fz_str()

```
db_fz_str(
    con_or_cur,
    substr,
    starts_with
  )
  returns list
```

Given a string, this function performs a fuzzy search in the database and returns a list of matches

If you use "starts_with", the stored strings must START WITH the given string

The list is sorted according to the quality of the results: the first element is gauranteed to be the best match possible

##### db_fz_num()

```
db_fz_num(
    con_or_cur,
    target,
    sort_results
  )
  returns list
```

Given a target (single number or range of numbers), find all keys that match

By default, results aren't sorted, so if you want sorted results, set "sort_results" to -1 (descending) or 1 (ascending)

#### Transaction functions

These functions take a cursor as a main argument, they allow you to begin, commit or rollback a transaction on that cursor

##### db_tx_begin()

```
db_tx_begin(cursor)

```

Begins transaction on a cursor

##### db_tx_commit()

```
db_tx_commit(cursor,close_cursor)
```

Commits changes to the database

If "close_cursor" is True, the cursor is also closed

##### db_tx_rollback()

```
db_tx_rollback(cursor,close_cursor)
```

Discards the transaction

If "close_cursor" (False by default) is True, the cursor is also closed

### Classes

See "test_classes.py" for usage example

#### DBControl

```
DBControl(filepath,setup)
```

On init this class takes a filepath as an argument, and creates its own connection

If "setup" is set to True, DBControl also sets up the table for the database

The class has access to all the standalone functions as methods and it has its own transaction methods

The transaction methods in this class work with a single cursor and transaction for this class only

This class can be used as a simple object or as a context manager


#### DBTransaction

```
DBTransaction(connection)
```

On init, this class takes an existing connection as an argument and creates its own cursor and transaction on top of it

This class has access to all standalone functions as methods, but no access to the transaction functions

#### DBReadOnly

```
DBReadOnly(filepath)
```

Grants read-only access to a database

Useful for heavy reading, there are no write methods

The db_custom() method for this class does now do write changes, but it does return the result of the custom function that is ran


#### The purpose of these classes

Managing connections and cursors by hand can be a pain in the ass, so the idea with these classes is that they both handle a single cursor with a single transaction per connection, so that YOU don't have to

WARNING: Don't mix contet managers and resources of these classes


### Important concepts

These are some important concepts that you need to know in order to use this library


#### Connections, cursors and transactions

Even though you don't need to know SQL to use this library, you at least need to know what these concepts mean. If you already know SQL/SQlite, you can skip this

A connection is a link between a client and a database. When you make write changes through a connection directly in SQLite, these changes are committed immediately by default. If you want to read data from a connection or work on data with more care, you use a cursor

A cursor depends on an existing connection, it allows you to read information and also create a transaction. When you make changes through a cursor, changes can be committed right away unless you begin a transaction, include more operations and commit

A transaction is a set of write operations made by a cursor, you initiate at one point and then end it by committing on the cursor. In case something goes wrong, you can ditch a transaction and cancel all pending changes in order to not leave the desired set of data in an incomplete state


#### The "con_or_cur" argument

All of the main functions use "con_or_cur". This argument is short for "Connection or Cursor"

In functions and methods that write to the database, like as db_post() and db_delete(), they accept either a connection or a cursor

When you give them a connection, a local cursor is created inside that function so that the changes are all committed at once

When you use a cursor though, changes are not committed, so you have to use them after creating a transaction on that cursor

If you take a look at DBControl, it handles the "con_or_cur" argument directly underneath, depending on wether you initiated a cursor or not using its internal version of db_tx_begin(). DBTransaction only works with an already established connection to create its cursor. 


#### The selector argument called "target"

The functions db_lget(), db_ldelete() and db_mnum() have an argument called "target", this argument can be used as a tuple or an integer

When used as an integer, it can be any int value. For lists, negative numbers select from the end

When used as a tuple, the tuple must meet several requirements:

```
( X:Optional[int] , Y:Optional[int] )

X or Y can be None, X and Y cannot be None

if X and Y are integers, X must be smaller than Y

In the specific case of list indexes, these ingeters CANNOT be negative
```

These are some valid target tuples:

- (3,None) Selects 3 and larger than 3

- (None,9) Selects 9 and below 9

- (-42,69) Selects all values between -42 and 69, including 42 and 69

- (0, 60) Selects all values between 0 and 60, including 0 and 60

In the files "test_lists.py" and "test_matching.py" you can see some example code of how the target tuple is used


#### The "page" argument

By default a single table is used to store all the data. Need to have different data without using an extra file? Check out the "test_pages.py" file to see how to create additional pages on a single file

All main functions and methods (db_post(), db_lget(), db_hupdate(), etc...) have access to the "page" argument

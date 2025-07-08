# Yet another NoSQL/Key-Value-store library on top of SQLite for python

It doesn't have a name


## How to install

This is the file: "a_key_value_store_on_top_of_sqlite.py"

Just copy it directly to your code and rename it to something smaller like "db.py"


## API

Check out the "test_*.py" files for usage examples

### Standalone functions

The standalone functions is what make this library GREAT


#### Initializaion functions


db_init(filepath,confirm_only) returns bool, None or Connection

- Takes a path to a file as an argument and initializes an SQL Table and returning (by default) a connection

- The arg "confirm_only" (False by default), makes the function only return True if the table was created successfully in the requested filepath


db_getcon(filepath) returns Connection

- Takes a path to a file and returns a connection. It's just a fancy wrapper for sqlite3.connect()


db_getcur(con_or_cur,begin_transaction) returns Cursor

- Given a connection or a cursor, it returns a cursor

- In case of giving it a cursor, the same cursor is returned

- The arg "begin_transaction" (False by default) allows you to initiate a transaction on the new cursor


#### Main functions


##### Basic


db_post(con_or_cur,key_name,value,force) returns bool

- Writes a value to the database with a key name

- If the key already exists, it will throw an error, unless "force" is used


db_get(con_or_cur,key_name) returns None or Any

- Pulls a value from a specific key from a database


db_delete(con_or_cur,key_name,return_val) returns bool, None or Any

- Finds and deletes a value by its key

- If "return_val" (False by default) is True, the now deleted value is returned


##### Lists


db_lpost(con_or_cur,key_name,value,force) returns bool

- Adds a value to an existing list

- If the value itself is a list, the stored list is extended

- If the stored value is not a list, you will need "force" to replace the value


db_lget(con_or_cur,key_name,target) returns None, list or Any

- Pulls a specific index or slice from a list (using target)

- Returns a list if it's a slice, returns Any or None if it's a specific index


db_ldelete(con_or_cur,key_name,target,return_val) returns bool

- Deletes a specific index or slice from a list (using target, again)

- By default it returns wether the targets were deleted (True) or not (False)

- When using "return_val", the function returns all the deleted elements


##### Hashmaps


db_hupdate(con_or_cur,key_name,data_to_add,data_to_remove,return_val,force) returns bool or Mapping

- Updates a key with a new dictionary to merge (data_to_add) and/or keys to delete (data_to_remove)

- By default returns True if it managed to do everything right, False if it didn't

- When "return_val" (False by default) is set to True, the removed values are returned


db_hget(con_or_cur,key_name,subkeys) returns Mapping

- Given a key that SHOULD lead to a hashmap, and a list of subkeys, a Mapping based on that selection should be returned, otherwise an empty dict will

- If the subkeys list is empty, you get NOTHING


##### Others (read only)


db_len(con_or_cur,key_name) returns int

- By deault, it returns the ammount of items in the database

- If "key_name" (None by default) is provided, it will (attempt to) return the length of the list or hashmap that correspond to that key


db_fz_str(con_or_cur,substr,starts_with) returns list

- Given a string, this function performs a fuzzy search in the database and returns a list of matches

- If you use "starts_with", the stored strings must START WITH the given string

- The list is sorted according to the quality of the results: the first element is gauranteed to be the best match possible


db_fz_num(con_or_cur,target,sort_results) returns list

- Given a target (single number or range of numbers), find all keys that match

- By default, results aren't sorted, so if you want sorted results, set "sort_results" to -1 (descending) or 1 (ascending)


#### Transaction functions

These functions take a cursor as a main argument, they allow you to begin, commit or rollback a transaction on that cursor


db_tx_begin(cursor)

- Begins transaction on a cursor


db_tx_commit(cursor,close_cursor)

- Commits changes to the database

- If "close_cursor" is True, the cursor is also closed


db_tx_rollback(cursor,close_cursor)

- Trashes the transaction

- If "close_cursor" is True, the cursor is also closed


### Classes

See "test_classes.py" for usage example


#### DBControl

DBControl(filepath,setup)

On init this class takes a filepath as an argument, and creates its own connection

If "setup" is set to True, DBControl also sets up the table for the database

The class has access to all the standalone functions as methods and it has its own transaction methods

The transaction methods in this class work with a single cursor and transaction for this class only

This class can be used as a simple object or as a context manager


#### DBTransaction

DBTransaction(connection)

On init, this class takes an existing connection as an argument and creates its own cursor and transaction on top of it

This class has access to all standalone functions as methods, but no access to the transaction functions


#### The purpose of these classes

Managing connections and cursors by hand can be a pain in the ass, so the idea with these classes is that they both handle a single cursor with a single transaction per connection, so that YOU don't blow your brains out

A warning: don't use DBTransaction inside DBControl's context manager with DBControl's own connection. You have been warned, don't come back to me crying


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

- Form of ( X , Y ). In other words, length of 2

- X or Y must be integers or None. Both of them CANNOT be None

- In the case X or Y are integers, they have to be larger than or equal to zero

- In case X and Y are both integers, X must be smaller than Y, because X is the min and Y is the max

These are some valid target tuples:

- (3,None)

- (None,9)

- (42,69)

- (0, 60)

In the files "test_lists.py" and "test_matching.py" you can see some example code of how the target tuple is used

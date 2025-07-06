# A Key Value store on top of SQLite

## How to install

This is the file: "a_key_value_store_on_top_of_sqlite.py"

Just copy the library to your code, no need to go through pip

## How to use

The "test_*.py" files have usage examples

### This is what you will see on each example

- "test_datatypes.py" → Data types and a sort of "introduction" to the library

- "test_lists.py" → How to store, update, read and delete lists precisely + how to do DBTransactions

- "test_hashmaps" → How to store, update, read and delete dictionaries precisely

- "test_fmatch.py" → How to do a fuzzy search

- "test_classes.py" → The DBControl and DBTransaction classes, the less verbose way to deal with this library

### These are some facts you will notice

- There are like 3 different ways to use this library

- Both DBControl and DBDBTransaction classes depend on the standalone functions. DBTransaction uses SQLConnection directly, and DBControl uses a path to the DB file

- The standalone functions take as an argument either an SQLite connection or an SQLite cursor

- Standalone functions that use SQLite connections such as db_post(),db_lpost(),etc... are all isolated (a temporary cursor is created within them), whereas those that use a cursor, are funcctions that are part of a transaction

#!/usr/bin/python3.9

from pysqlitekv import (
	DBControl,
	DBTransaction,
	DBReadOnly,
	db_getcon,db_get,db_keys
)

if __name__=="__main__":

	from pathlib import Path

	fpath=Path("example_classes.db")
	if fpath.is_file():
		fpath.unlink()

	# The DBControl class

	print("\nDBControl with context manager")

	# DBControl opens up its own connection from the file
	# and it opens up its own cursor when using its own
	# transaction functions. When the context manager dies,
	# it closes everything related to the connection

	with DBControl(fpath,setup=True,cfg_verbose=True) as dbc:
		dbc.db_post("key0","hello")

		if dbc.db_tx_begin():
			dbc.db_post("key1",[1,2,3,4,5])
			dbc.db_lpost("key1",["66",7,88])

	print("\nDBControl WITHOUT the context manager")

	# DBControl can also be used without a context manager, but
	# in this case, you have to use .close() to do the cleanup

	mydb=DBControl(fpath,cfg_verbose=True)
	

	another_key="Another Key"
	mydb.db_hupdate(
		another_key,
		{"eggs":69}
	)

	this_is_fine=False
	# this_is_fine=True

	if mydb.db_tx_begin():

		mydb.db_hupdate(
			another_key,
			{"mustard":11}
		)
		mydb.db_hupdate(
			another_key,
			{"cheese":6}
		)

		if not this_is_fine:
			mydb.db_tx_commit()
		if this_is_fine:
			mydb.db_tx_rollback()

	print(
		f"contents of {another_key}:",
		mydb.db_get(another_key)
	)

	mydb.close()

	# DBTransaction

	# The DBTransaction class takes an aready existing connection and creates a cursor
	# and a transaction in its context manager

	# It is recommended to use the standalone versions of db_init() and db_getcon()
	# to get a connection

	print("\nDBTransaction class (context manager only)")

	k="key111"
	new_con=db_getcon(fpath)

	try:
		with DBTransaction(new_con,cfg_verbose=True) as tx:
			tx.db_post(k,{"person":True})

		db_get(new_con,k,display_results=True)

		with DBTransaction(
				new_con,
				cfg_verbose=True
			) as tx:
	
			tx.db_hupdate(
				k,
				new={"name":"Mike"},
				remove=["person"]
			)
			# assert False
			tx.db_hupdate(
				k,
				new={"age":45,"bald":"Yes"}
			)

	except Exception as exc:
		print(exc)

	db_get(new_con,k,display_results=True)
	new_con.close()

	print("\nDBReadOnly")

	with DBReadOnly(fpath,cfg_verbose=True) as dbro:

		keys=dbro.db_keys()
		print("keys in this DB:",keys)

		print("key0 =",dbro.db_get("key0"))

	# VERY IMPORTANT NOTE:

	# DO NOT MIX CURSORS AND CONNECTIONS FROM DIFFERENT CLASSES TOGETHER
	# unless you know exactly what you are doing, because all classes are meant to be
	# used in different scenarios:

	# DBControl → Creates a connection from scratch and its own cursors and transactions
	# Recommended for looking around stuff in the database

	# DBTransaction → Creates a cursor + a transaction from existing connection
	# Recommended for precise writing on the database

	# DBReadOnly → Creates a connection from a file and its own cursor
	# Recommended if all you're gonna do is read heavily (because it has no write methods)
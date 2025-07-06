#!/usr/bin/python3.9

from a_key_value_store_on_top_of_sqlite import (
	DBControl,
	DBTransaction,
	db_post,db_hupdate
)

if __name__=="__main__":

	from pathlib import Path

	fpath=Path("ftest_classes.db")
	if fpath.exists():
		fpath.unlink()

	# NOTE:
	# DBControl does not have a context manager, but it works fine

	dbc=DBControl(fpath,setup=True,cfg_verbose=True)

	if dbc.db_tx_begin():
		dbc.db_post("key1",[1,2,3,4,5])
		dbc.db_lpost("key1",["66",7,88])
		dbc.db_tx_commit()

	print(
		dbc.db_get("key1")
	)

	# NOTE:
	# The transaction context manager uses an SQLConnection directly
	# as an argument and it also uses its own cursor

	k="key111"

	dbc.db_post(k,{})

	try:
		with DBTransaction(
				dbc.con,
				cfg_verbose=True
			) as tx:
	
			tx.db_hupdate(k,{"name":"Mike"})
			assert False
			tx.db_hupdate(k,{"age":45})
			print("→",tx.db_get(k))
	except Exception as exc:
		print(exc)

	print(dbc.db_get(k))

	# ↓ I HOPE SOMEONE READS THIS

	dbc.con.close()

	# ↑ THE CONNECTION CLOSES HERE

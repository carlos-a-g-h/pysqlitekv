#!/usr/bin/python3.9

# from a_key_value_store_on_top_of_sqlite import (
from yanosqlkvslibsqlite import (
	db_init,
	db_get,db_post,db_hget,db_hupdate,
	db_getcur,
	db_tx_begin,db_tx_commit,
)

if __name__=="__main__":

	from pathlib import Path

	key="key-01"

	fpath=Path("example_hashmaps.db")
	if fpath.is_file():
		fpath.unlink()

	con=db_init(fpath)

	# NOTE: this cursor is being returned with a transaction initialized
	cur=db_getcur(con,begin_transaction=True)

	# Hashmaps are all updated using dictionaries
	db_post(cur,key,{"name":"Mike"})
	db_hupdate(cur,key,{"age":69})

	# NOTE: the cursor is still open after committing the changes
	db_tx_commit(cur)

	# NOTE: this is a new transaction on the same cursor
	db_tx_begin(cur)

	# You can pull any key you want from a hashmap
	db_hget(cur,key,subkeys=["name"],display_results=True)

	# When you request data to be added and removed, the data is first removed and then added/merged
	db_hupdate(cur,key,data_to_add={"unknown":True},data_to_remove=["name","age"])

	# NOTE: this commit requests closing the cursor
	db_tx_commit(cur,close_cursor=True)

	db_get(con,key,display_results=True)

	con.close()

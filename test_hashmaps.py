#!/usr/bin/python3.9

from a_key_value_store_on_top_of_sqlite import (
	db_init,
	db_get,db_post,db_hget,db_hupdate,
	db_getcur,
	db_tx_begin,db_tx_commit,
)

if __name__=="__main__":

	from pathlib import Path

	key="key-01"

	con=db_init(Path("ftest_hashmaps.db"))

	cur=db_getcur(con,begin_transaction=True)
	db_post(cur,key,{"name":"Mike"})
	db_hupdate(cur,key,{"age":69})
	db_tx_commit(cur)

	db_tx_begin(cur)
	db_get(cur,key,print_instead=True)
	print("Name:",db_hget(cur,key,["name"]))
	db_hupdate(cur,key,data_to_add={"unknown":True},data_to_remove=["name","age"])
	db_tx_commit(cur,close_cursor=True)

	db_get(con,key,print_instead=True)

	con.close()

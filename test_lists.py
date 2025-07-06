#!/usr/bin/python3.9

from a_key_value_store_on_top_of_sqlite import (
	db_init,
	db_getcur,db_tx_commit,
	db_post,db_get,db_lpost,db_lget,db_ldelete
)

if __name__=="__main__":

	from pathlib import Path

	fpath=Path("ftest_lists.db")

	con=db_init(fpath)

	keyname="test_list1"
	cur=db_getcur(con,begin_transaction=True)
	db_post(cur,keyname,[0,1,"2"],verbose=True)
	db_lpost(cur,keyname,[3,"40",56,77],verbose=True)
	db_tx_commit(cur,close_cursor=True)

	db_get(con,keyname,display_results=True)

	db_lget(con,keyname,(3,None),display_results=True)
	db_lget(con,keyname,(None,5),display_results=True)

	deleted=db_ldelete(con,keyname,(3,5),return_values=True)
	print("Deleted values = ",deleted)

	db_get(con,keyname,display_results=True)

	con.close()

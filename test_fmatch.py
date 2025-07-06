#!/usr/bin/python3.9

from a_key_value_store_on_top_of_sqlite import (
	db_init,
	db_getcur,db_tx_commit,
	db_post,db_fmstr,
)

if __name__=="__main__":

	from pathlib import Path

	fpath=Path("ftest_fuzzymatching.db")

	con=db_init(fpath)

	cur=db_getcur(con,begin_transaction=True)
	db_post(cur,"k01","The Catcher in the rye")
	db_post(cur,"k02","Fritz, the cat")
	db_post(cur,"k03","Kitties")
	db_post(cur,"k04","The Cat")
	db_post(cur,"k05","the cat")
	db_post(cur,"k06","The cat in the hat")
	db_post(cur,"k07","MEOWWWWW")
	db_tx_commit(cur,close_cursor=True)

	# NOTE: results are organized depending on their match level

	db_fmstr(con,"cat",display_results=True)

	db_fmstr(con,"The Cat",display_results=True,verbose=True)

	db_fmstr(con,"the cat",display_results=True,verbose=True)

	con.close()

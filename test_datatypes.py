#!/usr/bin/python3.9

from a_key_value_store_on_top_of_sqlite import (
	db_init,
	db_post,db_lpost,db_hupdate,
	db_get,
)

from pathlib import Path

if __name__=="__main__":


	fpath=Path("ftest_classes.db")
	if fpath.is_file():
		fpath.unlink()

	sqlcon=db_init(fpath)

	key="test-key-1"

	db_post(sqlcon,key,"this is a test",verbose=True)
	db_get(sqlcon,key,display_results=True)

	# This one will failed because the key is already occupied
	try:
		db_post(sqlcon,key,69,verbose=True)
	except Exception as exc:
		print(db_post.__name__,exc)
	db_get(sqlcon,key,display_results=True)

	# THis one will work
	db_post(sqlcon,key,69,force=True,verbose=True)
	db_get(sqlcon,key,display_results=True)

	# This changes the data type to a list
	db_lpost(sqlcon,key,[0,1,2],force=True)
	db_get(sqlcon,key,display_results=True)


	# THis changes the data type to a hashmap
	db_post(sqlcon,key,{"nice":True},force=True)
	db_get(sqlcon,key,display_results=True)
	db_hupdate(sqlcon,key,data_to_add={"name":"Charles","age":30},data_to_remove=["nice"])
	db_get(sqlcon,key,display_results=True)

	sqlcon.close()

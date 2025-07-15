#!/usr/bin/python3.9

from pysqlitekv import (
	db_init,
	db_post,db_lpost,db_hupdate,
	db_get,db_delete,db_keys,
	_TYPE_STRING
)

from pathlib import Path

if __name__=="__main__":

	fpath=Path("example_datatypes.db")
	if fpath.is_file():
		fpath.unlink()

	sqlcon=db_init(fpath)


	key="this-is-a-bool"
	db_post(sqlcon,key,True,verbose=True)
	db_get(sqlcon,key,verbose=True,display_results=True)

	key="test-key-1"

	db_post(sqlcon,key,"this is a test",verbose=True)
	db_get(sqlcon,key,display_results=True)

	# Check if it's a string
	if db_get(
		sqlcon,key,
		verbose=True,
		get_type_only=True
	)==_TYPE_STRING:
		print("It's a string!")

	# This one will failed because the key is already occupied
	try:
		db_post(sqlcon,"seventy",70,verbose=True)
		db_post(sqlcon,"seventy One",71,verbose=True)
		db_post(sqlcon,"six nine",69,verbose=True)
		# db_post(sqlcon,key,69,verbose=True)
	except Exception as exc:
		print(db_post.__name__,exc)

	db_get(sqlcon,key,display_results=True)
	# THis one will work
	db_post(sqlcon,key,69,force=True,verbose=True)
	# Check data type (this one will fail)
	ok=(
		db_get(
			sqlcon,key,
			verbose=True,
			get_type_only=True
		)==_TYPE_STRING
	)
	print(
		"is it a string?",
		{True:"YEs",False:"Nooo"}[ok]
	)

	db_get(sqlcon,key,display_results=True)

	# This changes the data type to a list
	db_lpost(sqlcon,key,[0,1,2],force=True)
	db_get(sqlcon,key,display_results=True)


	# THis changes the data type to a hashmap
	db_post(sqlcon,key,{"nice":True},force=True)
	db_get(sqlcon,key,display_results=True)
	db_hupdate(sqlcon,key,data_to_add={"name":"Charles","age":30},data_to_remove=["nice"])
	db_get(sqlcon,key,display_results=True)

	deleted=db_delete(sqlcon,key,return_val=True)
	print("DELETED:",deleted)

	# Listing keys
	db_keys(sqlcon,display_results=True)
	db_keys(sqlcon,limit=2,display_results=True)

	sqlcon.close()

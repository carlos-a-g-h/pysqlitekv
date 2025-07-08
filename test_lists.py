#!/usr/bin/python3.9

from a_key_value_store_on_top_of_sqlite import (
	db_init,
	db_getcur,db_tx_commit,
	db_post,db_get,db_lpost,db_lget,db_ldelete
)

if __name__=="__main__":

	from pathlib import Path

	fpath=Path("example_lists.db")
	if fpath.is_file():
		fpath.unlink()

	con=db_init(fpath)

	keyname="test_list1"

	print("\nAdding values")

	# we first open up a cursor
	cur=db_getcur(con,begin_transaction=True)

	# By posting a list, the datatype changes to a list
	db_post(cur,keyname,[0,1,"2"],verbose=True)

	# Lists can only be updated using db_lpost()
	# Any value that is not a list is appended to the end
	db_lpost(cur,keyname,3,verbose=True)

	# In this case, thjis list is not added as a single element, it is used to
	# extend the already existing list
	db_lpost(cur,keyname,["40",56,77],verbose=True)

	# Changes are commited to the DB
	db_tx_commit(cur,close_cursor=True)

	db_get(con,keyname,display_results=True)

	print("\nReading and deleting")

	# To read from a list, you use db_lget(), like this:
	db_lget(con,keyname,4,display_results=True)

	# What if you want to read a specific portion of the list? you use a tuple, like this:
	db_lget(con,keyname,(3,None),display_results=True)

	# NOTE: This is how the selector works when it's a tuple
	# target:tuple = ( X:Optional[int] , Y:Optional[int] );
	# Where X and Y are either None or Int ( ==0 OR >0 )
	# Both X and Y cannot be None, only one of them can

	# Given the previous definition, In this case X is None, and Y is 5
	# The last 5 elements of the list are selected
	db_lget(con,keyname,(None,5),display_results=True)

	# This is how you delete from the list
	deleted=db_ldelete(con,keyname,(3,5),return_val=True)
	print("Deleted values = ",deleted)

	# This is how the full list looks like
	db_get(con,keyname,display_results=True)

	con.close()

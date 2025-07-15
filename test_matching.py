#!/usr/bin/python3.9

from pysqlitekv import (
	db_init,
	db_getcur,db_tx_commit,
	db_post,db_fz_str,db_fz_num,_SORT_HI_TO_LOW
)

if __name__=="__main__":

	from pathlib import Path

	fpath=Path("example_matching.db")
	if fpath.is_file():
		fpath.unlink()

	con=db_init(fpath)

	# The function db_mstr() is for fuzzy string matching
	# Returns a list with the results

	print("\nString matching")

	cur=db_getcur(con,begin_transaction=True)
	db_post(cur,"k01","The Catcher in the rye")
	db_post(cur,"k02","Fritz, the cat")
	db_post(cur,"k03","Kitties")
	db_post(cur,"k04","The Cat")
	db_post(cur,"k05","the cat")
	db_post(cur,"k06","The cat in the hat")
	db_post(cur,"k07","MEOWWWWW")
	db_post(cur,"k08","say meoWWW")
	db_tx_commit(cur,close_cursor=True)

	# NOTE:
	# If you take a look at the output, you will notice that the
	# results are organized depending on the match
	db_fz_str(con,"cat",display_results=True)
	db_fz_str(con,"The Cat",display_results=True)
	db_fz_str(con,"the cat",display_results=True)

	# By using startswith, the search is based on wether the string starts with the given substring
	db_fz_str(con,"OW",starts_with=True,display_results=True)
	db_fz_str(con,"meow",starts_with=True,display_results=True)

	print("\nRanged and single number matching")

	# The function db_mnum() allows you to do a ranged match or single number match
	# Returns a list with the results

	# NOTE: You select the same way you do on db_lget() and db_ldelete()

	db_post(con,"bananas",9)
	db_post(con,"pickles",17)
	db_post(con,"potatoes",15)
	db_post(con,"onions",14)
	db_post(con,"eggs",14)
	db_post(con,"apples",10)
	db_post(con,"oranges",11)
	db_post(con,"tomatoes",13)

	db_fz_num(
		con,
		14,
		display_results=True
	)

	# By default, the results aren't sorted, but in this case
	# they are being sorted from the Highest to the lowest
	db_fz_num(
		con,
		(11,15),
		sort_results=_SORT_HI_TO_LOW,
		display_results=True
	)

	con.close()

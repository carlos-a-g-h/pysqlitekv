#!/usr/bin/python3.9


from pathlib import Path
from typing import Union

from yanosqlkvslibsqlite import DBControl,db_check_changes

def last_person_in_line_is_now_first(the_list:list):

	line=[the_list[-1]]

	size=len(the_list)-1
	idx=-1

	while True:
		idx=idx+1
		line.append(
			the_list[idx]
		)
		if idx==size-1:
			break

	return line

def do_some_math(
		og_val:int,
		myargs:Union[tuple,list]
	)->int:

	x,y=myargs
	res=(og_val*(x+y))
	print(
		f"{og_val} * ( {x} + {y} ) = {res}"
	)

	return res

if __name__=="__main__":

	# Custom function example

	fpath=Path("example_customfun.db")
	if fpath.is_file():
		fpath.unlink()

	kn_num="test-key-69"
	kn_list="People_in_line"

	with DBControl(
			fpath,setup=True,
			cfg_verbose=True
		) as dbctl:

		print("\nSimple custom function")

		# This one only takes the stored value as the only argument, it does not have extra parameters

		dbctl.db_post(
			kn_list,
			[
				"first",
				"second",
				"third",
				"4th",5,
				"last"
			]
		)
		print(
			f"{kn_list} =",
			dbctl.db_get(kn_list)
		)
		dbctl.db_custom(
			kn_list,
			last_person_in_line_is_now_first,
			res_write=True
		)

		db_check_changes(dbctl.cur,True)

		print(
			"The last one is now the first one:",
			dbctl.db_get(kn_list)
		)

		print("\nMulti param custom function")

		# This one does have extra params in the form of a tuple

		dbctl.db_post(kn_num,50)
		print(
			f"{kn_num} =",
			dbctl.db_get(kn_num)
		)

		# Does some math, but it doesn't save the changes
		dbctl.db_custom(
			kn_num,
			custom_func=do_some_math,
			custom_func_params=(5,6),
		)
		print(
			f"{kn_num} =",
			dbctl.db_get(kn_num)
		)

		# This one does write the changes
		dbctl.db_custom(
			kn_num,
			do_some_math,
			custom_func_params=(4,20),
			res_write=True
		)
		print(
			f"{kn_num} =",
			dbctl.db_get(kn_num)
		)


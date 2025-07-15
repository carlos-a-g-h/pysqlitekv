#!/usr/bin/python3.9

from pysqlitekv import DBControl

if __name__=="__main__":

	# Using pages, you can group data into different tables using a single file
	# By default, when setting up the database the default page is 0,
	# but you can set a custom page or a number of different custom pages using a list of integers

	from pathlib import Path

	fpath=Path("example_pages.db")
	if fpath.is_file():
		fpath.unlink()

	page_livingroom=23
	page_kitchen=69

	print("\n")

	with DBControl(
			fpath,
			setup=True,
			pages=[
				page_kitchen,
				page_livingroom
			],
			cfg_verbose=True
		) as dbctl:

		# Adding items to the kitchen page
		page=page_kitchen
		dbctl.db_tx_begin()
		dbctl.db_post("furnace",1,page=page)
		dbctl.db_post("sink",1,page=page)
		dbctl.db_post("fridge",2,page=page)

		# Adding items to the livingroom
		page=page_livingroom
		dbctl.db_post("chairs",4,page=page)
		dbctl.db_post("lamps",2,page=page)

	print("\n")

	with DBControl(
			fpath,
			cfg_verbose=True
		) as dbctl:

		try:
			dbctl.db_get("fridge")
		except Exception as exc:
			print(
				f"→ This error"
				f"\n\t{exc}"
				"\n\thappens because the only pages that exist are "
				f"\n\t{page_kitchen} and {page_livingroom}"
			)

		print("the furnace is found in the kitchen")
		print("\tfurnace:",dbctl.db_get("furnace",page=page_kitchen))

		print("the lamps have been found on the living room")
		print("\tlamps:",dbctl.db_get("lamps",page=page_livingroom))


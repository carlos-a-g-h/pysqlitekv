#!/usr/bin/python3.9

from pathlib import Path

from aiosqlite import connect as aiosqlite_connect

from pysqlitekv_async import (

	db_init,db_getcur,
	db_post,db_lpost,db_get,db_lget,db_custom,db_keys,
	db_tx_commit,

	# The async version of db_custom() allows you to run
	# not just regular functions, but can also run awaitable
	# functions and blocking code that would require the use of the
	# to_thread async function. However, you have to state that
	# explicitly
	_RUN_NORMAL,
	_RUN_AWAITABLE,
	_RUN_TOTHREAD,

)

def add_more(val:int,new:int=0):

	# For simple code

	return val+new

async def main(fpath:Path):

	p_fridge=42
	p_shelf=69

	ok=await db_init(
		fpath,
		new_pages=[p_fridge,p_shelf],
		confirm_only=True
	)
	if not ok:
		"???"
		return

	async with aiosqlite_connect(fpath) as con:

		print("\n→ isolated ops")

		await db_post(
			con,"tomatoes",40,
			page=p_fridge,
			verbose=True
		)
		await db_get(
			con,"tomatoes",
			page=p_fridge,
			display_results=True
		)
		await db_custom(
			con,"tomatoes",
			# This is a custom function
				add_more,
			# This is the argument for the custom function
				custom_func_params=100,

			page=p_fridge,
			res_write=True,
			verbose=True
		)
		await db_get(con,"tomatoes",page=p_fridge,display_results=True)
		await db_post(con,"lettuce",44,page=p_fridge,verbose=True)
		await db_keys(con,page=p_fridge,display_results=True)

		print("\n→ with transaction")

		cur=await db_getcur(con,begin_transaction=True)
		await db_post(cur,"beancans",1,page=p_shelf)
		await db_post(cur,"sugar",2,page=p_shelf)
		# await db_tx_rollback(cur,close_cursor=True,verbose=True)
		await db_lpost(cur,"cabinet","knife",page=p_shelf,verbose=True)
		await db_lpost(cur,"cabinet",["spoon","medicine",69,"smthn random"],page=p_shelf)
		await db_tx_commit(cur,close_cursor=True,verbose=True)

		await db_get(con,"cabinet",page=p_shelf,display_results=True)

		await db_lget(con,"cabinet",(2,None),page=p_shelf,display_results=True,verbose=True)

		await db_keys(con,page=p_shelf,display_results=True)

if __name__=="__main__":

	from asyncio import run as async_run

	fpath=Path("example_async.db")
	if fpath.is_file():
		fpath.unlink()

	async_run(main(fpath))
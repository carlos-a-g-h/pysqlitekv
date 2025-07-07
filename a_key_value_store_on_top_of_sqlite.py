#!/usr/bin/python3.9

# pysqlitekv

from pathlib import Path
from pickle import (
	dumps as pckl_encode,
	loads as pckl_decode
)
from sqlite3 import (
	connect as sql_connect,
	Connection as SQLConnection,
	Cursor as SQLCursor
)
from typing import Any,Mapping,Optional,Union

_SQL_TAB_ITEMS="Items"
_SQL_COL_KEY="Key_name"
_SQL_COL_TYPE="Data_type"
_SQL_COL_VALUE_STR="Value_as_String"
_SQL_COL_VALUE_INT="Value_as_Int"
_SQL_COL_VALUE_BLOB="Value_as_Any"

_TYPE_STRING=0
_TYPE_INT=1
_TYPE_LIST=2
_TYPE_HASHMAP=3
_TYPE_ANY=4

# Utilities

def util_fmatch(
		text_sub:str,
		text_orig:Optional[str]
	)->int:

	# 0 = No match
	# 1 = Match
	# 2 = Exact match
	# 3 = Perfect match

	score=0
	if text_orig is None:
		return score
	if len(text_orig)==0:
		return score
	if len(text_orig.strip())==0:
		return score

	text_orig_low=text_orig.strip().lower()
	text_sub_low=text_sub.strip().lower()

	if text_orig_low.find(text_sub_low)>-1:
		score=score+1
		if text_orig_low==text_sub_low:
			score=score+1
			if text_orig==text_sub:
				score=score+1

	return score

def util_dtype_check(dtype:int)->bool:
	return dtype in (
		_TYPE_STRING,_TYPE_INT,
		_TYPE_LIST,_TYPE_HASHMAP,
		_TYPE_ANY
	)

def util_extract_correct_value(
		row:tuple,
		tgt_col:Optional[str]=None
	)->Optional[Any]:

	# NOTE:
	# this tuple is the result of a select WITHOUT the ID
	# ( data type, str, int, blob )

	if tgt_col is not None:

		if tgt_col==_SQL_COL_VALUE_STR:
			return row[1]
		if tgt_col==_SQL_COL_VALUE_INT:
			return row[2]
		if tgt_col==_SQL_COL_VALUE_BLOB:
			return pckl_decode(row[3])

		return None

	# Use data type in row[0]

	if row[0]==_TYPE_STRING:
		return row[1]
	if row[0]==_TYPE_INT:
		return row[2]
	if row[0] in (_TYPE_LIST,_TYPE_HASHMAP,_TYPE_ANY):
		return pckl_decode(row[3])

	return None

def util_get_dtype_col_from_dtype_id(dtype:int)->str:

	if dtype==0:
		return _SQL_COL_VALUE_STR
	if dtype==1:
		return _SQL_COL_VALUE_INT

	return _SQL_COL_VALUE_BLOB

def util_get_dtype_id_from_value(data:Any)->Optional[int]:

	if data is None:
		return None

	data_type=_TYPE_ANY
	if isinstance(data,str):
		if len(data)==0:
			return None
		if len(data.strip())==0:
			return None
		data_type=_TYPE_STRING

	if isinstance(data,(int,float)):
		data_type=_TYPE_INT

	if isinstance(data,Mapping):
		data_type=_TYPE_HASHMAP

	if isinstance(data,list):
		data_type=_TYPE_LIST

	return data_type

def util_is_cur(con_or_cur:Union[SQLConnection,SQLCursor])->bool:

	return isinstance(con_or_cur,SQLCursor)

# Connection and init functions

def db_init(
		filepath:Path,
		confirm_only:bool=False,
	)->Union[bool,Optional[SQLConnection]]:

	filepath.parent.mkdir(
		parents=True,
		exist_ok=True
	)

	con:SQLConnection=sql_connect(filepath)
	con.execute(
		f"CREATE TABLE IF NOT EXISTS {_SQL_TAB_ITEMS}("
			f"{_SQL_COL_KEY} VARCHAR(64) UNIQUE,"
			f"{_SQL_COL_TYPE} INT,"
			f"{_SQL_COL_VALUE_STR} VARCHAR,"
			f"{_SQL_COL_VALUE_INT} INT,"
			f"{_SQL_COL_VALUE_BLOB} BLOB"
		");"
	)
	if confirm_only:
		con.close()
		return True

	return con

def db_getcon(filepath:Path)->SQLConnection:

	return sql_connect(filepath)

def db_getcur(
		con_or_cur:Union[SQLConnection,SQLCursor],
		begin_transaction:bool=False,
		verbose:bool=False
	)->SQLCursor:

	if not isinstance(con_or_cur,SQLCursor):
		if verbose:
			print(
				db_getcur.__name__,
				"creating a new cursor from the given connection"
			)

		cur=con_or_cur.cursor()
		if begin_transaction:
			if verbose:
				print(
					db_getcur.__name__,
					"begginning transaction in the new cursor"
				)
			cur.execute("BEGIN TRANSACTION")

		return cur

	if begin_transaction:
		if verbose:
			print(
				db_getcur.__name__,
				"begginning transaction in the given cursor"
			)
		con_or_cur.execute("BEGIN TRANSACTION")

	return con_or_cur

# Transaction functions

def db_tx_begin(
		cur:SQLCursor,
		verbose:bool=False
	):
	if verbose:
		print(
			db_tx_begin.__name__,
			"begginning transaction"
		)

	cur.execute("BEGIN TRANSACTION")

def db_tx_commit(
		cur:SQLCursor,
		close_cursor:bool=False,
		verbose:bool=False
	):
	if verbose:
		print(
			db_tx_commit.__name__,
			"committing changes"
		)

	cur.execute("COMMIT")
	if close_cursor:
		if verbose:
			print(
				db_tx_commit.__name__,
				"closing cursor after committing"
			)
		cur.close()

def db_tx_rollback(
		cur:SQLCursor,
		close_cursor:bool=False,
		verbose:bool=False
	):
	if verbose:
		print(
			db_tx_rollback.__name__,
			"rolling back"
		)

	cur.execute("ROLLBACK")
	if close_cursor:
		if verbose:
			print(
				db_tx_rollback.__name__,
				"closing after rolling back..."
			)
		cur.close()

# Main functions

def db_post(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,value:Any,
		force:bool=False,
		verbose:bool=False,
	)->bool:

	# OK

	# NOTE:
	# If you push a list or a mapping,
	# they can only be updated/modified with
	# db_lpost, db_ldelete (for lists)
	# and db_hupdate (for hashmaps)

	dtype=util_get_dtype_id_from_value(value)
	if dtype is None:
		if verbose:
			print(
				db_post.__name__,
				"data type not valid"
			)

		return False

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	query="INSERT"
	if force:
		query=f"{query} OR REPLACE"
	query=(
		f"{query} INTO {_SQL_TAB_ITEMS} "
		"VALUES(?,?,?,?,?)"
	)

	key_ready=key_name.strip().lower()
	params:Optional[tuple]=None

	if dtype==_TYPE_STRING:
		params=(key_ready,dtype,value,None,None)
	if dtype==_TYPE_INT:
		params=(key_ready,dtype,None,int(value),None)
	if dtype in (_TYPE_ANY,_TYPE_LIST,_TYPE_HASHMAP):
		params=(key_ready,dtype,None,None,pckl_encode(value))

	cur.execute(query,params)

	if isolated:
		con_or_cur.commit()
		cur.close()

	if verbose:
		print(
			db_post.__name__,
			f"OK ({key_ready})",
			value
		)

	return True

def db_get(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,
		display_results:bool=False,
		verbose:bool=True
	)->Optional[Any]:

	# OK

	# NOTE:
	# For specific values inside mappings and lists, db_lget and db_mget must be used

	key_ready=key_name.strip().lower()
	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	cur.execute(
		f"SELECT {_SQL_COL_TYPE},{_SQL_COL_VALUE_STR},{_SQL_COL_VALUE_INT},{_SQL_COL_VALUE_BLOB} "
			f"FROM {_SQL_TAB_ITEMS} "
				f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
					f"""AND {_SQL_COL_TYPE}>{_TYPE_STRING-1} """
					f"""AND {_SQL_COL_TYPE}<{_TYPE_ANY+1};"""
	)
	select_result=cur.fetchone()

	if isolated:
		cur.close()

	if select_result is None:
		if verbose:
			print(
				db_get.__name__,
				f"{key_ready} Not found"
			)

		return None

	the_value=util_extract_correct_value(select_result)
	if the_value is None:
		if verbose:
			print(
				db_get.__name__,
				f"{key_ready} Not found in {select_result} ?"
			)

		return None

	if display_results:
		print(
			f"{db_get.__name__}[{key_ready}] =",
			the_value
		)

	return the_value

def db_delete(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,
		return_value:bool=False,
		verbose:bool=False
	)->Union[bool,Optional[Any]]:

	# NOTE:
	# If you want the value to be returned, it has to be deleted successfully first

	key_ready=key_name.strip().lower()
	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	if return_value:

		cur.execute(
			f"SELECT {_SQL_COL_TYPE},{_SQL_COL_VALUE_STR},{_SQL_COL_VALUE_INT},{_SQL_COL_VALUE_BLOB} "
				f"FROM {_SQL_TAB_ITEMS} "
					f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
						f"""AND {_SQL_COL_TYPE}>{_TYPE_STRING-1} """
						f"""AND {_SQL_COL_TYPE}<{_TYPE_ANY+1};"""
		)

	if not return_value:

		# NOTE: this is better in case the row is screwed up or whatever and the only choice left is to nuke it

		cur.execute(
			f"SELECT {_SQL_COL_KEY} FROM {_SQL_TAB_ITEMS} "
				f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
		)

	result=cur.fetchone()
	if result is None:
		if verbose:
			print(
				db_delete.__name__,
				f"{key_ready} Not found"
			)
		if isolated:
			cur.close()
		if return_value:
			return None
		return False

	value:Optional[Any]=None

	if return_value:

		value=util_extract_correct_value(result)
		if value is None:
			if verbose:
				print(
					db_delete.__name__,
					f"{key_ready} Not found in {result} ?"
				)
			if isolated:
				cur.close()

			return None

	cur.execute(
		f"DELETE FROM {_SQL_TAB_ITEMS} "
			f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
	)
	if isolated:
		con_or_cur.commit()
		cur.close()

	if return_value:
		return value

	return True

def db_lpost(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,value:Any,
		force:bool=False,
		verbose:bool=False,
	)->bool:

	# NOTE:
	# If you push a list,
	# the stored list gets extended with .extend()

	is_list=(isinstance(value,list))

	key_ready=key_name.strip().lower()
	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)
	cur.execute(
		f"SELECT {_SQL_COL_TYPE},{_SQL_COL_VALUE_BLOB} "
			f"FROM {_SQL_TAB_ITEMS} "
				f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
					f"AND {_SQL_COL_TYPE}>{_TYPE_STRING-1} "
					f"AND {_SQL_COL_TYPE}<{_TYPE_ANY+1};"
	)
	result=cur.fetchone()
	if result is not None:

		if not force:

			if not result[0]==_TYPE_LIST:
				cur.close()
				return False

			the_thing=pckl_decode(result[1])

			if not is_list:
				the_thing.append(value)
			if is_list:
				the_thing.extend(value)

			the_query=(
				f"UPDATE {_SQL_TAB_ITEMS} "
					f"SET {_SQL_COL_VALUE_BLOB}=? "
					f"WHERE {_SQL_COL_KEY}=? "
			)

			cur.execute(
				the_query.strip(),
				(
					pckl_encode(the_thing),
					key_ready
				)
			)
			if isolated:
				con_or_cur.commit()
				cur.close()

			if verbose:
				print(
					db_lpost.__name__,
					f"OK ({key_ready})",
					value
				)
			return True

	value_ok=[]
	if not is_list:
		value_ok.append(value)
	if is_list:
		value_ok.extend(value)

	the_query="INSERT"
	if force:
		the_query=f"{the_query} OR REPLACE"
	the_query=(
		f"{the_query} INTO {_SQL_TAB_ITEMS} "
		"VALUES(?,?,?,?,?)"
	)

	cur.execute(
		the_query,
		(
			key_ready,
			_TYPE_LIST,
			None,
			None,
			pckl_encode(value_ok)
		)
	)
	if isolated:
		con_or_cur.commit()
		cur.close()

	if verbose:
		print(
			db_lpost.__name__,
			f"OK ({key_ready})",
			value_ok
		)

	return True

def db_lget(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,target:Union[int,tuple],
		display_results:bool=False,
		verbose:bool=False,
	)->Optional[Union[list,Any]]:

	select_one=isinstance(target,int)
	select_slice=isinstance(target,tuple)
	idx_min_ok=False
	idx_max_ok=False
	if select_slice:
		select_slice=len(target)==2
		if select_slice:
			idx_min_ok=isinstance(target[0],int)
			idx_max_ok=isinstance(target[1],int)
			select_slice=(
				idx_min_ok or
				idx_max_ok
			)

	if not (select_one or select_slice):
		raise Exception(
			f"{db_lget.__name__}: target not valid"
		)

	key_ready=key_name.strip().lower()
	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	query=(
		f"SELECT {_SQL_COL_VALUE_BLOB} FROM {_SQL_TAB_ITEMS} "
			f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
				f"AND {_SQL_COL_TYPE}={_TYPE_LIST}"
	)
	cur.execute(query.strip())
	result=cur.fetchone()
	if result is None:
		if verbose:
			print(
				db_lget.__name__,
				f"{key_ready} not found"
			)
		if isolated:
			cur.close()
		if not select_one:
			return []
		return None

	the_thing:list=pckl_decode(result[0])

	size=len(the_thing)

	if select_one:

		if target>-1:

			if target>size-1:
				return None

			if display_results:
				print(
					db_lget.__name__,
					f"{key_ready}[{target}] =",
					the_thing[target]
				)
				return None

			return the_thing[target]

		# From the end

		if target>size:
			return None

		idx=-1*target

		if display_results:
			print(
				f"{db_lget.__name__}[{key_ready}][{target}] =",
				the_thing[target]
			)
			return None

		return the_thing[idx]

	# Select a slice

	idx_min,idx_max=target
	if idx_min:
		if idx_min<0:
			idx_min=0
	if idx_max:
		if idx_max>size-1:
			idx_max=size
	if not idx_min_ok:
		idx_min=0
	if not idx_max_ok:
		idx_max=size

	if idx_min>idx_max:
		return []

	if display_results:
		print(
			f"{db_lget.__name__}[{key_ready}][{idx_min} to {idx_max}] =",
			the_thing[idx_min:idx_max+1]
		)

	return the_thing[idx_min:idx_max+1]

def db_ldelete(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,
		target:Union[int,tuple],
		return_values:bool=False,
		verbose:bool=False,
	)->bool:

	select_one=isinstance(target,int)
	select_slice=isinstance(target,tuple)
	idx_min_ok=False
	idx_max_ok=False
	if select_slice:
		select_slice=len(target)==2
		if select_slice:
			idx_min_ok=isinstance(target[0],int)
			idx_max_ok=isinstance(target[1],int)
			select_slice=(
				idx_min_ok or
				idx_max_ok
			)

	if not (select_one or select_slice):
		raise Exception(
			f"{db_ldelete.__name__}: target not valid"
		)

	key_ready=key_name.strip().lower()

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)
	cur.execute(
		f"SELECT {_SQL_COL_VALUE_BLOB} FROM {_SQL_TAB_ITEMS} "
			f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
				f"AND {_SQL_COL_TYPE}={_TYPE_LIST}"
	)

	result=cur.fetchone()
	if result is None:
		if verbose:
			print(
				db_ldelete.__name__,
				f"{key_name} not found"
			)
		if isolated:
			cur.close()
		if return_values:
			if not select_one:
				return []
			return None
		return False

	the_thing:list=pckl_decode(result[0])

	size=len(the_thing)

	values=[]

	if size>0 and select_one:

		if target>-1:
			if target>size-1:
				if return_values:
					return None
				return False

			if return_values:
				return the_thing.pop(target)

			the_thing.pop(target)
			return True

		# From the end

		if target>size:
			if return_values:
				return None
			return False

		idx=(-1)*target

		if return_values:
			values.append(
				the_thing.pop(idx)
			)
		if not return_values:
			the_thing.pop(idx)

	if size>0 and select_slice:

		idx_min,idx_max=target
		if idx_min:
			if idx_min<0:
				idx_min=0
		if idx_max:
			if idx_max>size-1:
				idx_max=size
		if not idx_min_ok:
			idx_min=0
		if not idx_max_ok:
			idx_max=size
		if idx_min>idx_max:
			if return_values:
				return []
			return False

		idx=idx_min
		targets=idx_max-idx_min

		while True:
			if idx>size-1:
				break

			if targets==0:
				break

			if return_values:
				values.append(
					the_thing.pop(idx)
				)
			if not return_values:
				the_thing.pop(idx)

			size=size-1
			targets=targets-1

	the_query=(
		f"UPDATE {_SQL_TAB_ITEMS} "
			f"SET {_SQL_COL_VALUE_BLOB}=? "
			f"WHERE {_SQL_COL_KEY}=? "
	)
	cur.execute(
		the_query.strip(),
		(
			pckl_encode(the_thing),
			key_ready
		)
	)

	if isolated:
		con_or_cur.commit()
		cur.close()

	if return_values:
		if select_slice:
			return values
		if select_one:
			return values.pop()

	return True

def db_hupdate(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,
		data_to_add:Mapping={},
		data_to_remove:list=[],
		force:bool=False,
		return_value:bool=False,
		verbose:bool=False,
	)->Union[bool,Mapping]:

	# NOTE: Data is first removed and then added

	has_stuff_to_add=(len(data_to_add)>0)
	has_stuff_to_remove=(len(data_to_remove)>0)

	if not (has_stuff_to_add or has_stuff_to_remove):
		if return_value:
			return {}

		return False

	key_ready=key_name.strip().lower()

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)
	cur.execute(
		f"SELECT {_SQL_COL_VALUE_BLOB} FROM {_SQL_TAB_ITEMS} "
			f"""WHERE {_SQL_COL_KEY}="{key_ready}" AND """
				f"{_SQL_COL_TYPE}={_TYPE_HASHMAP}"
	)
	result=cur.fetchone()

	if result is not None:

		if not force:

			the_thing:Mapping=pckl_decode(result[0])

			if has_stuff_to_remove:
				for target in data_to_remove:
					if target not in the_thing.keys():
						continue
					the_thing.pop(target)

			if has_stuff_to_add:
				the_thing.update(data_to_add)

			the_query=(
				f"UPDATE {_SQL_TAB_ITEMS} "
					f"SET {_SQL_COL_VALUE_BLOB}=? "
					f"WHERE {_SQL_COL_KEY}=? "
			)
			cur.execute(
				the_query.strip(),
				(
					pckl_encode(the_thing),
					key_ready
				)
			)

			if isolated:
				con_or_cur.commit()
				cur.close()

			if return_value:
				return the_thing

			return True

	the_query="INSERT"
	if force:
		the_query=f"{the_query} OR REPLACE"
	the_query=(
		f"{the_query} INTO {_SQL_TAB_ITEMS} "
		"VALUES(?,?,?,?,?)"
	)

	cur.execute(
		the_query,
		(
			key_ready,
			_TYPE_HASHMAP,
			None,
			None,
			pckl_encode(data_to_add)
		)
	)
	if isolated:
		con_or_cur.commit()
		cur.close()

	return True

def db_hget(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,
		target:list=[],
		display_results:bool=False,
		verbose:bool=False,
	)->Mapping:

	key_ready=key_name.strip().lower()
	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	cur.execute(
		f"SELECT {_SQL_COL_VALUE_BLOB} FROM {_SQL_TAB_ITEMS} "
			f"""WHERE {_SQL_COL_KEY}="{key_ready}" AND """
				f"{_SQL_COL_TYPE}={_TYPE_HASHMAP}"
	)
	result=cur.fetchone()
	if result is None:
		if verbose:
			print(
				db_hget.__name__,
				f"{key_ready} not found"
			)
		if isolated:
			cur.close()

		return {}

	the_thing:Mapping=pckl_decode(result[0])

	if len(target)==0:
		if display_results:
			print(the_thing)
			return {}

		return the_thing

	# Specific keys

	selection={}

	for key in target:

		if key not in the_thing.keys():
			continue

		selection.update(
			{key:the_thing.pop(key)}
		)

	if display_results:
		print(
			f"{db_hget.__name__}[{key_ready}]{target} =",
			selection
		)
		return {}

	return selection

def db_len(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:Optional[str]=None,
	)->int:

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	from_specific_key=isinstance(key_name,str)

	size=0

	if from_specific_key:

		key_ready=key_name.strip().lower()

		cur.execute(
			f"SELECT {_SQL_COL_TYPE} FROM {_SQL_TAB_ITEMS} "
				f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
		)
		result=cur.fetchone()
		if result is None:
			if isolated:
				cur.close()
			return -1

		if result[0] not in (_TYPE_LIST,_TYPE_HASHMAP):
			if isolated:
				cur.close()
			return -1

		cur.execute(
			f"SELECT {_SQL_COL_VALUE_BLOB} FROM {_SQL_TAB_ITEMS} "
				f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
		)
		result=cur.fetchone()
		if result is None:
			if isolated:
				cur.close()
			return -1

		size=len(
			pckl_decode(
				result[0]
			)
		)

	if not from_specific_key:

		cur.execute(
			f"SELECT {_SQL_COL_KEY} FROM {_SQL_TAB_ITEMS}"
		)
		for thing in cur:
			size=size+1

	if isolated:
		cur.close()

	return size

# The following are fuzzy matching functions

def db_fmstr(
		con_or_cur:Union[SQLConnection,SQLCursor],
		text_sub:str,
		display_results:bool=False,
	)->list:

	text_ok=text_sub.strip()

	matches=[]
	matches_exact=[]
	matches_perfect=[]

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	cur.execute(
		f"SELECT {_SQL_COL_KEY},{_SQL_COL_VALUE_STR} "
			f"FROM {_SQL_TAB_ITEMS} "
				f"WHERE {_SQL_COL_TYPE}={_TYPE_STRING}"
	)

	for row in cur:
		res=util_fmatch(
			text_ok,
			row[1]
		)
		if res==0:
			continue

		if res==1:
			matches.append(row)

		if res==2:
			matches_exact.append(row)

		if res==3:
			matches_perfect.append(row)

	if isolated:
		cur.close()

	final_list=[]

	size=len(matches_perfect)
	if not size==0:
		while True:
			final_list.append(
				matches_perfect.pop()
			)
			size=size-1
			if size==0:
				break

	size=len(matches_exact)
	if not size==0:
		while True:
			final_list.append(
				matches_exact.pop()
			)
			size=size-1
			if size==0:
				break

	size=len(matches)
	if not size==0:
		while True:
			final_list.append(
				matches.pop()
			)
			size=size-1
			if size==0:
				break

	if display_results:
		print(db_fmstr,text_ok,final_list)

	return final_list

# Class object with ALL the functions

class DBControl:

	# fpath:Optional[Path]=None
	# setup:bool=False

	verbose:bool=False

	as_cm:bool=False

	con:Optional[SQLConnection]=None
	cur:Optional[SQLCursor]=None

	def dbg_msg(self,message:str):
		if self.verbose:
			print(
				self.__class__.__name__,
				message
			)

	def __init__(
			self,
			filepath:Path,
			setup:bool=False,
			cfg_verbose:bool=False
		):

		self.verbose=cfg_verbose

		if setup:
			self.con=db_init(filepath)

		if not setup:
			self.con=db_getcon(filepath)

		# self.fpath=filepath
		# self.setup=setup

	def __enter__(self):

		self.dbg_msg("opening context manager")

		self.as_cm=True

		return self

	def __exit__(self,exc_type,exc_value,exc_traceback):

		if not self.as_cm:
			self.dbg_msg("NOTE: This method is not meant to run outside the context manager")
			raise Exception("WTF man")

		has_cursor=(self.cur is not None)
		has_tx=self.con.in_transaction

		self.dbg_msg(f"closing context manager; has_cursor = {has_cursor}; has_tx = {has_tx}")

		if exc_type is None:

			if has_cursor and has_tx:
				self.dbg_msg("committing changes to the pending transaction on the cursor")
				self.cur.execute("COMMIT;")
				self.cur.close()
				has_tx=self.con.in_transaction

			if has_tx:
				self.dbg_msg("committing changes to ALL pending transactions from this connection")
				self.con.commit()

		if exc_type is not None:

			if has_cursor and has_tx:
				self.dbg_msg("rolling back due to an error")
				self.cur.execute("ROLLBACK;")

		if has_cursor:
			self.dbg_msg("closing the cursor before closing the connection")
			self.cur.close()

		self.dbg_msg("closing the connection")
		self.con.close()

	def close(self,rollback:bool=False)->bool:

		# NOTE: Rollback only works if the cursor is up and it has a pending transaction

		if self.as_cm:
			self.dbg_msg("NOTE: This method is not meant to run inside a context manager")
			return False

		has_cursor=(self.cur is not None)
		has_tx=self.con.in_transaction

		self.dbg_msg(f"closing the object; has_cursor = {has_cursor}; has_tx = {has_tx}")

		if not rollback:

			if has_cursor and has_tx:
				self.dbg_msg("committing changes to the pending transaction on the cursor")
				self.cur.execute("COMMIT;")
				self.cur.close()
				has_tx=self.con.in_transaction

			if has_tx:
				self.dbg_msg("committing changes to ALL pending transactions from this connection")
				self.con.commit()

		if rollback:

			if has_cursor and has_tx:
				self.dbg_msg("rolling back due to an error")
				self.cur.execute("ROLLBACK;")

		if has_cursor:
			self.dbg_msg("closing the cursor before closing the connection")
			self.cur.close()

		self.dbg_msg("closing the connection")
		self.con.close()

		return True

	def db_tx_begin(self)->bool:

		if self.cur is not None:
			return False

		if self.con.in_transaction:
			return False

		self.cur=db_getcur(
			self.con,
			begin_transaction=True,
			verbose=self.verbose
		)
		return True

	def db_tx_commit(self)->bool:

		if self.cur is None:
			return False
		if not self.con.in_transaction:
			return False

		db_tx_commit(
			self.cur,
			close_cursor=True,
			verbose=self.verbose
		)

		self.cur=None

		return True

	def db_tx_rollback(self)->bool:

		if self.cur is not None:
			return False
		if not self.con.in_transaction:
			return False

		db_tx_rollback(
			self.cur,
			close_cursor=True,
			verbose=self.verbose
		)

		self.cur=None

		return True

	def db_post(
			self,
			keyname:str,
			value:Any,
			force:bool=False
		)->bool:

		if self.cur is not None:
			return db_post(
				self.cur,
				keyname,value,
				force=force,
				verbose=self.verbose
			)
		return db_post(
			self.con,
			keyname,value,
			force=force,
			verbose=self.verbose
		)

	def db_get(
			self,
			keyname:str
		)->Optional[Any]:

		if self.cur is not None:
			return db_get(
				self.cur,keyname,
				verbose=self.verbose
			)
		return db_get(
			self.con,keyname,
			verbose=self.verbose
		)

	def db_delete(
			self,
			keyname:str,
			retval:bool=False
		)->Union[bool,Optional[Any]]:

		if self.cur is not None:
			return db_delete(
				self.cur,
				keyname,
				return_value=retval,
				verbose=self.verbose
			)
		return db_delete(
			self.con,
			keyname,
			return_value=retval,
			verbose=self.verbose
		)

	def db_lpost(
			self,
			keyname:str,
			value:Any,
			force:bool=False
		)->bool:

		if self.cur is not None:
			return db_lpost(
				self.cur,
				keyname,
				value,
				force=force,
				verbose=self.verbose
			)
		return db_lpost(
			self.con,
			keyname,
			value,
			force=force,
			verbose=self.verbose
		)

	def db_lget(
			self,
			keyname:str,
			target:Union[int,tuple],
		)->Optional[Union[list,Any]]:

		if self.cur is not None:
			return db_lget(
				self.cur,
				keyname,target,
				verbose=self.verbose
			)
		return db_lget(
			self.con,
			keyname,target,
			verbose=self.verbose
		)

	def db_ldelete(
			self,
			keyname:str,
			target:Union[int,tuple],
			retval:bool=False,
		)->bool:

		if self.cur is not None:
			return db_ldelete(
				self.cur,
				keyname,target,
				return_values=retval,
				verbose=self.verbose
			)
		return db_ldelete(
			self.con,
			keyname,target,
			return_values=retval,
			verbose=self.verbose
		)

	def db_hupdate(
			self,
			keyname:str,
			new:Mapping={},
			remove:list=[],
			retval:bool=False,
			force:bool=False,
		)->Union[bool,Mapping]:

		if self.cur is not None:
			return db_hupdate(
				self.cur,
				keyname,
				new,remove,
				force=force,
				return_value=retval,
				verbose=self.verbose
			)
		return db_hupdate(
			self.con,
			keyname,
			new,remove,
			force=force,
			return_value=retval,
			verbose=self.verbose
		)

	def db_hget(
			self,
			keyname:str,
			target:list=[]
		)->Mapping:

		if self.cur is not None:
			return db_hget(
				self.cur,
				keyname,target,
				verbose=self.verbose
			)
		return db_hget(
			self.con,
			keyname,target,
			verbose=self.verbose
		)

	def db_len(
			self,
			keyname:Optional[str]=None
		)->int:

		if self.cur is not None:
			return db_len(
				self.cur,
				keyname
			)
		return db_len(
			self.con,
			keyname
		)

	def db_fmstr(
			self,
			sub_str:str,
		)->list:

		if self.cur is not None:
			return db_fmstr(
				self.cur,
				sub_str,
			)
		return db_fmstr(
			self.con,
			sub_str,
		)

# Transaction object

class DBTransaction:

	cur:Optional[SQLCursor]=None
	sqlcon:Optional[SQLConnection]=None
	verbose:bool=False

	def __init__(
			self,sqlcon:SQLConnection,
			cfg_verbose:bool=False,
		):

		self.sqlcon=sqlcon
		self.verbose=cfg_verbose

	def dbg_msg(self,message:str):
		if self.verbose:
			print(
				self.__class__.__name__,
				message
			)

	def db_post(
			self,
			keyname:str,
			value:Any,
			force:bool=False
		)->bool:

		return db_post(
			self.cur,
			keyname,value,
			force=force,
			verbose=self.verbose
		)

	def db_get(
			self,
			keyname:str
		)->Optional[Any]:

		return db_get(
			self.cur,keyname,
			verbose=self.verbose
		)

	def db_delete(
			self,
			keyname:str,
			retval:bool=False
		)->Union[bool,Optional[Any]]:

		return db_delete(
			self.cur,keyname,
			return_value=retval,
			verbose=self.verbose
		)

	def db_lpost(
			self,
			keyname:str,
			value:Any,
			force:bool=False
		)->bool:

		return db_lpost(
			self.cur,
			keyname,
			value,
			force=force,
			verbose=self.verbose
		)

	def db_lget(
			self,
			keyname:str,
			target:Union[int,tuple],
		)->Optional[Union[list,Any]]:

		return db_lget(
			self.cur,
			keyname,target,
			verbose=self.verbose
		)

	def db_ldelete(
			self,
			keyname:str,
			target:Union[int,tuple],
			retval:bool=False,
		)->bool:

		return db_ldelete(
			self.cur,
			keyname,target,
			return_values=retval,
			verbose=self.verbose
		)

	def db_hupdate(
			self,
			keyname:str,
			new:Mapping={},
			remove:list=[],
			retval:bool=False,
			force:bool=False,
		)->Union[bool,Mapping]:

		return db_hupdate(
			self.cur,
			keyname,
			new,remove,
			force=force,
			return_value=retval,
			verbose=self.verbose
		)

	def db_hget(
			self,
			keyname:str,
			target:list=[]
		)->Mapping:

		return db_hget(
			self.cur,
			keyname,target,
			verbose=self.verbose
		)

	def db_len(
			self,
			keyname:Optional[str]=None
		)->int:

		return db_len(
			self.cur,
			keyname
		)

	def db_fmstr(
			self,
			sub_str:str,
		)->list:

		return db_fmstr(
			self.cur,
			sub_str,
		)

	def __enter__(self):

		self.dbg_msg("opening context manager...")

		if self.sqlcon.in_transaction:
			self.dbg_msg("WARNING: There is a pending transaction on this connection")

		self.cur=db_getcur(
			self.sqlcon,
			begin_transaction=True,
			verbose=self.verbose
		)
		return self

	def __exit__(self,exc_type,exc_value,exc_traceback):

		self.dbg_msg("closing context manager...")

		if exc_type is None:
			self.dbg_msg("committing changes to the database")
			self.cur.execute("COMMIT")

		if exc_type is not None:
			self.dbg_msg("rolling back changes due to an error")
			self.cur.execute("ROLLBACK")

		self.dbg_msg("closing the cursor")
		self.cur.close()

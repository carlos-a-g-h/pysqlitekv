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

from typing import Any,Callable,Mapping,Optional,Union

_SQL_TAB_ITEMS="Items"
_SQL_COL_KEY="Key_name"
_SQL_COL_TYPE="Data_type"
_SQL_COL_VALUE_STR="Value_as_String"
_SQL_COL_VALUE_INT="Value_as_Int"
_SQL_COL_VALUE_BLOB="Value_as_Any"

_SQL_TX_BEGIN="BEGIN TRANSACTION"
_SQL_TX_COMMIT="COMMIT"
_SQL_TX_ROLLBACK="ROLLBACK"

_SORT_LOW_TO_HI=1
_SORT_NONE=0
_SORT_HI_TO_LOW=-1

_TYPE_STRING=0
_TYPE_INT=1
_TYPE_LIST=2
_TYPE_HASHMAP=3
_TYPE_ANY=4

_TARGET_ONE=0
_TARGET_SLICE=1

# Utilities

def util_fmatch(
		text_sub:str,
		text_orig:Optional[str],
		starts_with:bool,
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

	if starts_with:
		if not text_orig_low.startswith(text_sub_low):
			return score

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

def util_get_dtype_from_value(
		data:Any,
		ret_colname:bool=False
	)->Optional[Union[int,str]]:

	if data is None:
		return None

	data_type:Union[int,str]={
		True:_SQL_COL_VALUE_BLOB,
		False:_TYPE_ANY,
	}[ret_colname]
	if isinstance(data,str):
		if len(data)==0:
			return None
		if len(data.strip())==0:
			return None

		data_type={
			True:_SQL_COL_VALUE_STR,
			False:_TYPE_STRING
		}[ret_colname]

	if isinstance(data,(int,float)):
		data_type={
			True:_SQL_COL_VALUE_INT,
			False:_TYPE_INT
		}[ret_colname]

	if isinstance(data,Mapping):
		data_type={
			True:_SQL_COL_VALUE_BLOB,
			False:_TYPE_HASHMAP,
		}[ret_colname]

	if isinstance(data,list):
		data_type={
			True:_SQL_COL_VALUE_BLOB,
			False:_TYPE_LIST,
		}[ret_colname]

	return data_type

def util_extract_from_target_tuple(
		target:Union[int,tuple],
		for_lists:bool=False
	)->Optional[tuple]:

	# NOTE:
	# one: ( _TARGET_ONE , Index )
	# slice: ( _TARGET_SLICE, has_min, has_max ,Minval, Maxval )

	select_one=isinstance(target,int)
	select_slice=isinstance(target,tuple)

	if not (select_one or select_slice):
		return None

	if select_one:
		return (_TARGET_ONE,target)

	# Slice

	select_slice=(len(target)==2)
	if not select_slice:
		return None

	idx_min_ok=isinstance(target[0],int)
	idx_max_ok=isinstance(target[1],int)
	select_slice=(
		idx_min_ok or
		idx_max_ok
	)
	if not select_slice:
		return None

	valmin,valmax=target

	if for_lists:

		if idx_min_ok:
			select_slice=(valmin>0 or valmin==0)
			if not select_slice:
				return None

		if idx_max_ok:
			select_slice=(valmax>0 or valmax==0)
			if not select_slice:
				return None

	if idx_min_ok and idx_max_ok:
		select_slice=(valmax>valmin)

	if not select_slice:
		return None

	return (
		_TARGET_SLICE,
		idx_min_ok,idx_max_ok,
		valmin,valmax
	)

# Utils for building SQL queries

def util_bquery_init(show:bool=False)->str:

	query=(
		f"CREATE TABLE IF NOT EXISTS {_SQL_TAB_ITEMS}("
			f"{_SQL_COL_KEY} VARCHAR(64) UNIQUE,"
			f"{_SQL_COL_TYPE} INT,"
			f"{_SQL_COL_VALUE_STR} VARCHAR,"
			f"{_SQL_COL_VALUE_INT} INT,"
			f"{_SQL_COL_VALUE_BLOB} BLOB"
		");"
	)
	if show:
		print(f"{util_bquery_init.__name__}():",query.strip())

	return query

def util_bquery_select(
		keyname:Optional[str]=None,
		datatype:int=-1,
		show:bool=False,
	)->str:

	has_keyname=isinstance(keyname,str)
	spec_datatype=(
		datatype in (
			_TYPE_STRING,_TYPE_INT,
			_TYPE_LIST,_TYPE_HASHMAP,
			_TYPE_ANY
		)
	)

	# SELECT

	query="SELECT"

	if not spec_datatype:
		query=(
			f"{query} {_SQL_COL_TYPE},"
				f"{_SQL_COL_VALUE_STR},"
				f"{_SQL_COL_VALUE_INT},"
				f"{_SQL_COL_VALUE_BLOB}"
		)
	if spec_datatype:
		query=(
			f"{query} {util_get_dtype_col_from_dtype_id(datatype)}"
		)

	query=f"{query} FROM {_SQL_TAB_ITEMS}"

	# WHERE

	ok=False

	woa={
		False:"WHERE",
		True:"AND"
	}

	if has_keyname:
		query=(
			f"{query} {woa[ok]} "
			f"""{_SQL_COL_KEY}="{keyname}" """
		)
		query.strip()
		ok=True

	if spec_datatype:
		query=(
			f"{query} {woa[ok]} "
			f"{_SQL_COL_TYPE}={datatype}"
		)
		ok=True

	if not spec_datatype:
		query=(
			f"{query} "
				f"{woa[ok]} ( {_SQL_COL_TYPE}={_TYPE_STRING} OR {_SQL_COL_TYPE}>{_TYPE_STRING} ) "
				f"{woa[True]} ( {_SQL_COL_TYPE}={_TYPE_ANY} OR {_SQL_COL_TYPE}<{_TYPE_ANY} )"
		)

	if show:
		print(f"{util_bquery_select.__name__}():",query.strip())

	return query.strip()

def util_bquery_insert(
		replace:bool=False,
		show:bool=False
	)->str:

	query="INSERT"
	if replace:
		query=f"{query} OR REPLACE"

	query=(
		f"{query} INTO {_SQL_TAB_ITEMS} "
		"VALUES(?,?,?,?,?)"
	)

	if show:
		print(f"{util_bquery_insert.__name__}():",query)

	return query

def util_bparams(
		keyname:str,
		value:Any,
		dtype_id:int,
	)->Optional[tuple]:

	if dtype_id==_TYPE_STRING:
		return (
			keyname,dtype_id,
			value,None,None
		)

	if dtype_id==_TYPE_INT:
		return (
			keyname,dtype_id,
			None,value,None
		)

	if dtype_id in (_TYPE_ANY,_TYPE_LIST,_TYPE_HASHMAP):
		return (
			keyname,dtype_id,
			None,None,pckl_encode(value)
		)

	return None

def util_is_cur(con_or_cur:Union[SQLConnection,SQLCursor])->bool:

	return isinstance(con_or_cur,SQLCursor)

# Connection and init functions

def db_init(
		filepath:Path,
		confirm_only:bool=False,
		verbose:bool=False,
	)->Union[bool,Optional[SQLConnection]]:

	filepath.parent.mkdir(
		parents=True,
		exist_ok=True
	)

	con:SQLConnection=sql_connect(filepath)
	con.execute(
		util_bquery_init(verbose)
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

def db_check_changes(
		con_or_cur:Union[SQLConnection,SQLCursor],
		verbose:bool=False
	)->bool:

	has_changes=False

	if isinstance(con_or_cur,SQLConnection):

		has_changes=(con_or_cur.in_transaction)
		if verbose:
			print("In transaction?",has_changes)

	if isinstance(con_or_cur,SQLCursor):
		con_or_cur.execute("SELECT changes()")
		print("CHANGES:")
		qtty=0
		for row in con_or_cur:
			# print(row)
			qtty=qtty+1

		print("pending changes =",qtty)

		has_changes=(not qtty==0)
		if verbose:
			print("Has changes?",has_changes,qtty)

	return has_changes

# Main functions

def db_post(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,value:Any,
		force:bool=False,
		verbose:bool=False,
	)->bool:

	dtype=util_get_dtype_from_value(value)
	if dtype is None:
		if verbose:
			print(
				f"{db_post.__name__}",
				"data type not valid"
			)

		return False

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	key_ready=key_name.strip().lower()
	params:Optional[tuple]=util_bparams(key_ready,value,dtype)
	if params is None:
		if verbose:
			print(
				f"{db_post.__name__}",
				"unable to build params"
			)

		return False

	if isolated:
		cur.execute(_SQL_TX_BEGIN)

	cur.execute(
		util_bquery_insert(replace=force),
		params
	)

	if isolated:
		cur.execute(_SQL_TX_COMMIT)
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
		util_bquery_select(keyname=key_ready)
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
		return_val:bool=False,
		verbose:bool=False
	)->Union[bool,Optional[Any]]:

	# NOTE:
	# If you want the value to be returned, it has to be deleted successfully first

	key_ready=key_name.strip().lower()
	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	if isolated:
		cur.execute(_SQL_TX_BEGIN)

	if return_val:

		cur.execute(
			util_bquery_select(keyname=key_ready)
		)

	if not return_val:

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
			cur.execute(_SQL_TX_ROLLBACK)
			cur.close()
		if return_val:
			return None
		return False

	value:Optional[Any]=None

	if return_val:

		value=util_extract_correct_value(result)
		if value is None:
			if verbose:
				print(
					db_delete.__name__,
					f"{key_ready} Not found in {result} ?"
				)
			if isolated:
				cur.execute(_SQL_TX_ROLLBACK)
				cur.close()

			return None

	cur.execute(
		f"DELETE FROM {_SQL_TAB_ITEMS} "
			f"""WHERE {_SQL_COL_KEY}="{key_ready}" """
	)
	if isolated:
		cur.execute(_SQL_TX_COMMIT)
		cur.close()

	if return_val:
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
					f"""AND ({_SQL_COL_TYPE}={_TYPE_STRING} OR {_SQL_COL_TYPE}>{_TYPE_STRING}) """
					f"""AND ({_SQL_COL_TYPE}={_TYPE_ANY} OR {_SQL_COL_TYPE}<{_TYPE_ANY});"""
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

			if isolated:
				cur.execute(_SQL_TX_BEGIN)

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
				cur.execute(_SQL_TX_COMMIT)
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

	if isolated:
		cur.execute(_SQL_TX_BEGIN)

	cur.execute(
		util_bquery_insert(replace=force),
		(
			key_ready,
			_TYPE_LIST,
			None,
			None,
			pckl_encode(value_ok)
		)
	)
	if isolated:
		# con_or_cur.commit()
		cur.execute(_SQL_TX_COMMIT)
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
	)->Union[list,Optional[Any]]:

	key_ready=key_name.strip().lower()

	target_ok=util_extract_from_target_tuple(target,for_lists=True)
	if target_ok is None:
		if display_results or verbose:
			print(f"{db_delete.__name__}[{key_ready}][{target}] the target is not valid")

		if isinstance(target,tuple):
			return []

		return None

	select_one=(target_ok[0]==_TARGET_ONE)

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	query=(
		util_bquery_select(
			keyname=key_ready,
			datatype=_TYPE_LIST,
			# show=True
		)
	)
	cur.execute(query)
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
	last_idx=size-1

	if select_one:

		idx=target_ok[1]

		# zero and up

		if not idx<0:

			if idx>last_idx:
				if display_results:
					print(
						f"{db_lget.__name__}[{key_ready}][{idx}] is beyond the last index"
					)
				return None

			if display_results:
				print(
					f"{db_lget.__name__}[{key_ready}][{idx}] = {the_thing[idx]}"
				)
				return None

			return the_thing[idx]

		# From the end

		idx_ok=size+idx
		if idx_ok<0:
			return None

		if display_results:
			print(
				f"{db_lget.__name__}[{key_ready}][{idx}] = {the_thing[idx]}"
			)
			return None

		return the_thing[idx_ok]

	# NOTE:
	# Select a slice

	idx_min_ok=target_ok[1]
	idx_max_ok=target_ok[2]
	idx_min=target_ok[3]
	idx_max=target_ok[4]

	if not idx_min_ok:
		idx_min=0
	if not idx_max_ok:
		idx_max=last_idx

	if idx_min_ok:
		if idx_min>last_idx:
			if verbose:
				print(
					f"{db_lget.__name__}[{key_ready}][{(idx_min,idx_max)}] min is above last index"
				)
			return []

	if idx_max_ok:
		if idx_max>last_idx:
			idx_max=size-1

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
		return_val:bool=False,
		verbose:bool=False,
	)->Union[bool,list,Optional[Any]]:

	key_ready=key_name.strip().lower()

	target_ok=util_extract_from_target_tuple(target,for_lists=True)
	if target_ok is None:
		if verbose:
			print(f"{db_delete.__name__}[{key_ready}][{target}] the given target is not valid")

		if isinstance(target,tuple):
			return []

		return None

	select_one=(target_ok[0]==_TARGET_ONE)
	select_slice=(target_ok[0]==_TARGET_SLICE)

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)
	cur.execute(
		util_bquery_select(
			keyname=key_ready,
			datatype=_TYPE_LIST
		)
	)

	result=cur.fetchone()
	if result is None:
		if verbose:
			print(
				f"{db_ldelete.__name__}[{key_ready}]",
				"not found"
			)
		if isolated:
			cur.close()
		if return_val:
			if not select_one:
				return []
			return None
		return False

	the_thing:list=pckl_decode(result[0])

	size=len(the_thing)
	if size==0:
		if verbose:
			print(
				f"{db_ldelete.__name__}[{key_ready}]",
				"is empty"
			)
		if isolated:
			cur.close()
		if return_val:
			if not select_one:
				return []
			return None
		return False

	last_idx=size-1

	values=[]

	if select_one:

		target_idx=target[1]

		if not target_idx<0:
			if target_idx>last_idx:
				if return_val:
					return None
				return False

			if return_val:
				return the_thing.pop(target_idx)

			the_thing.pop(target_idx)
			return True

		# From the end

		idx_ok=size+target_idx
		if idx_ok<0:
			if verbose:
				print(
					f"{db_ldelete.__name__}[{key_ready}][{target_idx}] does not exist"
				)
			if return_val:
				return None
			return False

		if return_val:
			values.append(
				the_thing.pop(idx_ok)
			)
		if not return_val:
			the_thing.pop(idx_ok)

	if select_slice:

		idx_min_ok=target_ok[1]
		idx_max_ok=target_ok[2]
		idx_min=target_ok[3]
		idx_max=target_ok[4]

		if not idx_min_ok:
			idx_min=0
		if not idx_max_ok:
			idx_max=last_idx

		if idx_min_ok:
			if idx_min>last_idx:
				if verbose:
					print(
						f"{db_ldelete.__name__}[{key_ready}][{(idx_min,idx_max)}] min is above last index"
					)
				if return_val:
					return []
				return False

		if idx_max_ok:
			if idx_max>last_idx:
				idx_max=size-1

		idx=idx_min
		targets=idx_max-idx_min

		while True:
			if idx>size-1:
				break

			if targets==0:
				break

			if return_val:
				values.append(
					the_thing.pop(idx)
				)
			if not return_val:
				the_thing.pop(idx)

			size=size-1
			targets=targets-1

	if isolated:
		cur.execute(_SQL_TX_BEGIN)

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
		# con_or_cur.commit()
		cur.execute(_SQL_TX_COMMIT)
		cur.close()

	if return_val:
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
		return_val:bool=False,
		verbose:bool=False,
	)->Union[bool,Mapping]:

	# NOTE: Data is first removed and then added

	has_stuff_to_add=(len(data_to_add)>0)
	has_stuff_to_remove=(len(data_to_remove)>0)

	if not (has_stuff_to_add or has_stuff_to_remove):
		if return_val:
			return {}

		return False

	key_ready=key_name.strip().lower()

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)
	cur.execute(
		util_bquery_select(
			keyname=key_ready,
			datatype=_TYPE_HASHMAP
		)
	)
	result=cur.fetchone()

	if result is not None:

		if not force:

			the_thing:Mapping=pckl_decode(result[0])

			removed_data:Mapping={}

			if has_stuff_to_remove:
				for target in data_to_remove:
					if target not in the_thing.keys():
						continue
					removed_data.update(
						{target:the_thing.pop(target)}
					)

			if has_stuff_to_add:
				the_thing.update(data_to_add)

			if isolated:
				cur.execute(_SQL_TX_BEGIN)

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
				# con_or_cur.commit()
				cur.execute(_SQL_TX_COMMIT)
				cur.close()

			if return_val:
				return removed_data

			return True

	# the_query="INSERT"
	# if force:
	# 	the_query=f"{the_query} OR REPLACE"
	# the_query=(
	# 	f"{the_query} INTO {_SQL_TAB_ITEMS} "
	# 	"VALUES(?,?,?,?,?)"
	# )

	cur.execute(
		util_bquery_insert(
			replace=force,
			show=verbose
		),
		util_bparams(
			key_ready,data_to_add,
			_TYPE_HASHMAP
		)
		# (
		# 	key_ready,
		# 	_TYPE_HASHMAP,
		# 	None,
		# 	None,
		# 	pckl_encode(data_to_add)
		# )
	)
	if isolated:
		# con_or_cur.commit()
		cur.execute(_SQL_TX_COMMIT)
		cur.close()

	return True

def db_hget(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,
		subkeys:list=[],
		display_results:bool=False,
		verbose:bool=False,
	)->Mapping:

	if len(subkeys)==0:
		return {}

	key_ready=key_name.strip().lower()
	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)

	cur.execute(
		util_bquery_select(
			keyname=key_ready,
			datatype=_TYPE_HASHMAP
		)
	)
	result=cur.fetchone()
	if result is None:
		if verbose:
			print(
				f"{db_hget.__name__}[{key_ready}]",
				"not found"
			)
		if isolated:
			cur.close()

		return {}

	the_thing:Mapping=pckl_decode(result[0])

	selection={}

	for key in subkeys:
		if key not in the_thing.keys():
			continue
		selection.update(
			{key:the_thing.pop(key)}
		)

	if display_results:
		print(
			f"{db_hget.__name__}[{key_ready}]{subkeys} =",
			selection
		)
		return {}

	return selection

def db_custom(
		con_or_cur:Union[SQLConnection,SQLCursor],
		key_name:str,
		custom_func:Callable,
		custom_func_params:Optional[Any],
		res_write:bool=False,
		res_return:bool=False,
		verbose:bool=False
	)->Optional[Any]:

	display_result=(
		not (
			res_write or
			res_return
		)
	)

	key_ready=key_name.strip().lower()
	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur,verbose=verbose)

	cur.execute(
		util_bquery_select(
			keyname=key_ready
		)
	)

	select_result=cur.fetchone()
	if select_result is None:
		if verbose:
			print(
				f"{db_custom.__name__}[{key_ready}]",
				"Not found"
			)
		if isolated:
			cur.close()

		return None

	the_value=util_extract_correct_value(select_result)
	if the_value is None:
		if verbose:
			print(
				f"{db_custom.__name__}[{key_ready}]",
				"the extracted value is corrupt"
			)
		if isolated:
			cur.close()

		return None

	# dtype_orig=util_get_dtype_from_value(the_value)

	if isolated and res_write:
		cur.execute(_SQL_TX_BEGIN)

	func_result:Optional[Any]=None
	has_args=(custom_func_params is not None)
	if has_args:
		func_result=custom_func(the_value,custom_func_params)
	if not has_args:
		func_result=custom_func(the_value)

	print("func_result =",func_result)

	if display_result:
		print(
			f"{db_custom.__name__}",
			f"{custom_func.__name__}([{custom_func_params}])",
			func_result
		)

	if res_write:

		dtype_new=util_get_dtype_from_value(func_result)

		# NOTE: UPDATE has an issue, replacing the full row is safer

		exe_params:Optional[tuple]=util_bparams(
			key_ready,func_result,dtype_new
		)
		cur.execute(
			util_bquery_insert(replace=True),
			exe_params
		)

		if isolated:
			cur.execute(_SQL_TX_COMMIT)
			cur.close()

	if res_return:

		return func_result

	return None

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

def db_fz_str(
		con_or_cur:Union[SQLConnection,SQLCursor],
		substring:str,
		starts_with:bool=False,
		display_results:bool=False,
	)->list:

	text_ok=substring.strip()

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
			row[1],
			starts_with
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
		print(
			db_fz_str.__name__,
			text_ok,
			final_list
		)

	return final_list

def db_fz_num(
		con_or_cur:Union[SQLConnection,SQLCursor],
		target:Union[int,tuple],
		sort_results:int=0,
		display_results:bool=False
	)->Optional[list]:

	target_ok=util_extract_from_target_tuple(target)
	if target_ok is None:
		# if verbose:
		# 	print(f"{db_fz_num.__name__}[{key_ready}][{target}] the given target is not valid")

		if isinstance(target,tuple):
			return []

		return None

	select_one=(target_ok[0]==_TARGET_ONE)
	select_slice=(target_ok[0]==_TARGET_SLICE)

	query=(
		f"SELECT {_SQL_COL_KEY},{_SQL_COL_VALUE_INT} FROM {_SQL_TAB_ITEMS} "
			f"WHERE {_SQL_COL_TYPE}={_TYPE_INT}"
	)
	if select_one:
		query=f"{query} AND {_SQL_COL_VALUE_INT}={target_ok[1]}"

	if select_slice:

		idx_min_ok=target_ok[1]
		idx_max_ok=target_ok[2]
		idx_min=target_ok[3]
		idx_max=target_ok[4]

		if idx_min_ok:
			query=(
				f"{query} AND ("
					f"{_SQL_COL_VALUE_INT}={idx_min} OR "
					f"{_SQL_COL_VALUE_INT}>{idx_min}"
				")"
			)

		if idx_max_ok:
			query=(
				f"{query} AND ("
					f"{_SQL_COL_VALUE_INT}={idx_max} OR "
					f"{_SQL_COL_VALUE_INT}<{idx_max}"
				")"
			)

	if sort_results in (_SORT_LOW_TO_HI,_SORT_HI_TO_LOW):
		query=f"{query} ORDER BY {_SQL_COL_VALUE_INT}"
		if sort_results==_SORT_LOW_TO_HI:
			query=f"{query} ASC"
		if sort_results==_SORT_HI_TO_LOW:
			query=f"{query} DESC"

	isolated=isinstance(con_or_cur,SQLConnection)
	cur=db_getcur(con_or_cur)
	cur.execute(query.strip())

	if display_results:
		print(
			db_fz_num.__name__,
			f"[{target}]"
		)
		for row in cur:
			print(f"\t{row}")

		return None

	results=cur.fetchall()

	if isolated:
		cur.close()

	return results

# Transaction functions

def db_tx_begin(
		cur:SQLCursor,
		verbose:bool=False
	)->bool:

	if not db_check_changes(cur,verbose=verbose):
		return False

	if verbose:
		print(
			db_tx_begin.__name__,
			"begginning transaction"
		)

	cur.execute("BEGIN TRANSACTION")

	return True

def db_tx_commit(
		cur:SQLCursor,
		close_cursor:bool=False,
		verbose:bool=False
	)->bool:

	if not db_check_changes(cur,verbose=verbose):
		return False

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

	return True

def db_tx_rollback(
		cur:SQLCursor,
		close_cursor:bool=False,
		verbose:bool=False
	)->bool:

	if not db_check_changes(cur,verbose=verbose):
		return False

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

	return True

# Class object with ALL the functions

class DBControl:

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

	def db_tx_begin(self)->bool:

		if self.cur is not None:
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

		if not db_tx_commit(
			self.cur,
			close_cursor=True,
			verbose=self.verbose
		):
			return False

		self.cur=None

		return True

	def db_tx_rollback(self)->bool:

		if self.cur is not None:
			return False

		if not db_tx_rollback(
			self.cur,
			close_cursor=True,
			verbose=self.verbose
		):
			return False

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
				return_val=retval,
				verbose=self.verbose
			)
		return db_delete(
			self.con,
			keyname,
			return_val=retval,
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
				return_val=retval,
				verbose=self.verbose
			)
		return db_ldelete(
			self.con,
			keyname,target,
			return_val=retval,
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
				return_val=retval,
				verbose=self.verbose
			)
		return db_hupdate(
			self.con,
			keyname,
			new,remove,
			force=force,
			return_val=retval,
			verbose=self.verbose
		)

	def db_hget(
			self,
			keyname:str,
			subkeys:list=[]
		)->Mapping:

		if len(subkeys)==0:
			return {}

		if self.cur is not None:
			return db_hget(
				self.cur,
				keyname,subkeys=subkeys,
				verbose=self.verbose
			)
		return db_hget(
			self.con,
			keyname,subkeys=subkeys,
			verbose=self.verbose
		)

	def db_custom(
		self,
		keyname:str,
		custom_func:Callable,
		custom_func_params:Optional[Any]=None,
		res_write:bool=False,
		res_return:bool=False
	)->Optional[Any]:

		if self.cur is not None:

			return db_custom(
				self.cur,
				keyname,
				custom_func,
				custom_func_params=custom_func_params,
				res_write=res_write,
				res_return=res_return,
				verbose=self.verbose
			)

		return db_custom(
			self.con,
			keyname,
			custom_func,
			custom_func_params=custom_func_params,
			res_write=res_write,
			res_return=res_return,
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

	def db_fz_str(
			self,
			sub_str:str,
			starts_with:bool=False
		)->list:

		if self.cur is not None:
			return db_fz_str(
				self.cur,
				sub_str,
				starts_with=starts_with,
			)
		return db_fz_str(
			self.con,
			sub_str,
			starts_with=starts_with,
		)

	def db_fz_num(
		self,
		target:Union[tuple],
		sort_results:bool=_SORT_NONE
	)->list:

		if self.cur is not None:
			return db_fz_num(
				self.cur,target,
				sort_results=sort_results
			)

		return db_fz_num(
			self.con,target,
			sort_results=sort_results
		)

	def __enter__(self):

		self.dbg_msg("opening context manager")

		self.as_cm=True

		return self

	def __exit__(self,exc_type,exc_value,exc_traceback):

		if not self.as_cm:
			self.dbg_msg("NOTE: This method is not meant to run outside the context manager")
			raise Exception("WTF man")

		con_has_tx=db_check_changes(self.cur)

		self.dbg_msg("closing context manager")

		if exc_type is None:

			self.db_tx_commit()

			if con_has_tx:
				self.con.commit()
				self.con.close()

			# # if has_cursor and has_tx:
			# # 	self.dbg_msg("committing changes to the pending transaction on the cursor")
			# # 	self.cur.execute("COMMIT;")
			# # 	self.cur.close()
			# # 	has_tx=self.con.in_transaction

			# if has_tx:
			# 	self.dbg_msg("committing changes to ALL pending transactions from this connection")
			# 	self.con.commit()

		if exc_type is not None:

			self.db_tx_rollback()

			if con_has_tx:
				self.con.commit()
				self.con.close()

		# 	if has_cursor and has_tx:
		# 		self.dbg_msg("discarding all pending changes")
		# 		self.cur.execute("ROLLBACK;")

		# if has_cursor:
		# 	self.dbg_msg("closing the cursor before closing the connection")
		# 	self.cur.close()

		# self.dbg_msg("closing the connection")
		# self.con.close()

	def close(self,rollback:bool=False)->bool:

		# NOTE: Rollback only works if the cursor is up and it has a pending transaction

		con_has_tx=db_check_changes(self.cur)

		if not rollback:

			self.db_tx_commit()

			if con_has_tx:
				self.con.commit()
				self.con.close()

		if rollback:

			self.db_tx_rollback()

			if con_has_tx:
				self.con.commit()
				self.con.close()

		# if self.as_cm:
		# 	self.dbg_msg("NOTE: This method is not meant to run inside a context manager")
		# 	return False

		# has_cursor=(self.cur is not None)
		# has_tx=self.con.in_transaction

		# self.dbg_msg(f"closing the object; has_cursor = {has_cursor}; has_tx = {has_tx}")

		# if not rollback:

		# 	if has_cursor and has_tx:
		# 		self.dbg_msg("committing changes to the pending transaction on the cursor")
		# 		self.cur.execute("COMMIT;")
		# 		self.cur.close()
		# 		has_tx=self.con.in_transaction

		# 	if has_tx:
		# 		self.dbg_msg("committing changes to ALL pending transactions from this connection")
		# 		self.con.commit()

		# if rollback:

		# 	if has_cursor and has_tx:
		# 		self.dbg_msg("discarding all pending changes")
		# 		self.cur.execute("ROLLBACK;")

		# if has_cursor:
		# 	self.dbg_msg("closing the cursor before closing the connection")
		# 	self.cur.close()

		# self.dbg_msg("closing the connection")
		# self.con.close()

		# return True

# Transaction object

class DBTransaction:

	rollback:bool=False
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
			return_val=retval,
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
			return_val=retval,
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
			return_val=retval,
			verbose=self.verbose
		)

	def db_hget(
			self,
			keyname:str,
			subkeys:list=[]
		)->Mapping:

		if len(subkeys)==0:
			return {}

		return db_hget(
			self.cur,
			keyname,
			subkeys=subkeys,
			verbose=self.verbose
		)

	def db_custom(
		self,
		keyname:str,
		custom_func:Callable,
		custom_func_params:Any,
		res_write:bool=False,
		res_return:bool=False
	)->Optional[Any]:

		return db_custom(
			self.cur,
			keyname,
			custom_func,
			custom_func_params=custom_func_params,
			res_write=res_write,
			res_return=res_return,
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

	def db_fz_str(
			self,
			sub_str:str,
			starts_with:bool=False
		)->list:

		return db_fz_str(
			self.cur,
			sub_str,
			starts_with=starts_with,
		)

	def db_fz_num(
		self,
		target:Union[tuple],
		sort_results:bool=_SORT_NONE
	)->list:

		return db_fz_num(
			self.cur,target,
			sort_results=sort_results
		)

	def db_req_rollback(self):

		# Requests a rollback so that at the end of the context
		# manager
		# All pending transactions will be dropped when the context manager closes

		if not self.rollback:
			self.dbg_msg("rollback requested")
			self.rollback=True

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

		if (exc_type is not None) or self.rollback:
			self.dbg_msg("discarding all pending changes")
			self.cur.execute("ROLLBACK")

		self.dbg_msg("closing the cursor")
		self.cur.close()

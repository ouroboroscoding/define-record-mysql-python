# coding=utf8
"""MySQL Server

Provides methods to access the MySQL / MariaDB server(s) directly
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-01"

# Limit imports
__all__ = [
	'add_host', 'db_create', 'db_drop', 'escape', 'execute', 'insert', 'select',
	'timestamp_timezone', 'verbose', 'uuid'
]

# Python imports
from enum import IntEnum
import re
from time import sleep

# Pip imports
import arrow
from record.exceptions import RecordDuplicate, RecordServerException
import pymysql

# List of available hosts
__hosts = {}

# List of available connection
__connections = {}

# The offset used to calculate timestamps
__timestamp_timezone = '+00:00'

# Verbose mode
__verbose = False

# constants
MAX_RETRIES = 3
DUP_ENTRY_REGEX = re.compile('Duplicate entry \'(.*?)\' for key \'(.*?)\'')

class Select(IntEnum):
	"""Select

	An enum for setting the type of data to return from a select() call
	"""
	ALL			= 1
	CELL		= 2
	COLUMN		= 3
	HASH		= 4
	HASH_ROWS	= 5
	ROW			= 6

def _clear_connection(host: str) -> None:
	"""Clear Connection

	Handles removing a connection from the module list

	Arguments:
		host (str): The host to clear

	Returns:
		None
	"""

	# If we have the connection
	if host in __connections:

		# Try to close the connection
		try:
			__connections[host].close()

			# Sleep for a second
			sleep(1)

		# Catch any exception
		except Exception as e:
			print('\n----------------------------------------')
			print('Unknown exception in record_mysql.server._clear_connection')
			print('host = %s' % str(host))
			print('exception = %s' % str(e.__class__.__name__))
			print('args = %s' % ', '.join([str(s) for s in e.args]))

		# Delete the connection
		del __connections[host]

def _connection(host: str, errcnt: int = 0) -> pymysql.Connection:
	"""Connection

	Returns a connection to the given host

	Arguments:
		host (str): The name of the host to connect to
		errcnt (uint): The current error count

	Returns:
		pymysql.Connection
	"""

	# If we already have the connection, return it
	if host in __connections:
		return __connections[host]

	# If no such host has been added
	if host not in __hosts:
		raise RecordServerException('no such host "%s"' % str(host))

	# Get the config
	dConf = __hosts[host]

	# Create a new connection
	try:
		oCon = pymysql.connect(**__hosts[host])

		# Turn autocommit on
		oCon.autocommit(False)

		# Change conversions
		conv = oCon.decoders.copy()
		for k in conv:
			if k in [7]: conv[k] = _converter_timestamp
			elif k in [10,11,12]: conv[k] = str
		oCon.decoders = conv

	# Check for errors
	except pymysql.err.OperationalError as e:

		# Increment the error count
		errcnt += 1

		# If we've hit our max errors, raise an exception
		if errcnt == MAX_RETRIES:
			raise ConnectionError(*e.args)

		# Else just sleep for a second and try again
		else:
			sleep(1)
			return _connection(host, errcnt)

	# Store the connection and return it
	__connections[host] = oCon
	return oCon

def _converter_timestamp(ts: str) -> int:
	"""Converter Timestamp

	Converts timestamps received from MySQL into proper integers

	Arguments:
		ts (str): The timestamp to convert

	Returns:
		uint
	"""

	# If there is no time
	if ts == '0000-00-00 00:00:00':
		return 0

	# Replace ' ' with 'T', add milliseconds, and then timezone
	ts = '%s.000000%s' % (
		ts.replace(' ', 'T'),
		__timestamp_timezone
	)

	# Conver the string to a timestamp and return it
	return arrow.get(ts).int_timestamp

def _cursor(host: str, dict_cur: bool = False):
	"""Cursor

	Returns a cursor for the given host

	Arguments:
		host (str): The name of the host
		dict_cur (bool): If true, cursor will use dicts

	Returns:
		Cursor
	"""

	# Get a connection to the host
	oCon = _connection(host)

	# Try to get a cursor on the connection
	try:

		# Start the transaction
		oCon.begin()

		if dict_cur:
			oCursor = oCon.cursor(pymysql.cursors.DictCursor)
		else:
			oCursor = oCon.cursor()

		# Make sure we're on the requested charset
		oCursor.execute('SET NAMES %s' % __hosts[host]['charset'])

	# If there's any exception whatsoever
	except :

		# Clear the connection and try again
		_clear_connection(host)
		return _cursor(host, dict_cur)

	# Return the connection and cursor
	return [oCon, oCursor]

def _print_sql(sql: str, type: str, host: str = '_'):
	"""Print SQL

	Print out a message with host and SQL information

	Useful for debugging problems

	Arguments:
		sql (str): The SQL to print
		type (str): The type of statment
		host (str): The host the statement will be run on

	Returns
		None
	"""
	print('----------------------------------------\n%s - %s - %s\n\n%s\n' % (
		host,
		type,
		arrow.get().format('YYYY-MM-DD HH:mm:ss'),
		sql
	))

class _wcursor(object):
	"""_with

	Used with the special Python with method to create a connection that will \
	always be closed regardless of exceptions
	"""

	def __init__(self, host: str, dict_cur: bool = False):
		self.con, self.cursor = _cursor(host, dict_cur)

	def __enter__(self):
		return self.cursor

	def __exit__(self, exc_type, exc_value, traceback):
		self.cursor.close()
		if exc_type is None:
			self.con.commit()
		else:
			return False

def add_host(info: dict, name: str = '_', update: bool = False) -> bool:
	"""Add Host

	Add a host that can be used by Records. By default, add_host ignores any \
	requests to add a host that's already been added. To intentionally set new \
	data for an existing host, set the `update` parameter to True

	Arguments:
		info (dict): The necessary credentials to connect to the host
		name (str): The name that will be used to fetch the host credentials
		update (bool): Optional, set to true to overwrite any existing data

	Returns:
		bool
	"""

	# If the info isn't already stored, or we want to overwrite it
	if name not in __hosts or update:

		# Add default charset if it wasn't passed
		if 'charset' not in info:
			info['charset'] = 'utf8'

		# Store the info
		__hosts[name] = info

		# Return OK
		return True

	# Nothing to do, not OK
	return False

def db_create(
	name: str,
	host: str = '_',
	charset: str = None,
	collate: str = None
):
	"""DB Create

	Creates a DB on the given host

	Arguments:
		name (str): The name of the DB to create
		host (str): Optional, the name of the host the DB will be on
		charset (str): Optional default charset
		collate (str): Optional default collate, charset must be set to use

	Returns:
		bool
	"""

	# Generate the statement
	sSQL = 'CREATE DATABASE IF NOT EXISTS `%s`' % name
	if charset:
		sSQL += ' DEFAULT CHARACTER SET %s' % charset
		if collate:
			sSQL += ' COLLATE %s' % collate

	# Create the DB
	execute(sSQL, host)
	return True

def db_drop(name: str, host: str = '_'):
	"""DB Drop

	Drops a DB on the given host

	Arguments:
		name (str): The name of the DB to delete
		host (str): Optional, the name of the host the DB is on

	Returns:
		bool
	"""

	# Delete the DB
	execute("DROP DATABASE IF EXISTS `%s`" % name, host)
	return True

def escape(value: str, host: str = '_'):
	"""Escape

	Used to escape string values for the DB

	Arguments:
		value (str): The value to escape
		host (str): Optional, the name of the connection to escape for

	Returns:
		str
	"""

	# Get a connection to the host
	oCon = _connection(host)

	# Get the value
	try:
		sRet = oCon.escape_string(value)

	# Else there's an operational problem so close the connection and
	#	restart
	except pymysql.err.OperationalError as e:

		# Clear the connection and try again
		_clear_connection(host)
		return escape(value, host)

	except Exception as e:
		print('\n----------------------------------------')
		print('Unknown Error in record_mysql.server.escape')
		print('host = %s' % host)
		print('value = %s' % str(value))
		print('exception = %s' % str(e.__class__.__name__))
		print('args = %s' % ', '.join([str(s) for s in e.args]))

		# Rethrow
		raise e

	# Return the escaped string
	return sRet

def execute(sql: str | list, host: str = '_', errcnt: int = 0) -> int:
	"""Execute

	Used to run SQL that doesn't return any rows

	Arguments:
		sql (str|tuple): The SQL (or SQL plus a list) statement to run
		host (str): Optional, the name of the connection to execute on

	Returns:
		uint
	"""

	# Print debug if requested
	if __verbose: _print_sql(sql, 'EXECUTE', host)

	# Fetch a cursor
	with _wcursor(host) as oCursor:

		try:

			# If we got a str
			if isinstance(sql, str):
				return oCursor.execute(sql)

			# Init the return
			iRet = 0

			# Go through each statment and execute it
			for s in sql:
				iRet += oCursor.execute(s)

			# Return the changed rows
			return iRet

		# If the SQL is bad
		except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as e:

			# Raise a Value Exception
			raise RecordServerException(
				e.args[0],
				'SQL error (%s): %s\n%s' % (
					str(e.args[0]), str(e.args[1]), str(sql)
				)
			)

		# Else, a duplicate key error
		except pymysql.err.IntegrityError as e:

			# Pull out the value and the index name
			oMatch = DUP_ENTRY_REGEX.match(e.args[1])

			# If we got a match
			if oMatch:

				# Raise a Duplicate Record Exception
				raise RecordDuplicate(oMatch.group(1), oMatch.group(2))

			# Else, raise an unkown duplicate
			raise RecordDuplicate(e.args[0], e.args[1])

		# Else there's an operational problem so close the connection and
		#	restart
		except pymysql.err.OperationalError as e:
			print('----------------------------------------')
			print('OPERATIONAL ERROR')
			print(e.args)
			print('')

			# If the error code is one that won't change
			if e.args[0] in [1051, 1054, 1136]:
				raise RecordServerException(
					e.args[0],
					'SQL error (%s): %s\n%s' % (
						str(e.args[0]), str(e.args[1]), str(sql)
					)
				)

			# Increment the error count
			errcnt += 1

			# If we've hit our max errors, raise an exception
			if errcnt == MAX_RETRIES:
				raise ConnectionError(*e.args)

			# Clear the connection and try again
			_clear_connection(host)
			return execute(sql, host, errcnt)

		# Else, catch any Exception
		except Exception as e:
			print('\n----------------------------------------')
			print('Unknown Error in record_mysql.server.execute')
			print('host = %s' % host)
			print('sql = %s' % str(sql))
			print('exception = %s' % str(e.__class__.__name__))
			print('args = %s' % ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

def insert(sql: str, host: str = '_', errcnt: int = 0):
	"""Insert

	Handles INSERT statements and returns the new ID. To insert records \
	without auto_increment it's best to just stick to execute()

	Args:
		sql (str): The SQL statement to run
		host (str): Optional, the name of the connection to insert on

	Returns:
		any
	"""

	# Print debug if requested
	if __verbose: _print_sql(sql, 'INSERT', host)

	# Fetch a cursor
	with _wcursor(host) as oCursor:

		try:

			# If the sql arg is a tuple we've been passed a string with a list
			#	for the purposes of replacing parameters
			if isinstance(sql, tuple):
				oCursor.execute(sql[0], sql[1])
			else:
				oCursor.execute(sql)

			# Get the ID
			mInsertID = oCursor.lastrowid

			# Return the last inserted ID
			return mInsertID

		# If the SQL is bad
		except pymysql.err.ProgrammingError as e:

			# Raise an SQL Exception
			raise RecordServerException(
				e.args[0],
				'SQL error (%s): %s\n%s' % (
					str(e.args[0]), str(e.args[1]), str(sql)
				)
			)

		# Else, a duplicate key error
		except pymysql.err.IntegrityError as e:

			# Pull out the value and the index name
			oMatch = DUP_ENTRY_REGEX.match(e.args[1])

			# If we got a match
			if oMatch:

				# Raise a Duplicate Record Exception
				raise RecordDuplicate(oMatch.group(1), oMatch.group(2))

			# Else, raise an unkown duplicate
			raise RecordDuplicate(e.args[0], e.args[1])

		# Else there's an operational problem so close the connection and
		#	restart
		except pymysql.err.OperationalError as e:

			# If the error code is one that won't change
			if e.args[0] in [1054]:
				raise RecordServerException(
				e.args[0],
				'SQL error (%s): %s\n%s' % (
					str(e.args[0]), str(e.args[1]), str(sql)
				)
			)

			# Increment the error count
			errcnt += 1

			# If we've hit our max errors, raise an exception
			if errcnt == MAX_RETRIES:
				raise ConnectionError(*e.args)

			# Clear the connection and try again
			_clear_connection(host)
			return insert(sql, host, errcnt)

		# Else, catch any Exception
		except Exception as e:
			print('\n----------------------------------------')
			print('Unknown Error in record_mysql.server.insert')
			print('host = %s' % host)
			print('sql = %s' % str(sql))
			print('exception = %s' % str(e.__class__.__name__))
			print('args = %s' % ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

def select(
	sql: str,
	seltype: Select = Select.ALL,
	field: str = None,
	host: str = '_',
	errcnt: int = 0
):
	"""Select

	Handles SELECT queries and returns the data

	Arguments:
		sql (str): The SQL statement to run
		seltype (ESelect): The format to return the data in
		field (str): Only used by HASH_ROWS since MySQLdb has no ordereddict \
			for associative rows
		host (str): Optional, the name of the host to select from

	Returns:
		mixed
	"""

	# Print debug if requested
	if __verbose: _print_sql(sql, 'SELECT', host)

	# Get a cursor
	bDictCursor = seltype in (Select.ALL, Select.HASH_ROWS, Select.ROW)

	# Fetch a cursor
	with _wcursor(host, bDictCursor) as oCursor:

		try:

			# If the sql arg is a tuple we've been passed a string with a list
			#	for the purposes of replacing parameters
			if isinstance(sql, tuple):
				oCursor.execute(sql[0], sql[1])
			else:
				oCursor.execute(sql)

			# If we want all rows
			if seltype == Select.ALL:
				mData = list(oCursor.fetchall())

			# If we want the first cell 0,0
			elif seltype == Select.CELL:
				mData = oCursor.fetchone()
				if mData != None:
					mData = mData[0]

			# If we want a list of one field
			elif seltype == Select.COLUMN:
				mData = []
				mTemp = oCursor.fetchall()
				for i in mTemp:
					mData.append(i[0])

			# If we want a hash of the first field and the second
			elif seltype == Select.HASH:
				mData = {}
				mTemp = oCursor.fetchall()
				for n,v in mTemp:
					mData[n] = v

			# If we want a hash of the first field and the entire row
			elif seltype == Select.HASH_ROWS:
				# If the field arg wasn't set
				if field == None:
					raise RecordServerException(
						'Must specificy a field for the dictionary key when ' \
						'using HASH_ROWS'
					)

				mData = {}
				mTemp = oCursor.fetchall()

				for o in mTemp:
					# Store the entire row under the key
					mData[o[field]] = o

			# If we want just the first row
			elif seltype == Select.ROW:
				mData = oCursor.fetchone()

			# Return the results
			return mData

		# If the SQL is bad
		except pymysql.err.ProgrammingError as e:

			# Raise an SQL Exception
			raise RecordServerException(
				e.args[0],
				'SQL error (%s): %s\n%s' % (
					str(e.args[0]), str(e.args[1]), str(sql)
				)
			)

		# Else there's an operational problem so close the connection and
		#	restart
		except pymysql.err.OperationalError as e:

			# If the error code is one that won't change
			if e.args[0] in [1054]:
				raise RecordServerException(
				e.args[0],
				'SQL error (%s): %s\n%s' % (
					str(e.args[0]), str(e.args[1]), str(sql)
				)
			)

			# Increment the error count
			errcnt += 1

			# If we've hit our max errors, raise an exception
			if errcnt == MAX_RETRIES:
				raise ConnectionError(*e.args)

			# Clear the connection and try again
			_clear_connection(host)
			return select(sql, seltype, field, host, errcnt)

		# Else, catch any Exception
		except Exception as e:
			print('\n----------------------------------------')
			print('Unknown Error in record_mysql.server.select')
			print('host = %s' % host)
			print('sql = %s' % str(sql))
			print('exception = %s' % str(e.__class__.__name__))
			print('args = %s' % ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

def timestamp_timezone(s: str) -> None:
	"""Timestamp Offset

	Used to deal with dumb mysql servers that return timestamps as a string in \
	the system's local time

	Arguments:
		s (str): The timezone offset

	Returns
		None
	"""
	global __timestamp_timezone
	__timestamp_timezone = s

def verbose(set_: bool = None) -> bool | None:
	"""Verbose

	Sets/Gets the debug flag

	Arguments:
		set_ (bool | None): Ignore to get the current value

	Returns
		bool | None
	"""
	global __verbose
	if set_ is None:
		return __verbose
	else:
		__verbose = set_

def uuid(host: str = '_') -> str:
	"""UUID

	Returns a universal unique ID

	Arguments:
		None

	Returns:
		str
	"""

	# Get the UUID
	return select('select uuid()', Select.CELL, host=host)
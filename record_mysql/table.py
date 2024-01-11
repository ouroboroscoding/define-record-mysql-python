# coding=utf8
"""Record Table

Handles a single SQL table and all that's required to interact with it
"""
from __future__ import annotations

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-01"

# Limit exports
__all__ = ['escape', 'Table']

# Ouroboros imports
from define import Node
from jobject import jobject
import jsonb
from tools import merge
import undefined

# Python imports
import re
from typing import Literal as LT

# Local imports
from record_mysql import server, transaction

_node_to_sql = {
	'any': False,
	'base64': False,
	'bool': 'tinyint(1) unsigned',
	'date': 'date',
	'datetime': 'datetime',
	'decimal': 'decimal',
	'float': 'double',
	'int': 'integer',
	'ip': 'char(15)',
	'json': 'text',
	'md5': 'char(32)',
	'price': 'decimal(8,2)',
	'string': False,
	'time': 'time',
	'timestamp': 'timestamp',
	'uint': 'integer unsigned',
	'uuid': 'char(36)',
	'uuid4': 'char(36)'
}
"""Node To SQL

Used as default values for define Node types to SQL data types
"""

BOOL_CONVERT = 0
JSON_CONVERT = 1
"""Constants"""

DIGITS = re.compile(r'^\d+$')
"""Digits

A regular expression to match a string that only contains digits"""

def _node_to_type(node: Node, host: str) -> str:
	"""Node To Type

	Converts the Node type to a valid MySQL field type

	Arguments:
		node (define.Node): The node we need an SQL type for
		host (str): The host in case we need to escape anything

	Raises:
		ValueError

	Returns:
		str
	"""

	# Get the node's class
	sClass = node.class_name()

	# If it's a regular node
	if sClass == 'Node':

		# Get the node's type
		sType = node.type()

		# Can't use any in MySQL
		if sType == 'any':
			raise ValueError('"any" nodes can not be used in record_mysql')

		# If the type is a string
		elif sType in ['base64', 'string']:

			# If we have options
			lOptions = node.options()
			if not lOptions is None:

				# Create an enum
				return 'enum(%s)' % (','.join([
					escape(node, s, host)
					for s in lOptions
				]))

			# Else, need maximum
			else:

				# Get min/max values
				dMinMax = node.minmax()

				# If we have don't have a maximum
				if dMinMax['maximum'] is None:
					raise ValueError(
						'"string" nodes must have a __maximum__ value if ' \
						'__sql__.type is not set'
					)

				# If the minimum matches the maximum
				if dMinMax['minimum'] == dMinMax['maximum']:

					# It's a char as all characters must be filled
					return 'char(%d)' % dMinMax['maximum']

				else:

					# long text
					if dMinMax['maximum'] == 4294967295:
						return 'longtext'
					elif dMinMax['maximum'] == 16777215:
						return 'mediumtext'
					elif dMinMax['maximum'] == 65535:
						return 'text'
					else:
						return 'varchar(%d)' % dMinMax['maximum']

		# Else, get the default
		elif sType in _node_to_sql:
			return _node_to_sql[sType]

		# Else
		else:
			raise ValueError(
				'"%s" is not a known type to record_mysql.table' % sType
			)

	# Else, any other type isn't implemented
	else:
		raise TypeError(
			'record_mysql.table can not process define %s nodes' % sClass
		)

def escape(node: Node, value: any, host = '_'):
	"""Escape

	Takes a value and turns it into an acceptable string for SQL

	Arguments:
		node (define.Node): The node associated with the data to escape
		value (any): The value to escape
		host (str): Optional, the name of the host if we need to call the server

	Returns:
		str
	"""

	# If it's a literal
	if isinstance(value, Literal):
		return value.get()

	elif value is None:
		return 'NULL'

	else:

		# Get the Node's class
		sClass = node.class_name()

		# If it's a standard Node
		if sClass == 'Node':

			# Get the type
			type_ = node.type()

			# If we're escaping a bool
			if type_ == 'bool':

				# If it's already a bool or a valid int representation
				if isinstance(value, bool) or \
					(isinstance(value, int) and value in [0,1]):
					return (value and '1' or '0')

				# Else if it's a string
				elif isinstance(value, str):

					# If it's a generally accepted string value of true, else
					#	false
					return (value in (
						'true', 'True', 'TRUE', 't', 'T',
						'x', 'X',
						'yes', 'Yes', 'YES', 'y', 'Y',
						'1') and '1' or '0')

			# Else if it's a date, md5, or UUID, return as is
			elif type_ in ('base64', 'date', 'datetime', 'md5', 'time', 'uuid',
							'uuid4'):
				return "'%s'" % value

			# Else if the value is a decimal value
			elif type_ in ('decimal', 'float', 'price'):
				return str(float(value))

			# Else if the value is an integer value
			elif type_ in ('int', 'uint'):
				return str(int(value))

			# Else if it's a timestamp
			elif type_ == 'timestamp' and \
				(isinstance(value, int) or DIGITS.match(value)):
				return 'FROM_UNIXTIME(%s)' % str(value)

			# Else it's a standard escape
			else:
				return "'%s'" % server.escape(value, host)

		# Else, any other type isn't implemented
		else:
			raise TypeError(
				'record_mysql.table can not process define %s nodes' % sClass
			)

class Func(object):
	"""Func

	Used as a field that won't be escaped or parsed
	"""

	def __init__(self, name: str, params: str):
		if not isinstance(name, str):
			raise ValueError('`name` in Func must be a string')
		if not isinstance(params, str):
			raise ValueError('`params` in Func must be a string')
		self._text = '%s(%s)' % (name, params)
	def __str__(self):
		return self._text
	def get(self):
		return self._text

class Literal(object):
	"""Literal

	Used as a value that won't be escaped or parsed
	"""

	def __init__(self, text: str):
		if not isinstance(text, str):
			raise ValueError('`text` in Literal must be a string')
		self._text = text
	def __str__(self):
		return self._text
	def get(self):
		return self._text

class Table(object):
	"""Table

	Represents a single SQL table and interacts with raw data in the form of \
	python dictionaries
	"""

	def __init__(self, struct: dict, columns: dict):
		"""Constructor

		Creates a new instance

		Arguments:
			struct (dict): Configuration details for building and accessing \
							the table
			columns (dict): The definitions for each column of the table

		Returns:
			Table
		"""

		# Generate the structure
		self._struct: jobject = merge(jobject({
			'auto_key': False,
			'db': 'db',
			'host': '_',
			'indexes': [],
			'key': False,
			'revisions': False,
			'name': None
		}), struct)

		# If the table name is missing
		if not isinstance(self._struct.name, str):
			raise KeyError('record_mysql.table.struct.name must be a str')

		# Store the columns
		self._columns: dict = columns

		# Keep track of fields that need to be converted
		self._convert: list[list[str]] = []

		# Clean up the indexes if there is any
		if self._struct.indexes:
			self._simple_indexes()

		# Step through each column
		for f in self._columns:

			# If it's a node
			if self._columns[f].class_name() == 'Node':

				# If it's a bool
				if self._columns[f].type() == 'bool':

					# Add it to the bool list
					self._convert.append([f, BOOL_CONVERT])

			# Else, it's complex
			else:

				# If it's not marked as JSON
				dMySQL = self._columns[f].special('mysql', default={})
				if 'json' not in dMySQL or not dMySQL['json']:
					raise ValueError(
						'record_mysql.table.%s must be flagged as JSON'
					)

				# Add it to the json list
				self._convert.append([f, JSON_CONVERT])

	def _simple_indexes(self):
		"""Simple Indexes

		Goes through all the indexes and makes sure they are uniform so that \
		any code using them is uniform
		"""

		# Make sure it's a dict
		if not isinstance(self._struct.indexes, dict):
			raise ValueError(
				'record_mysql.table.struct.indexes must be an Object ' \
				'(python dict type) not "%s"' % str(type(self._struct.indexes))
			)

		# Init the indexes
		dIndexes = {}

		# Loop through the pairs to get the data associated
		for sName, mValue in self._struct.indexes.items():

			# If it's None
			if mValue is None:
				dIndexes[sName] = {
					'fields': [ sName ],
					'type': 'INDEX'
				}

			# If the index is a string
			elif isinstance(mValue, str):
				dIndexes[sName] = {
					'fields': [ mValue ],
					'type': 'INDEX'
				}

			# If it's a list
			elif isinstance(mValue, list):
				dIndexes[sName] = {
					'fields': self._simple_indexes_list(mValue, sName),
					'type': 'INDEX'
				}

			# If it's a dictionary
			elif isinstance(mValue, dict):

				# Init the dict
				dIndexes[sName] = {}

				# If there's fields
				try:

					# If it's None
					if mValue.fields is None:
						dIndexes[sName]['fields'] = [ sName ]

					# If the index is a string
					elif isinstance(mValue.fields, str):
						dIndexes[sName]['fields'] =  [ mValue.fields ]

					# If it's a list
					elif isinstance(mValue.fields, list):
						dIndexes[sName]['fields'] = \
							self._simple_indexes_list(mValue.fields, sName)

				# Else, no fields, use the name as the only field
				except AttributeError:
					dIndexes[sName]['fields'] = [ sName ]

				# If we have a type
				if 'type' in mValue:
					dIndexes[sName]['type'] = mValue.type.upper()

		# Overwrite the old indexes
		self._struct.indexes = dIndexes

	@classmethod
	def _simple_indexes_list(cls, fields: list, name: str) -> list:
		"""Simple Indexes List

		Used to handle lists of fields which might be strings or dicts

		Arguments:
			fields (list): A list of strings or dicts
			name (str): The name of the index

		Raises:
			ValueError

		Returns:
			list
		"""

		# Init the list
		lRet = []

		# Go through each field
		for mField in fields:

			# If it's a str
			if isinstance(mField, str):
				lRet.append(mField)

			# Else, if it's a dict
			elif isinstance(mField, dict):

				# If we are missing a name
				if 'name' not in mField:
					raise ValueError(
						'record_mysql.table.struct.indexes.%s.fields[].' \
						'name is required' % name
					)

				# Add the name
				lRet.append(mField.name)

			# Else, invalid field
			else:
				raise ValueError(
					'record_mysql.table.struct.indexes.%s.' \
					'fields must be Strings or Objects' % name
				)

		# Return the fields found
		return lRet

	def create(self) -> bool:
		"""Create

		Creates the record's table in the database

		Arguments:
			None

		Returns:
			bool
		"""

		# If the 'create' value is missing
		if 'create' not in self._struct:

			# Get all the field names
			self._struct.create = list(self._columns.keys())

			# Order them alphabetically
			self._struct.create.sort()

		# Remove the primary key if it's in the 'create' list
		try: self._struct.create.remove(self._struct.key)
		except ValueError: pass

		# Get all child node keys
		lNodeKeys = self._columns.keys()
		lMissing = [
			s for s in lNodeKeys \
			if s not in self._struct.create and \
				s != self._struct.key
		]

		# If any are missing
		if lMissing:
			raise ValueError(
				'record_mysql.table._struct.create missing fields `%s` ' \
				'for `%s`.`%s`' % (
					'`, `'.join(lMissing),
					self._struct.db,
					self._struct.name
				)
			)

		# Generate the list of fields
		lFields = []
		for f in self._struct.create:

			# Get the sql special data
			try:
				dMySQL = self._columns[f].special('mysql', default={})
			except KeyError:
				raise ValueError(
					'record_mysql.table._struct.create contains an ' \
					'invalid field `%s` for `%s`.`%s`' % (
						f, self._struct.db, self._struct.name
					)
				)

			# If it's a string, store the value under 'type' in a new dict
			if isinstance(dMySQL, str):
				dMySQL = { 'type': dMySQL }

			# If the JSON flag is set, change the type to 'text'
			if 'json' in dMySQL and dMySQL['json']:
				dMySQL['type'] = 'text'

			# Add the line
			try:
				lFields.append('`%s` %s %s' % (
					f,
					('type' in dMySQL and dMySQL['type'] or \
						_node_to_type(self._columns[f], self._struct.host)
					),
					('opts' in dMySQL and dMySQL['opts'] or \
						(self._columns[f].optional() and 'null' or 'not null')
					)
				))
			except ValueError as e:
				raise ValueError(f, e.args[0])

		# Init the list of indexes
		lIndexes = []

		# If we have a primary key
		if self._struct.key:

			# Push the primary key to the front
			#	Get the sql special data
			dMySQL = self._columns[self._struct.key].special(
				'mysql', {}
			)

			# If it's a string, store the value under 'type' in a new dict
			if isinstance(dMySQL, str):
				dMySQL = { 'type': dMySQL }

			# Primary key type
			sIDType = 'type' in dMySQL and \
						dMySQL['type'] or \
						_node_to_type(
							self._columns[self._struct.key],
							self._struct.host
						)
			sIDOpts = 'opts' in dMySQL and dMySQL['opts'] or 'not null'

			# Add the line
			lFields.insert(0, '`%s` %s %s%s' % (
				self._struct.key,
				sIDType,
				(self._struct.auto_key is True and 'auto_increment ' or ''),
				sIDOpts
			))

			# Init the list of indexes
			lIndexes.append('primary key (`%s`)' % self._struct.key)

		# If there are indexes
		if self._struct.indexes:

			# Loop through the pairs to get the data associated
			for sName, mValue in self._struct.indexes.items():

				# Init the list of index fields
				lIndexFields = []

				# Go through each field in the list
				for mf in mValue.fields:

					# If it's a string, use it as is
					if isinstance(mf, str):
						lIndexFields.append('`%s`' % mf)

					# Else, if it's a dict
					elif isinstance(mf, dict):

						# If we are missing a name
						if 'name' not in mf:
							raise ValueError(
								'record_mysql.table.struct.indexes[].' \
								'fields[].name is required'
							)

						# If we have a order set
						if 'order' in mf:

							# If the order is invalid
							if mf.order.upper() not in ['ASC', 'DESC']:
								raise ValueError(
									'record_mysql.table._struct.' \
									'indexes[].fields[].order must ' \
									'be one of \'ASC\' | \'DESC\''
								)

							# Set the order
							sIndexFieldOrder = mf.order.upper()

						# Else, make it an ascending index
						else:
							sIndexFieldOrder = 'ASC'

						# If we have a size set
						if 'size' in mf:

							# If the size is invalid
							if not isinstance(mf.size, int):
								raise ValueError(
									'record_mysql.table._struct.' \
									'indexes[].fields[].size must be ' \
									'an int'
								)

							# Set the size
							sIndexFieldSize = '(%d)' % mf.size

						# Else, make it a simple index
						else:
							sIndexFieldSize = ''

						# Combine the parts into one index field
						lIndexFields.append('`%s`%s %s' % (
							mf.name,
							sIndexFieldSize,
							sIndexFieldOrder
						))

				# Join the fields together
				sIndexFields = ', '.join(lIndexFields)

				# If the type is invalid
				if mValue.type not in [
					'INDEX', 'UNIQUE', 'FULLTEXT', 'SPATIAL'
				]:
					raise ValueError(
						'record_mysql.table.struct.indexes[].type ' \
						'must be one of \'UNIQUE\' | \'FULLTEXT\' | ' \
						'\'SPATIAL\''
					)

				# Append the index
				lIndexes.append('%s `%s` (%s)' % (
					mValue.type, sName, sIndexFields
				))

		# Generate the CREATE statement
		sSQL = 'CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s, %s) '\
				'ENGINE=%s CHARSET=%s COLLATE=%s' % (
					self._struct.db,
					self._struct.name,
					', '.join(lFields),
					', '.join(lIndexes),
					'engine' in self._struct and \
						self._struct['engine'] or \
						'InnoDB',
					'charset' in self._struct and \
						self._struct['charset'] or \
						'utf8mb4',
					'collate' in self._struct and \
						self._struct['collate'] or \
						'utf8mb4_bin'
				)

		# Create the table
		server.execute(sSQL, self._struct.host)

		# If revisions are required
		if self._struct.revisions:

			# Generate the CREATE statement
			sSQL = 'CREATE TABLE IF NOT EXISTS `%s`.`%s_revisions` (' \
					'`%s` %s NOT NULL %s, ' \
					'`created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, ' \
					'`items` TEXT NOT NULL, ' \
					'INDEX `%s` (`%s`)) ' \
					'ENGINE=%s CHARSET=%s COLLATE=%s' % (
				self._struct.db,
				self._struct.name,
				self._struct.key,
				sIDType,
				sIDOpts,
				self._struct.key,
				self._struct.key,
				'engine' in self._struct and \
					self._struct.engine or 'InnoDB',
				'charset' in self._struct and \
					self._struct.charset or 'utf8mb4',
				'collate' in self._struct and \
					self._struct.collate or 'utf8mb4_bin'
			)

			# Create the table
			server.execute(sSQL, self._struct.host)

		# Return OK
		return True

	def _delete(self, where: dict = undefined) -> str:
		"""_delete

		Generates the actual DELETE SQL

		Returns:
			str
		"""

		# Init the where fields
		sWhere = None

		# If there's an additional where
		if where is not undefined:

			# Init the list of WHERE statements
			lWhere = []

			# Go through each filed/value pair in the where
			for f,v in where.items():

				# If the field doesn't exist
				if f not in self._columns:
					raise ValueError(
						'record_mysql.table.delete.where `%s` not a valid ' \
						'node' % f
					)

				# Generate the SQL and append it to the where list
				lWhere.append(
					'`%s` %s' % (f, self.process_value(f, v))
				)

			# Set the WHERE statment
			sWhere = 'WHERE %s' % ' AND '.join(lWhere)

		# Generate and return the SQL to update the field
		return 'DELETE FROM `%s`.`%s` ' \
				'%s' % (
			self._struct.db,
			self._struct.name,
			sWhere or ''
		)

	def delete(self, where: dict = undefined) -> int:
		"""Delete

		Deletes all or some records

		Arguments:
			where (dict): Optional, field/value pairs to decide what records \
				get deleted

		Returns:
			uint: number of records deleted
		"""

		# Delete the records and return the number of rows changed
		return server.execute(
			self._delete(where),
			self._struct.host
		)

	def drop(self) -> bool:
		"""Drop

		Deletes the record's table from the database

		Arguments:
			None

		Returns:
			bool
		"""

		# Generate the DROP statement
		sSQL = 'drop table `%s`.`%s`' % (
			self._struct.db,
			self._struct.name,
		)

		# Delete the table
		try:
			server.execute(sSQL, self._struct.host)
		except ValueError:
			return False

		# If revisions are required
		if self._struct.revisions:

			# Generate the DROP statement
			sSQL = 'drop table `%s`.`%s_revisions`' % (
				self._struct.db,
				self._struct.name,
			)

			# Delete the table
			server.execute(sSQL, self._struct.host)

		# Return OK
		return True

	def _insert(self,
		values: dict,
		conflict: str = 'error'
	) -> str:
		"""_insert

		Generates the actual INSERT SQL

		Returns:
			str
		"""

		# If we didn't get a dictionary
		if not isinstance(values, dict):
			raise ValueError('values', values)

		# Make sure conflict arg is valid
		if not isinstance(conflict, (tuple, list)) and \
			conflict not in ('error', 'ignore', 'replace'):
			raise ValueError('conflict', conflict)

		# Create the string of all fields and values but the primary if it's
		#	auto incremented
		lTemp = [[], []]
		for f in self._columns.keys():

			# If it's the key key with auto_key on and the value isn't
			#	passed
			if f == self._struct.key and \
				self._struct.auto_key and \
				f not in values:

				# If it's a string, add the field and set the value to the
				#	SQL variable
				if isinstance(self._struct.auto_key, str):

					# Add the field and set the value to the SQL variable
					lTemp[0].append('`%s`' % f)
					lTemp[1].append('@_AUTO_PRIMARY')

			elif f in values:
				lTemp[0].append('`%s`' % f)
				if values[f] != None:
					lTemp[1].append(escape(
						self._columns[f],
						values[f],
						self._struct.host
					))
				else:
					lTemp[1].append('NULL')

		# If we have replace for conflicts
		if conflict == 'replace':
			sUpdate = 'ON DUPLICATE KEY UPDATE %s' % ',\n'.join([
				"%s = VALUES(%s)" % (s, s)
				for s in lTemp[0]
			])

		elif isinstance(conflict, (tuple, list)):
			sUpdate = 'ON DUPLICATE KEY UPDATE %s' % ',\n'.join([
				"%s = VALUES(%s)" % (s, s)
				for s in conflict
			])

		# Else, no update
		else:
			sUpdate = ''

		# Join the fields and values
		sFields	= ','.join(lTemp[0])
		sValues	= ','.join(lTemp[1])

		# Cleanup
		del lTemp

		# Generate the INSERT statement
		return 'INSERT %sINTO `%s`.`%s` (%s) ' \
				'VALUES (%s) ' \
				'%s' % (
					(conflict == 'ignore' and 'IGNORE ' or ''),
					self._struct.db,
					self._struct.name,
					sFields,
					sValues,
					sUpdate
				)

	def insert(self,
		values: dict,
		conflict: str = 'error'
	) -> str | LT[True] | None:
		"""Insert

		Inserts a new record into the table

		Arguments:
			values (dict): The dictionary of fields to values to be inserted
			conflict (str | list): Must be one of 'error', 'ignore', 'replace',
				or a list of fields to update

		Returns:
			Returns True or the unique key for the newly inserted record, and
			None for failure
		"""

		# Generate the SQL
		sSQL = self._insert(values, conflict)

		# If the primary key is auto generated
		if self._struct.auto_key:

			# If it's a string
			if isinstance(self._struct.auto_key, str):

				# Set the SQL variable to the requested value
				server.execute(
					'SET @_AUTO_PRIMARY = %s' % self._struct.auto_key,
					self._struct.host
				)

				# Execute the regular SQL
				server.execute(sSQL, self._struct.host)

				# Fetch the SQL variable
				values[self._struct.key] = server.select(
					'SELECT @_AUTO_PRIMARY',
					server.Select.CELL,
					host=self._struct.host
				)

			# Else, assume auto_increment
			else:
				values[self._struct.key] = server.insert(
					sSQL,
					self._struct.host
				)

			# Return the new primary key
			return values[self._struct.key]

		# Else, the primary key was passed, we don't need to fetch it
		else:

			# if we succeeded, return True, else None
			return server.execute(sSQL, self._struct.host) and True or None

	def process_value(self, field: str, value: any) -> str:
		"""Process Value

		Takes a field and a value or values and returns the proper SQL
		to look up the values for the field

		Args:
			field (str): The name of the field
			value (any): The value as a single item, list, or dictionary

		Returns:
			str
		"""

		# If the value is a list
		if isinstance(value, (list, tuple)):

			# Build the list of values
			lValues = []
			for i in value:
				# If it's None
				if i is None:
					lValues.append('NULL')
				else:
					lValues.append(escape(
						self._columns[field],
						i,
						self._struct.host
					))
			sRet = 'IN (%s)' % ','.join(lValues)

		# Else if the value is a dictionary
		elif isinstance(value, dict):

			# If it has a start and end
			if 'between' in value:
				sRet = 'BETWEEN %s AND %s' % (
							escape(
								self._columns[field],
								value['between'][0],
								self._struct.host
							),
							escape(
								self._columns[field],
								value['between'][1],
								self._struct.host
							)
						)

			# Else if we have a less than
			elif 'lt' in value:
				sRet = '< %s' % escape(
					self._columns[field],
					value['lt'],
					self._struct.host
				)

			# Else if we have a greater than
			elif 'gt' in value:
				sRet = '> %s' % escape(
					self._columns[field],
					value['gt'],
					self._struct.host
				)

			# Else if we have a less than equal
			elif 'lte' in value:
				sRet = '<= %s' % escape(
					self._columns[field],
					value['lte'],
					self._struct.host
				)

			# Else if we have a greater than equal
			elif 'gte' in value:
				sRet = '>= %s' % escape(
					self._columns[field],
					value['gte'],
					self._struct.host
				)

			# Else if we have a not equal
			elif 'neq' in value:

				# If the value is a list
				if isinstance(value['neq'], (list, tuple)):

					# Build the list of values
					lValues = []
					for i in value['neq']:

						# If it's None, just use NULL
						if i is None:
							lValues.append('NULL')

						# Else, escape the value
						else:
							lValues.append(escape(
								self._columns[field],
								i,
								self._struct.host
							))
					sRet = 'NOT IN (%s)' % ','.join(lValues)

				# Else, it must be a single value
				else:
					if value['neq'] is None:
						sRet = 'IS NOT NULL'
					else:
						sRet = '!= %s' % escape(
							self._columns[field],
							value['neq'],
							self._struct.host
						)

			elif 'like' in value:
				sRet = 'LIKE %s' % escape(
					self._columns[field],
					value['like'],
					self._struct.host
				)

			# No valid key in dictionary
			else:
				raise ValueError(
					'value key must be one of "between", "lt", "gt", "lte", ' \
					'"gte", or "neq"'
				)

		# Else, it must be a single value
		else:

			# If it's None
			if value is None:
				sRet = 'IS NULL'
			else:
				sRet = '= %s' % escape(
					self._columns[field],
					value,
					self._struct.host
				)

		# Return the processed value
		return sRet

	def revision_add(self, key: any, items: dict) -> str:
		"""Revision Add

		Called to generate the code to add a record to the revision table \
		associated with this instance

		Arguments:
			key (any): The key to store the items under
			items (dict): The items to add to the revision table

		Returns:
			str
		"""

		# If changes are not allowed
		if self._struct.revisions == False:
			raise RuntimeError(
				'record_mysql.table isn\'t configured for revisions'
			)

		# If revisions requires specific indexes
		if isinstance(self._struct.revisions, list):
			for s in self._struct.revisions:
				if s not in items:
					raise ValueError(
						'record_mysql.table.revision_add.items missing "%s"' % s
					)

		# Generate and return the INSERT statement
		return 'INSERT INTO `%s`.`%s_revisions` (`%s`, `created`, `items`) ' \
				'VALUES(%s, CURRENT_TIMESTAMP, \'%s\')' % (
					self._struct.db,
					self._struct.name,
					self._struct.key,
					escape(
						self._columns[self._struct.key],
						key,
						self._struct.host
					),
					server.escape(
						jsonb.encode(items),
						self._struct.host
					)
				)

	def _select(self,
		distinct: bool = undefined,
		fields: list[str] = undefined,
		where: dict = undefined,
		groupby: str | list[str] = undefined,
		orderby: str | list[str] = undefined,
		limit: int | tuple = undefined
	) -> str:
		"""_select

		Generates the actual SELECT SQL

		Returns:
			str
		"""

		# Init the statements list with the SELECT
		lStatements = [
			'SELECT %s%s\n' \
			'FROM `%s`.`%s`' % (
				distinct and 'DISTINCT ' or '',
				fields is undefined and '*' or ','.join([
					(isinstance(f, Func) and \
						str(f) or \
						('`%s`' % f)
					) for f in fields
				]),
				self._struct.db,
				self._struct.name
			)
		]

		# If there's where pairs
		if where is not undefined:

			# Init list of WHERE
			lWhere = []

			# Go through each value
			for f,v in where.items():

				# If the field doesn't exist
				if f not in self._columns:
					raise ValueError(
						'record_mysql.table.update.where `%s` not a valid ' \
						'node' % str(f)
					)

				# Generate the SQL and append it to the where list
				lWhere.append(
					'`%s` %s' % (f, self.process_value(f, v))
				)

			# Add it to the list of statements
			lStatements.append(
				'WHERE %s' % '\nAND '.join(lWhere)
			)

		# If there's anything to group by
		if groupby is not undefined:

			# If it's a string
			if isinstance(groupby, str):

				# Add the single field to the list of statements
				lStatements.append(
					'GROUP BY `%s`' % groupby
				)

			# Else, if it's a list or tuple
			elif isinstance(groupby, (list, tuple)):

				# Add all the fields to the list of statements
				lStatements.append(
					'GROUP BY `%s`' % '`,`'.join(groupby)
				)

			# Else, it's invalid
			else:
				raise ValueError('groupby', groupby)

		# If there's anything to order by
		if orderby is not undefined:

			# If it's a string
			if isinstance(orderby, str):

				# Add the single field to the list of statements
				lStatements.append(
					'ORDER BY `%s`' % orderby
				)

			# Else, if it's a list or tuple
			elif isinstance(orderby, (list, tuple)):

				# Go through each field
				lOrderBy = []
				for m in orderby:
					if isinstance(m, (list, tuple)):
						lOrderBy.append('`%s` %s' % (m[0], m[1]))
					else:
						lOrderBy.append('`%s`' % m)

				# Add it to the list of statements
				lStatements.append(
					'ORDER BY %s' % ','.join(lOrderBy)
				)

			# Else, it's invalid
			else:
				raise ValueError('orderby', orderby)

		# If there's anything to limit by
		if limit is not undefined:

			# If we got an int
			if isinstance(limit, int):
				lStatements.append('LIMIT %d' % limit)

			# Else, if we got a tuple
			elif isinstance(limit, tuple):
				lStatements.append('LIMIT %d, %d' % limit)

			# Else
			else:
				raise ValueError(
					'record-mysql.table limit must be an int or a tuple, ' \
					'received: %s' % type(limit)
				)

		# Generate and return the SQL from the statements
		return '\n'.join(lStatements)

	def select(self,
		distinct: bool = undefined,
		fields: list[str] = undefined,
		where: dict = undefined,
		groupby: str | list[str] = undefined,
		orderby: str | list[str] = undefined,
		limit: int | tuple = undefined
	) -> list[dict]:
		"""Select

		Runs a select query and returns the results

		Arguments:
			distinct (bool): Optional, True to only return distinct records
			fields (str[]): Optional, the list of fields to return from the \
				table
			where (dict): Optional, field/value pairs to decide what records \
				to get
			groupby (str | str[]): Optional, a field or fields to group by
			orderby (str | str[]): Optional, a field or fields to order by
			limit (int | tuple): Optional, the max (int), or the starting \
				point and max (tuple)

		Returns:
			dict[]
		"""

		# Generate and run the query, then store the results
		lRows = server.select(
			self._select(distinct, fields, where, groupby, orderby, limit),
			host=self._struct.host
		)

		# If we have any data to convert in this table
		if self._convert:

			# Go through each row
			for d in lRows:

				# Go through each field
				for l in self._convert:

					# If the field exists and isn't None
					if l[0] in d and d[l[0]] is not None:

						# If it's a bool
						if l[1] == BOOL_CONVERT:
							d[l[0]] = d[l[0]] and True or False

						# If it's JSON
						elif [l[1]] == JSON_CONVERT:
							d[l[0]] = jsonb.decode(d[l[0]])

		# If we only want one record
		if (isinstance(limit, int) and limit == 1) or \
			(isinstance(limit, tuple) and limit[1] == 1):
			return lRows and lRows[0] or None

		# Return the rows
		return lRows

	def transaction(self) -> transaction.Transaction:
		"""transaction.Transaction

		Returns a new transaction.Transaction object associated with the \
		instance

		Returns:
			transaction.Transaction
		"""
		return transaction.Transaction(self)

	def _update(self,
		values: dict,
		where: dict = None,
		conflict: str = 'error'
	) -> str:
		"""_update

		Generates the actual UPDATE SQL

		Returns:
			str
		"""

		# If we didn't get a dictionary
		if not isinstance(values, dict):
			raise ValueError('values', values)

		# Make sure conflict arg is valid
		if conflict not in ('error', 'ignore'):
			raise ValueError('conflict', conflict)

		# Go through each value and create the pairs
		lSet = []
		for f in values.keys():

			# If the field doesn't exist
			if f not in self._columns:
				raise ValueError(
					'record_mysql.table.update.values `%s` not a valid node' % \
					f
				)

			# If it's None, set it to NULL
			if values[f] is None:
				lSet.append('`%s` = NULL' % f)
				continue

			# Escape the value using the node
			lSet.append('`%s` = %s' % (
				f, escape(
					self._columns[f],
					values[f],
					self._struct.host
				)
			))

		# Init the where fields
		lWhere = []

		# If there's an additional where
		if where:

			# Go through each value
			for f,v in where.items():

				# If the field doesn't exist
				if f not in self._columns:
					raise ValueError(
						'record_mysql.table.update.where `%s` not a valid ' \
						'node' % f
					)

				# Generate the SQL and append it to the where list
				lWhere.append(
					'`%s` %s' % (f, self.process_value(f, v))
				)

		# Generate the SQL to update the field and return it
		return 'UPDATE `%s`.`%s` ' \
				'SET %s ' \
				'%s' % (
			self._struct.db,
			self._struct.name,
			',\n'.join(lSet),
			lWhere and ('WHERE %s' % ' AND '.join(lWhere)) or ''
		)

	def update(self,
		values: dict,
		where: dict = None,
		conflict: str = 'error'
	) -> int:
		"""Update

		Updates a specific field to the value for an ID, many IDs, or the \
		entire table

		Arguments:
			values (dict): The dictionary of fields to values to be updated
			where (dict): Optional, field/value pairs to decide what records \
				get updated
			conflict (str): Must be one of 'error', 'ignore'

		Returns:
			uint: Number of records altered
		"""

		# Update all the records and return the number of rows changed
		return server.execute(self._update(
			values, where, conflict
		), self._struct.host)

	def uuid(self) -> str:
		"""UUID

		Returns a universal unique ID

		Arguments:
			None

		Returns:
			str
		"""

		# Get the UUID
		return server.uuid(self._struct.host)
# coding=utf8
"""Record Table

Handles a single SQL table and all that's required to interact with it
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-01"

# Limit imports
__all__ = ['escape', 'Table']

# Python imports
import re

# Pip imports
from define import Node, NOT_SET
import jsonb
from record import Limit

# Local imports
from . import Literal, server
from .storage import Storage

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
						'__sql__.type is not set in Record_MySQL'
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

class Table():
	"""Table

	Represents a single SQL table and interacts with raw data in the form of
	python dictionaries
	"""

	def __init__(self, name: str, details: dict):
		"""Constructor

		"""

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
			self._struct.create = self.parent.keys()

			# Order them alphabetically
			self._struct.create.sort()

		# If the primary key is added, remove it
		if self._struct.key in self._struct.create:
			self._struct.create.remove(self._struct.key)

		# Get all child node keys
		lNodeKeys = self._parent.keys()
		lMissing = [
			s for s in lNodeKeys \
			if s not in self._struct.create and \
				s != self._struct.key
		]

		# If any are missing
		if lMissing:
			raise ValueError(
				'record_mysql.table.create missing fields `%s` for `%s`.`%s`' % (
					'`, `'.join(lMissing),
					self._struct.db,
					self._struct.table
				)
			)

		# Generate the list of fields
		lFields = []
		for f in self._struct.create:

			# Get the sql special data
			dSQL = self._parent[f].special('mysql', default = {})

			# If it's a string
			if isinstance(dSQL, str):
				dSQL = { 'type': dSQL }

			# Add the line
			lFields.append('`%s` %s %s' % (
				f,
				('type' in dSQL and dSQL['type'] or \
					_node_to_type(self._parent[f], self._struct.host)
				),
				('opts' in dSQL and dSQL['opts'] or \
					(self._parent[f].optional() and 'null' or 'not null')
				)
			))

		# If we have a primary key
		if self._struct.key:

			# Push the primary key to the front
			#	Get the sql special data
			dSQL = self._parent[self._struct.key].special('mysql', default = {})

			# If it's a string
			if isinstance(dSQL, str):
				dSQL = { 'type': dSQL }

			# Primary key type
			sIDType = 'type' in dSQL and \
						dSQL['type'] or \
						_node_to_type(
							self._parent[self._struct.key],
							self._struct.host
						)
			sIDOpts = 'opts' in dSQL and dSQL['opts'] or 'not null'

			# Add the line
			lFields.insert(0, '`%s` %s %s%s' % (
				self._struct.key,
				sIDType,
				(self._struct.auto_key is True and 'auto_increment ' or ''),
				sIDOpts
			))

			# Init the list of indexes
			lIndexes = ['primary key (`%s`)' % self._struct.key]

		else:
			lIndexes = []

		# If there are indexes
		if self._struct.indexes:

			# Make sure it's a list
			if not isinstance(self._struct.indexes, list):
				raise ValueError(
					'record_mysql.table.create.indexes must be a list'
				)

			# Init the list of indexes
			lIndexes = []

			# Loop through the indexes to get the data associated
			for mi in self._struct.indexes:

				# If the index is a string
				if isinstance(mi, str):

					# Create a non-unique index with both the name and the field
					#	being the index string
					lIndexes.append('INDEX `%s` (`%s`)' % (
						sIndexType, mi, mi
					))

				# Else, if the index is a dict
				elif isinstance(mi, dict):

					# If the dictionary has no name
					if 'name' not in mi or not isinstance(mi.name, str):
						raise ValueError(
							'record_mysql.table.create.indexes[].name must ' \
							'be a string'
						)

					# If we have fields
					if 'fields' in mi:

						# If it's not a list
						if not isinstance(mi.fields, list):
							raise ValueError(
								'record_mysql.table.create.indexes[].fields ' \
								'must be a list'
							)

						# Init the list of index fields
						lFields = []

						# Go through each field in the list
						for mf in mi.fields:

							# If it's a string, use it as is
							if isinstance(mf, str):
								lFields.append('`%s`' % mf)

							# Else, if it's a dict
							elif isinstance(mf, dict):

								# If we are missing a name
								if 'name' not in mf:
									raise ValueError(
										'record_mysql.table.create.indexes[].' \
										'fields[].name is required'
									)

								# If we have a order set
								if 'order' in mf:

									# If the order is invalid
									if mf.order.upper() not in ['ASC', 'DESC']:
										raise ValueError(
											'record_mysql.table.create.indexes' \
											'[].fields[].order must be one of ' \
											'\'ASC\' | \'DESC\''
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
											'record_mysql.table.create.indexes' \
											'[].fields[].size must be an int'
										)

									# Set the size
									sIndexFieldSize = '(%d)' % mf.size

								# Else, make it a simple index
								else:
									sIndexFieldSize = ''

								# Combine the parts into one index field
								lFields.append('`%s`%s %s' % (
									mf.name,
									sIndexFieldSize,
									sIndexFieldOrder
								))

						# Join the fields together
						sIndexFields = ', '.join(lFields)

					# Else, use the name as the field
					else:
						sIndexFields = '`%s`' % mi.name

					# If we have a type set
					if 'type' in mi:

						# If the type is invalid
						if mi.type.upper() not in [
							'UNIQUE', 'FULLTEXT', 'SPATIAL'
						]:
							raise ValueError(
								'record_mysql.table.create.indexes[].type ' \
								'must be one of \'UNIQUE\' | \'FULLTEXT\' | ' \
								'\'SPATIAL\''
							)

						# Set the type
						sIndexType = mi.type.upper()

					# Else, make it a simple index
					else:
						sIndexType = 'INDEX'

					# Append the index
					lIndexes.append('%s `%s` (%s)' % (
						sIndexType, mi.name, sIndexFields
					))

				# Else, the index is invalid
				else:
					raise ValueError(
						'record_mysql.table.create.indexes[] must be a str or ' \
						'dict'
					)

		# Generate the CREATE statement
		sSQL = 'CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s, %s) '\
				'ENGINE=%s CHARSET=%s COLLATE=%s' % (
					self._struct.db,
					self._struct.table,
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
		if self._struct.key and self._struct.revisions:

			# Generate the CREATE statement
			sSQL = 'CREATE TABLE IF NOT EXISTS `%s`.`%s_revisions` (' \
					'`%s` %s NOT NULL %s, ' \
					'`created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, ' \
					'`items` TEXT NOT NULL, ' \
					'INDEX `%s` (`%s`)) ' \
					'ENGINE=%s CHARSET=%s COLLATE=%s' % (
				self._struct.db,
				self._struct.table,
				self._struct.key,
				sIDType,
				sIDOpts,
				self._struct.key, self._struct.key,
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

	def delete(self, where: dict = NOT_SET) -> int:
		"""Delete

		Deletes all or some records

		Arguments:
			where (dict): Optional, field/value pairs to decide what records get
							deleted

		Returns:
			uint: number of records deleted
		"""

		# Init the where fields
		sWhere = None

		# If there's an additional where
		if where is not NOT_SET:

			# Init the list of WHERE statements
			lWhere = []

			# Go through each filed/value pair in the where
			for f,v in where.items():

				# If the field doesn't exist
				if f not in self._parent:
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

		# Generate the SQL to update the field
		sSQL = 'UPDATE `%s`.`%s` ' \
				'%s' % (
			self._struct.db,
			self._struct.table,
			sWhere or ''
		)

		# Delete all the records and return the number of rows changed
		return server.execute(sSQL, self._struct.host)

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
			self._struct.table,
		)

		# Delete the table
		server.execute(sSQL, self._struct.host)

		# If revisions are required
		if self._struct.revisions:

			# Generate the DROP statement
			sSQL = 'drop table `%s`.`%s_revisions`' % (
				self._struct.db,
				self._struct.table,
			)

			# Delete the table
			server.execute(sSQL, self._struct.host)

		# Return OK
		return True

	def insert(self, values: dict, conflict: str = 'error') -> any:
		"""Insert

		Inserts a new record into the table

		Arguments:
			values (dict): The dictionary of fields to values to be inserted
			conflict (str | list): Must be one of 'error', 'ignore', 'replace',
				or a list of fields to update

		Returns:
			The unique key for the newly inserted record or True for success,
			and None for failure
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
		for f in self.keys():

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
					lTemp[1].append(self.escape(
						f,
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
		sSQL = 'INSERT %sINTO `%s`.`%s` (%s)\n' \
				' VALUES (%s)\n' \
				'%s' % (
					(conflict == 'ignore' and 'IGNORE ' or ''),
					self._struct.db,
					self._struct.table,
					sFields,
					sValues,
					sUpdate
				)

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

		# Get the field node
		oNode = self._parent[field]

		# If the value is a list
		if isinstance(value, (list, tuple)):

			# Build the list of values
			lValues = []
			for i in value:
				# If it's None
				if i is None: lValues.append('NULL')
				else: lValues.append(server.escape(oNode, i, self._struct.host))
			sRet = 'IN (%s)' % ','.join(lValues)

		# Else if the value is a dictionary
		elif isinstance(value, dict):

			# If it has a start and end
			if 'between' in value:
				sRet = 'BETWEEN %s AND %s' % (
							server.escape(
								oNode,
								value['between'][0],
								self._struct.host
							),
							server.escape(
								oNode,
								value['between'][1],
								self._struct.host
							)
						)

			# Else if we have a less than
			elif 'lt' in value:
				sRet = '< %s' % server.escape(
					oNode,
					value['lt'],
					self._struct.host
				)

			# Else if we have a greater than
			elif 'gt' in value:
				sRet = '> %s' % server.escape(
					oNode,
					value['gt'],
					self._struct.host
				)

			# Else if we have a less than equal
			elif 'lte' in value:
				sRet = '<= %s' % server.escape(
					oNode,
					value['lte'],
					self._struct.host
				)

			# Else if we have a greater than equal
			elif 'gte' in value:
				sRet = '>= %s' % server.escape(
					oNode,
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
							lValues.append(server.escape(
								oNode,
								i,
								self._struct.host
							))
					sRet = 'NOT IN (%s)' % ','.join(lValues)

				# Else, it must be a single value
				else:
					if value['neq'] is None:
						sRet = 'IS NOT NULL'
					else:
						sRet = '!= %s' % server.escape(
							oNode,
							value['neq'],
							self._struct.host
						)

			elif 'like' in value:
				sRet = 'LIKE %s' % server.escape(
					oNode,
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
				sRet = '= %s' % server.escape(
					oNode,
					value,
					self._struct.host
				)

		# Return the processed value
		return sRet

	def revision_add(self, key: any, items: dict):
		"""Revision Add

		Called to add a record to the revision table associated with this
		instance

		Arguments:
			key (any): The key to store the items under
			items (dict): The items to add to the revision table

		Returns:
			None
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

		# Generate the INSERT statement
		sSQL = 'INSERT INTO `%s`.`%s_changes` (`%s`, `created`, `items`) ' \
				'VALUES(%s, CURRENT_TIMESTAMP, \'%s\')' % (
					self._struct.db,
					self._struct.table,
					self._struct.key,
					self.escape(
						self._parent[self._struct.key],
						key,
						self._struct.host
					),
					server.escape(
						jsonb.encode(items),
						self._struct.host
					)
				)

		# Create the revisions record and return the records inserted
		server.execute(sSQL, self._struct.host)

	def select(self,
		distinct: bool = NOT_SET,
		fields: list = NOT_SET,
		where: dict = NOT_SET,
		groupby: str | list = NOT_SET,
		orderby: str | list = NOT_SET,
		limit: Limit = NOT_SET
	) -> list | dict | any:
		"""Select

		Runs a select query and returns the results

		Arguments:
			distinct (bool): Optional, True to only return distinct records
			fields (list): Optional, the list of fields to return from the table
			where (dict): Optional, field/value pairs to decide what records to
							get
			orderby (str | list): Optional, a field or fields to order by
			limit (records.Limit): Optional, the limit and starting point

		Returns:
			list
		"""

		# Init the statements list with the SELECT
		lStatements = [
			'SELECT %s%s\n' \
			'FROM `%s`.`%s`\n' \
			'%s' % (
				distinct and 'DISTINCT ' or '',
				fields is NOT_SET and '*' or ('`%s`' % '`,`'.join(fields)),
				self._struct.db,
				self._struct.table
			)
		]

		# If there's where pairs
		if where is not NOT_SET:

			# Init list of WHERE
			lWhere = []

			# Go through each value
			for f,v in where.items():

				# If the field doesn't exist
				if f not in self._parent:
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
				'WHERE %s' % ',\n'.join(lWhere)
			)

		# If there's anything to group by
		if groupby is not NOT_SET:

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
		if orderby is not NOT_SET:

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
		if limit is not NOT_SET:

			# If we have a max
			if limit.max:

				# If we have a start as well, use both
				if limit.start:
					lStatements.append('LIMIT %d, %d' % limit)

				# Else, we just have a max
				else:
					lStatements.append('LIMIT %d' % limit.max)

		# Combine all the statements into one string, run the query, and return
		#	the results
		return server.select(
			'\n'.join(lStatements),
			self._struct.host
		)

	def update(self,
		values: dict,
		where: dict = None,
		conflict: str = 'error') -> int:
		"""Update

		Updates a specific field to the value for an ID, many IDs, or the entire
		table

		Arguments:
			values (dict): The dictionary of fields to values to be updated
			where (dict): Optional, field/value pairs to decide what records get
							updated
			conflict (str): Must be one of 'error', 'ignore'

		Returns:
			uint: Number of records altered
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
			if f not in self._parent:
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
				f, self.escape(
					self._parent[f],
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
				if f not in self._parent:
					raise ValueError(
						'record_mysql.table.update.where `%s` not a valid ' \
						'node' % f
					)

				# Generate the SQL and append it to the where list
				lWhere.append(
					'`%s` %s' % (f, self.process_value(f, v))
				)

		# Generate the SQL to update the field
		sSQL = 'UPDATE `%s`.`%s` ' \
				'SET %s ' \
				'%s' % (
			self._struct.db,
			self._struct.table,
			',\n'.join(lSet),
			lWhere and ('WHERE %s' % ' AND '.join(lWhere)) or ''
		)

		# Update all the records and return the number of rows changed
		return server.execute(sSQL, self._struct.host)

	def uuid(self) -> str:
		"""UUID

		Returns a universal unique ID

		Arguments:
			None

		Returns:
			str
		"""

		# Get the UUID
		server.uuid(self._struct.host)
# coding=utf8
"""Record Leveled

Handles a structure which contains a list or hash of other nodes, including
other complex types
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-04"

# Limit exports
__all__ = ['Leveled']

# Python imports
from copy import copy

# Pip imports
import define
from jobject import JObject
from tools import combine, compare, merge, without

# Local imports
from .base import Base
from .table import Table

class Leveled(Base):
	"""Leveled

	Represents a define Array or Hash that contains other Nodes

	Extends:
		Base
	"""

	def __init__(self,
		name: str,
		parent: Base,
		details: define.Array | define.Hash
	):
		"""Constructor

		Creates a new instance

		Arguments:
			name (str): The name of the structure in its parent, if it has one
			parent (Base): The instance of the parent this struct belongs to
			details (define.Array | define.Hash): The definition associated

		Returns:
			a new instance
		"""

		# If the parent or name is missing
		if name is None or parent is None:
			raise ValueError('record_mysql.leveled must be a child of a parent')

		# Call the Base constructor
		super(Leveled, self).__init__(name, parent)

		# Get the child associated with this array, and it's class name
		oChild = details.child()
		sChild = oChild.class_name()

		# By default, mark this as a complex array
		self._node: bool = False

		# Add the key fields to the columns
		self._keys['_id'] = define.Node({ '__type__': 'uuid'})
		self._keys['_parent'] = define.Node({ '__type__': 'uuid' })

		# Init the number of levels and the first key based on whether we have
		#	an array or a hash
		if details.class_name() == 'Array':
			self._levels = ['_a_0']
			self._keys['_a_0'] = define.Node({ '__type__': 'uint' })
		elif details.class_name() == 'Hash':
			self._levels = ['_h_0']
			self._keys['_h_0'] = details.key()

		# Loop until we break
		while True:

			# If it's an array
			if sChild in ['Array', 'Hash']:

				# Add a column for the new array. Use the zero just to save
				#	a little memory
				self._levels.append('_a_%d' % len(self._levels))
				self._keys[self._levels[-1]] = self._keys['_a_0']

				# Set the child to the new child and loop back around
				oChild = oChild.child()
				sClass = oChild.class_name()
				continue

			# Else, if we got a Node, this is going to be a special table of
			#	just one column
			elif sChild == 'Node':

				# Mark it as one node
				self._node = True

				# Remove the ID key, we won't be updating individual rows
				del self._keys['_id']

				# We only have one column, the child
				self._columns['_value'] = oChild

				# We're done with the loop
				break

			# If it's a parent, we'll create columns for each field
			elif sChild == 'Parent':

				# Step through each one of fields in the Parent (oChild)
				for f in oChild:

					# Get the class name
					sFieldClass = oChild[f].class_name()

					# Check for a special section
					dMySQL = oChild[f].special('mysql', {})

					# If it's a Node or meant to be stored as JSON
					if sFieldClass == 'Node' or \
						('json' in dMySQL and dMySQL['json']):

						# Add the node to the columns list under its name
						self._columns[f] = oChild[f]

					# Else, it's a complex type
					else:

						# Try to create it and store it under its name
						self._complex[f] = self.create_type(
							sFieldClass,
							f,
							self,
							oChild[f]
						)

				# We're done with the loop
				break

			# Else
			raise ValueError(
				'record_mysql does not implement define.%s' % sClass
			)

		# Get the parent structure
		dParent = self._parent.struct()

		# Init the structure
		dStruct = JObject({
			'auto_key': (self._node == False) and 'UUID()' or False,
			'create': [
				*self._keys.keys(),
				*self._columns.keys()
			],
			'db': dParent.db,
			'host': dParent.host,
			'indexes': [{
				'name': 'parent_index',
				'fields': ['_parent', *self._levels],
				'type': 'unique'
			}],
			'key': (self._node == False) and '_id' or False,
			'revisions': False,
			'name': '%s_%s' % (dParent.name, name)
		})

		# If there's a special section, overwrite any values it has
		#	with the created ones
		dMySQL = details.special('mysql')
		if dMySQL:
			merge(dStruct, dMySQL)

		# Create a new columns with the ID
		dColumns = {**self._keys, **self._columns}

		# Create a table using the generated structure and the list of columns
		self._table = Table(
			dStruct,
			dColumns
		)

	def _elevate(self,
		rows: list[dict],
		level: int = 0
	) -> list[any] | dict[str, any]:
		"""Elevate

		Opposite of Flatten, goes through table rows and recursively turns them
		into arrays of arrays and hashes of hashes

		Arguments:
			rows (dict[]): The rows to loop through

		Returns:
			list | dict
		"""

		# Get the current field
		field = self._levels[level]

		# If we're on an array
		if field[1:2] == 'a':

			# Init the return list
			lRet = []

			# Init the starting point and first list
			i = 0
			l = []

			# Go through each record
			for d in rows:

				# If we have a new element
				if i != d[field]:

					# Add the current element to the return
					lRet.append(l)

					# Reset the list
					l = []

					# Set the current index
					i = d[field]

				# Append the data minus the current field
				l.append(without(d, field))

			# If we have anything in the list, append it to the return
			if l:
				lRet.append(l)

			# If we're on the last level
			if level + 1 == len(self._levels):

				# If it's a single node
				if self._node:

					# Get rid of the last list and dict
					lRet = lRet[0]['_value']

				# Else, just get rid of the last list
				else:
					lRet = lRet[0]

			# Else, we have more levels to go
			else:

				# Go through each one of the current return values to process the
				#	next level
				for i in range(len(lRet)):
					lRet[i] = self._arrays_of_arrays(lRet[i], level + 1)

			# Return the list
			return lRet

		# Else, if we have a hash
		elif field[1:2] == 'h':

			# Init the return dict
			dRet = {}

			# Go through each record
			for d in rows:

				# Try to add it to the list
				try:
					dRet[d[field]].append(without(d, field))
				except KeyError:
					dRet[d[field]] = [without(d, field)]

			# If we're on the last level
			if level + 1 == len(self._levels):

				# Go through each key
				for k in dRet:

					# If it's a single node, get rid of the last list and dict
					if self._node:
						dRet[k] = dRet[k][0]['_value']

					# Else, just get rid of the last list
					else:
						dRet[k] = dRet[k][0]

			# Else, we have more levels to go
			else:

				# Go through each of the current keys and process the next level
				for k in dRet:
					dRet[k] = self._arrays_of_arrays(dRet[k], level + 1)

			# Return the dict
			return dRet

		# Something went wrong
		raise ValueError('level', field)

	def _flatten(self,
		data: dict | list,
		level: int = 0,
		row: dict = {}
	) -> list:
		"""Flatten

		Opposite of Elevate, takes a complex structure and flattens it into a
		set of fields describing the data's levels based on the current
		structure

		Arguments:
			data (dict | list): The data in it's pure form

		Returns:
			dict[]
		"""

		# Get the current field
		field = self._levels[level]


		# Init the return
		lRet = []

		# If we're on an array
		if field[1:2] == 'a':

			# For each passed record
			for i in range(len(data)):

				# Create a new dict starting with the passed in row
				dRow = copy(row)

				# If we're on the last level
				if level + 1 == len(self._levels):

					# And add the new column for the level
					dRow[field] = i

					# If we're on a single node, store the value under _value
					if self._node:
						dRow['_value'] = data[i]

					# Else, update the row with the last dict
					else:
						dRow.update(data[i])

					# Add the row to the return list
					lRet.append(dRow)

				# Else, we have more levels to go
				else:

					# Add a new column for the level
					dRow[field] = i

					# Pass it down the line and extend the return with whatever
					# we get back
					lRet.extend(
						self._flatten(data[i], level + 1, dRow)
					)

		# Else, if we're on a hash
		elif field[1:2] == 'h':

			# Go through each key
			for k in data:

				# Create a new dict starting with the passed in row
				dRow = copy(row)

				# If we're on the last level
				if level + 1 == len(self._levels):

					# Add a new column for the level
					dRow[field] = k

					# If we're on a single node, store the value under _value
					if self._node:
						dRow['_value'] = data[k]

					# Else, update the row with the last dict
					else:
						dRow.update(data[k])

					# Add the row to the list
					lRet.append(dRow)

					# Add the row to the return list
					lRet.append(dRow)

				# Else, we have more levels to go
				else:

					# Add a new column for the level
					dRow[field] = k

					# Pass it down the line and extend the return with whatever we
					#	get back
					lRet.extend(
						self._flatten(data[k], level + 1, dRow)
					)

		# Else we got an invalid level field
		else:
			raise ValueError('level', field)

		# Return the list of flattened data
		return lRet

	def _get_ids(self, ids: list[str]) -> list[str]:
		"""Get IDs

		Returns the IDs associated with the ones given

		Arguments:
			ids (str[]): The IDs to find the parents for

		Returns:
			str[]
		"""

		# If there's a table
		try:
			lIDs = [d['_parent'] for d in
				self._table.select(
					distinct = True,
					fields = ['_parent'],
					where = { '_id': ids }
				)
			]

		# If there's no table, get the parent's IDs as passed, or return them as
		#	is
		except AttributeError:
			return self._parent and self._parent._get_ids(ids) or ids

		# Get the parent's IDs or return them as is
		return self._parent and self._parent.get_ids(lIDs) or lIDs

	def filter(self, filter: any) -> list[str]:
		"""Filter

		Returns the top level IDs filtered by the given field/value pairs

		Arguments:
			values (dict): The field and value pairs to filter by

		Returns:
			str[]
		"""

		# Set the filter values
		lFilter = self._node and {'_value': filter} or filter

		# If there's a table
		lIDs = [d['_parent'] for d in
			self._table.select(
				distinct = True,
				fields = ['_parent'],
				where = lFilter
			)
		]

		# Get the parent's IDs or return them as is
		return self._parent and self._parent.get_ids(lIDs) or lIDs

	def get(self, id: str) -> list[dict]:
		"""Get

		Retrieves all the rows associated with the given ID

		Arguments:
			id (str): The ID to fetch rows for

		Returns:
			dict[]
		"""

		# Find the records ordered by the levels and store them by unique ID
		dRows = {d['_id']:d for d in self._table.select(
			where = { '_parent': id },
			orderby = self._levels
		)}

		# Go through each complex record
		for f in self._complex:

			# For each row
			for sID in dRows:

				# Call the child get, passing along the ID, then store the
				#	results by that ID
				dRows[sID][f] = self._complex[f].get(sID)

		# Now that we have all the data, split it up by the levels and return it
		return self._elevate(
			list(dRows.values())
		)

	def update(self,
		id: str,
		data: list | dict,
		return_revisions: bool
	) -> list | dict | bool:
		"""Update

		Updates the record(s) associated with the given ID and returns True if
		something was updated. If `return_revisions` is True, the revisions list
		or dict will be returned instead of True. In all cases, nothing being
		updated returns False

		Arguments:
			id (str): The ID to update records for
			data (list | dict): A list or dict representing a structure of data
									to be updated under the given ID
			return_revisions (bool): If True, returns a structure of values
									changed instead of True

		Returns:
			list | dict | bool
		"""

		# Init the return
		if return_revisions:
			if self.__class__.__name__ == 'Array':
				mRet = []
			else:
				mRet = {}
		else:
			mRet = False

		# Flatten the values recieved so we can compare them to the table rows
		lData = self._flatten(data)

		# Create the transactions list
		oT = self._table.transaction()

		# If it's a single node table
		if self._node:

			# Get the existing values
			lValues = self._table.select(
				fields = [*self._levels, '_value'],
				orderby = self._levels,
				where = { '_parent': id }
			)

			# If the data is not the same
			if not compare(lValues, lData):

				# Create a new transaction
				oT = self._table.transaction()

				# Generate the SQL to delete all rows associated with the parent
				oT.add('delete', where = { '_parent': id })

				# Go through each new row
				for d in lData:

					# Generate the SQL to insert the row with the parent ID
					oT.add('insert', values = combine(
						d, { '_parent': id }
					))

				# Run all the SQL statements
				oT.go()

				# If we want revisions
				if return_revisions:
					return {
						'old': self._elavate(lValues),
						'new': data
					}

		# Else, we have a multi-value table
		else:

			# Fetch the records associated with the ID
			dRows = {d['_id']:d for d in self._table.select(
				where = { '_parent': id },
				orderby = self._levels
			)}

			# Init the list of IDs with array swaps
			lSwapIDs = []
			lSwapFields = set()

			# Go through each "row" passed
			for d in lData:

				# If it has an ID and it exists in the rows
				if '_id' in d and d['_id'] in dRows:

					# If it has the deleted record
					if '__delete__' in d:

						# Add the delete to the transactions
						oT.add('delete', where = { '_id': d['_id'] })

						# Mark the record as removed
						mRet = {
							'old': without(dRows[d['_id']], self._levles),
							'new': None
						}

					# Else, we are updating
					else:

						# Init the fields to update
						dUpdate = {}

						# Go through each level
						for s in self._levels:

							# If data doesn't match, the record has moved somewhere
							#	down the line
							if d[s] != dRows[d['_id']][s]:

								# If it's an array, store the value as it's opposite
								#	and store the ID so we know to fix it later
								if s[1:2] == 'a':
									dUpdate[s] = -d[s]
									lSwapIDs.append(d['_id'])
									lSwapFields.add(s)

								# Else, if it's a hash, just change the value, we
								#	should be fine
								elif s[1:2] == 'h':
									dUpdate[s] = d[s]

						# Go through each possible field of the actual data
						for f in self._columns:

							# If the field exists in the data
							if f in d:

								# If the value doesn't exist in the existing
								#	data, or it does but it's different
								if f not in dRows[d['_id']] or \
									dRows[d['_id']][f] != d[f]:

									# Update the field
									dUpdate[f] = d[f]

									# If we want revisions
									if return_revisions:
										mRet[f] = {
											'old': f in dRows[d['_id']] and \
													dRows[d['_id']]['f'] or \
													None,
											'new': d[f]
										}

									# Else, just true
									else:
										mRet = True

						# If the number of updates equals the total columns
						if len(dUpdate) == len(self._columns):
							mRet = {
								'old': self._elevate(dRows[d['_id']]),
								'new': self._elevate(d)
							}

						# If we have anything to update
						if dUpdate:

							# Add it to the transaction
							oT.add('update',
								values = dUpdate,
								where = { '_id': d['_id'] }
							)

				# If it doesn't have an ID, assume a new record
				else:

					# Add the create to the transactions
					oT.add('insert', values = d)
					if return_revisions:
						mRet = {
							'old': None,
							'new': d
						}
					else:
						mRet = True



			# If we had any swaps
			if lSwapIDs:

				# Add the swap statement
				oT._sql("UPDATE `%s`.`%s` SET %s WHERE `_id` IN ('%s')" % (
					self._table._struct.db,
					self._table._struct.name,
					', '.join([
						'`%(s)s` = ABS(`%(s)s`)' % {'s': s} for s in lSwapFields
					]),
					"','".join(lSwapIDs)
				))

			# Run all the transactions
			if not oT.go():
				mRet = False

			# Go through each "row" passed
			for d in lData:

				# Go through each complex part
				for f in self._complex:

					# If it exists in the values
					if f in d:

						# Set it
						mComplexRet = self._complex[f].set(
							id,
							d[f],
							return_revisions
						)

						# If we got a positive result
						if mComplexRet:

							# If we are returning revisions, add them to the return
							if return_revisions:
								mRet[f] = mComplexRet

							# Else, note that something changed
							else:
								mRet = True

		# If we want revisions but we only have an empty dict, change the
		#	return to False
		if return_revisions and not mRet:
			mRet = False

		# Return the result
		return mRet

# Add the Array and Hash types to the base
Base.add_type('Array')
Base.add_type('Hash')
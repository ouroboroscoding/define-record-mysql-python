# coding=utf8
"""Record Leveled

Handles a structure which contains a list or hash of other nodes, including \
other complex types
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-04"

# Limit exports
__all__ = ['Leveled']

# Ouroboros imports
import define
from jobject import jobject
from tools import combine, compare, lfindi, merge, without
import undefined

# Python imports
from copy import copy
from typing import List

# Local imports
from record_mysql.base import Base
from record_mysql.table import Table
from record_mysql.transaction import Transaction

# Mapping / Constants
_types = {
	'Array': 'a',
	'Hash': 'h'
}

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

		# By default, mark this as a complex array
		self._node: bool = False

		# Add the key fields to the columns
		self._keys[parent._table._struct.key] = \
			define.Node({ '__type__': 'uuid'})
		self._keys['_parent'] = define.Node({ '__type__': 'uuid' })

		# Init the number of levels and the first key based on whether we have
		#	an array or a hash
		if details.class_name() == 'Array':
			self._levels = ['_a_0']
			self._keys['_a_0'] = define.Node({ '__type__': 'uint' })
		elif details.class_name() == 'Hash':
			self._levels = ['_h_0']
			self._keys['_h_0'] = details.key()

		# Get the child associated with this array/hash, and it's class name
		oChild = details.child()
		sChild = oChild.class_name()

		# Loop until we break
		while True:

			# If it's an array
			if sChild in ['Array', 'Hash']:

				# Add a column to the levels for the new array or hash
				self._levels.append(
					'_%s_%d' % (_types[sChild], len(self._levels))
				)

				# If it's an array, create an unsigned int node, otherwise use
				#	the key of the child
				self._keys[self._levels[-1]] = sChild == 'Array' and \
					define.Node({ '__type__': 'uint' }) or \
					oChild.key()

				# Set the child to the new child and loop back around
				oChild = oChild.child()
				sChild = oChild.class_name()
				continue

			# Else, if we got a Node, this is going to be a special table of
			#	just one column
			elif sChild == 'Node':

				# Mark it as one node
				self._node = True

				# Remove the ID key, we won't be updating individual rows
				del self._keys[parent._table._struct.key]

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
					dMySQL = oChild[f].special('mysql', default={})

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
				'record_mysql does not implement define.%s' % sChild
			)

		# Get the parent structure
		dParent = self._parent.struct()

		# Init the structure
		dStruct = jobject({
			'auto_key': (self._node == False) and 'UUID()' or False,
			'create': [
				*self._keys.keys(),
				*self._columns.keys()
			],
			'db': dParent.db,
			'host': dParent.host,
			'indexes': {
				'parent_index': {
					'fields': ['_parent', *self._levels],
					'type': 'unique'
				}
			},
			'key': (self._node == False) and dParent.key or False,
			'revisions': False,
			'name': '%s_%s' % (dParent.name, name)
		})

		# If there's a special section, overwrite any values it has
		#	with the created ones
		dMySQL = details.special('mysql')
		if dMySQL:

			# If there's any additional indexes
			if 'indexes' in dMySQL:

				# Remove them and use them to extend the main struct
				merge(dStruct.indexes, dMySQL.pop('indexes'))

			# Merge whatever remains
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

		Opposite of Flatten, goes through table rows and recursively turns \
		them into arrays of arrays and hashes of hashes

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
					lRet = [l[0]['_value'] for l in lRet]

				# Else, just get rid of the last list
				else:
					lRet = lRet[0]

			# Else, we have more levels to go
			else:

				# Go through each one of the current return values to process
				#	the next level
				for i in range(len(lRet)):
					lRet[i] = self._elevate(lRet[i], level + 1)

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
					dRet[k] = self._elevate(dRet[k], level + 1)

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

		Opposite of Elevate, takes a complex structure and flattens it into a \
		set of fields describing the data's levels based on the current \
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

				# Else, we have more levels to go
				else:

					# Add a new column for the level
					dRow[field] = k

					# Pass it down the line and extend the return with whatever
					#	we get back
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
					where = { self._table._struct.key: ids }
				)
			]

		# If there's no table, get the parent's IDs as passed, or return them as
		#	is
		except AttributeError:
			return self._parent and self._parent._get_ids(ids) or ids

		# Get the parent's IDs or return them as is
		return self._parent and self._parent.get_ids(lIDs) or lIDs

	def delete(self,
		_id: str,
		ta: Transaction = undefined
	) -> list | dict | None:
		"""Delete

		Deletes one or more rows associated with the given ID and returns what \
		was deleted

		Arguments:
			_id (str): The unique ID associated with rows to be deleted
			ta (Transaction): Optional, the open transaction to add new sql \
				statements to

		Returns:
			list | None
		"""

		# Create a transaction using our table
		lTA = self._table.transaction()

		# Get the existing data with the levels
		lOldData = self._table.select(
			where = { '_parent' : _id },
			orderby = self._levels
		)

		# If any exists
		if lOldData:

			# Delete them
			lTA.delete(where = { '_parent': _id })

			# Go through each complex field
			for f in self._complex:

				# Go through each old row
				for i in range(len(lOldData)):
					mRet = self._complex[f].delete(
						lOldData[i][self._table._struct.key],
						lTA
					)
					if mRet:
						lOldData[i][f] = mRet

		# If we have a transaction passed in, extend it with ours
		if ta is not undefined:
			ta.extend(lTA)

		# Else, run everything
		else:

			# If we were not successful, return failure
			if not lTA.run():
				return False

		# If we have old data
		if lOldData:
			lOldData = self._elevate(
				without(lOldData, [self._table._struct.key, '_parent'])
			)

		# Return the data
		return lOldData or None

	def filter(self, filter: any) -> list[str]:
		"""Filter

		Returns the top level IDs filtered by the given field/value pairs

		Arguments:
			values (dict): The field and value pairs to filter by

		Returns:
			str[]
		"""

		# If we only have a single node
		if self._node:

			# Fetch and return the IDs
			return [d['_parent'] for d in
				self._table.select(
					distinct = True,
					fields = ['_parent'],
					where = { '_value': filter }
				)
			]

		# Else, we have numerous fields
		#	Init the columns and IDs
		dColumns = {}
		dComplex = {}
		lsIDs = None

		# First, go through each key of the filter and pull out all the ones
		#	in this level
		for k in list(filter.keys()):
			if k in self._columns:
				dColumns[k] = filter.pop(k)
			elif k in self._complex:
				dComplex[k] = filter.pop(k)
			else:
				raise KeyError(k, 'not a valid column')

		# If there's any local columns
		if dColumns:

			# Find the IDs of the records with the given filter in the table,
			#	then try to find the top level IDs they correspond to
			lIDs = self._get_ids([
				d[self._table._struct.key] for d in
				self._table.select(
					distinct = True,
					fields = [ self._table._struct.key ],
					where = dColumns
				)
			])

			# If we got nothing, we will always get nothing
			if not lIDs:
				return []

			# We got something, set the IDs
			lsIDs = set(lIDs)

		# If we have any complex
		if dComplex:

			# Go through each one
			for k in dComplex:

				# Call the child's filter
				lIDs = self._complex[k].filter(dComplex[k])

				# If we got nothing, we will always get nothing
				if not lIDs:
					return []

				# If we got any IDs
				if lIDs:

					# If we currently have none, this is the starting point
					if lsIDs is None:
						lsIDs = set(lIDs)

					# Else, only store the intersection of the previous IDs and
					#	the new IDs, we only want records with all the data
					else:
						lsIDs = lsIDs.intersection(lIDs)

		# If we got nothing
		if not lsIDs:
			return []

		# Get the parent's IDs or return them as is
		return self._parent and \
				self._parent.get_ids(list(lsIDs)) or \
				list(lsIDs)

	def get(self, _id: str | List[str]) -> dict | List[dict]:
		"""Get

		Retrieves all the rows associated with the given ID

		Arguments:
			_id (str | str[]): The ID to fetch rows for

		Returns:
			dict | dict[]
		"""

		# If we are getting a single set of rows
		if isinstance(_id, str):

			# If the level represents only a single node
			if self._node:

				# Find the records ordered by the levels
				lRows = self._table.select(
					where = { '_parent': _id },
					orderby = self._levels
				)

				# Now that we have all the data, split it up by the levels and
				#	return it
				return self._elevate(lRows)

			# Else, we have more complex types
			else:

				# Find the records ordered by the levels and store them by
				#	unique ID
				dRows = {d[self._table._struct.key]:d for d in self._table.select(
					where = { '_parent': _id },
					orderby = self._levels
				)}

				# Go through each complex record
				for f in self._complex:

					# For each row
					for sID in dRows:

						# Call the child get, passing along the ID, then store the
						#	results by that ID
						dRows[sID][f] = self._complex[f].get(sID)

				# Now that we have all the data, split it up by the levels and
				#	return it
				return self._elevate(
					list(dRows.values())
				)

		# Else, we are getting rows for multiple parents
		else:

			# Get all rows for all parents
			lRows = self._table.select(
				where = { '_parent': _id },
				orderby = self._levels
			)

			# Go through each complex record
			for f in self._complex:

				# Get all possible rows for all the IDs
				dComplex = self._complex[f].get([
					d[self._table._struct.key] for d in lRows
				])

				# Go through each row and add the values found for the complex
				#	node
				for d in lRows:
					try: d[f] = dComplex[d['_id']]
					except KeyError: pass

				# Clear the memory immediately
				del dComplex

			# Group the rows by the parent
			dParents = {}
			for d in lRows:
				try:
					dParents[d['_parent']].append(d)
				except KeyError:
					dParents[d['_parent']] = [d]

			# Now go through each list and elevate it
			for k in dParents:
				dParents[k] = self._elevate(dParents[k])

			# Return all the data
			return dParents

	def set(self,
		_id: str,
		data: dict,
		ta: Transaction = undefined
	) -> dict | list | None:
		"""Set

		Sets the rows associated with the given ID and returns the previous \
		rows that were overwritten if there's any changes

		Arguments:
			_id (str): The ID of the parent
			data (dict): A dict representing a structure of data to be set \
				under the given ID
			ta (Transaction): Optional, the open transaction to add new sql \
				statements to

		Returns:
			list | None
		"""

		# Set the local transaction
		lTA = self._table.transaction()

		# See if we have any existing rows
		lOldData = self._table.select(
			where = { '_parent': _id },
			orderby = self._levels
		)

		# If we do
		if lOldData:

			# Delete the existing data
			lTA.delete( where = { '_parent': _id })

			# Go through each complex field
			for f in self._complex:

				# Go through each record
				for i in range(len(lOldData)):

					# Delete the rows associated
					mRet = self._complex[f].delete(
						lOldData[i][self._table._struct.key]
					)
					if mRet:
						lOldData[i][f] = mRet

			# Elevate the old data
			lOldData = self._elevate(lOldData)

		# Flatten the passed in data
		lData = self._flatten(data)

		# Go through each one of the rows passed in
		for d in lData:

			# Create a new row with a new ID, the parent, and all the local
			#	columns that were passed in
			dRow = {
				self._table._struct.key: self._table.uuid(),
				'_parent': _id,
				**{ k: d[k] for k in self._keys if k in d },
				**{ k: d[k] for k in self._columns if k in d }
			}

			# Insert the row
			lTA.insert(dRow)

			# Go through each complex field
			for f in self._complex:

				# If we have data for the field
				if f in d:

					# Set any records
					self._complex[f].set(
						dRow[self._table._struct.key],
						d[f],
						lTA
					)

		# If we have a transaction passed in, extend it with ours
		if ta is not undefined:
			ta.extend(lTA)

		# Else, run everything
		else:

			# If we were not successful
			if not lTA.run():
				return None

		# Return the old data
		return lOldData or None

	def update(self,
		_id: str,
		data: list | dict,
		ta: Transaction = undefined
	) -> list | None:
		"""Update

		Updates the rows associated with the given ID and returns the previous \
		ows that were overwritten if there's any changes

		Arguments:
			_id (str): The ID to update records for
			data (list | dict): A list or dict representing a structure of \
				data to be updated under the given ID
			ta (Transaction): Optional, the open transaction to add new sql \
				statements to

		Returns:
			list | None
		"""

		# Set the local transaction
		lTA = self._table.transaction()

		# Flatten the values recieved so we can compare them to the table rows
		lData = self._flatten(data)

		# If it's a single node table
		if self._node:

			# Get the existing values
			lValues = self._table.select(
				fields = [*self._levels, '_value'],
				orderby = self._levels,
				where = { '_parent': _id }
			)

			# If the data is not the same
			if not compare(lValues, lData):

				# Delete all rows associated with the parent
				lTA.delete({ '_parent': _id })

				# Go through each new row
				for d in lData:

					# Generate the SQL to insert the row with the parent ID
					lTA.insert( values = combine(
						d, { '_parent': _id }
					))

				# If we have a transaction passed in, extend it with ours
				if ta is not undefined:
					ta.extend(lTA)

				# Else, run everything
				else:

					# If we were not successful
					if not lTA.run():
						return None

				# Return the old records
				return self._elevate(lValues)

		# Else, we have a multi-value table
		else:

			# Store the old records
			lOldData = self._table.select(
				where = { '_parent': _id },
				orderby = self._levels
			)

			# Init the list of IDs to delete
			lToDelete = []

			# Go through each row of the old data
			for i in range(len(lOldData)):

				# If there's no corresponding record in the data
				if not next(o for o in data \
							if d[self._table._struct.key] == lOldData[i][self._table._struct.key]
				):

					# Store the ID
					lToDelete.append(lOldData[i][self._table._struct.key])

					# Go through each complex field
					for f in self._complex:
						mTemp = self._complex[f].delete(
							lOldData[i][self._table._struct.key],
							lTA
						)
						if mTemp:
							lOldData[i][f] = mTemp

			# If there's any to delete
			if lToDelete:
				self._table.delete({ self._table._struct.key: lToDelete })

			# Init the list of IDs with array swaps
			lSwapIDs = set()
			lSwapFields = set()

			# Go through each "row" passed
			for d in lData:

				# Find the index of the old data
				i = -1
				if self._table._struct.key in d:
					i = lfindi(
						lOldData,
						self._table._struct.key,
						d[self._table._struct.key]
					)

				# If it has a valid ID
				if i > -1:

					# Init the fields to update
					dUpdate = {}

					# Go through each level
					for s in self._levels:

						# If it's an array
						if s[1:2] == 'a':

							# If data doesn't match, the record has moved
							#	somewhere down the line
							if d[s] != lOldData[i][s]:

								# Store the value as it's opposite and store the
								# ID so we know to fix it later
								dUpdate[s] = -d[s]
								lSwapIDs.add(d[self._table._struct.key])
								lSwapFields.add(s)

						# Else, if it's a hash
						elif s[1:2] == 'h':

							# And a the value has changed
							if d[s] != lOldData[i][s]:
								raise ValueError(
									'define.Hash keys associated with an _id ' \
									'can not be changed. Change the values ' \
									'instead'
								)

					# Go through each possible field of the actual data
					for f in self._columns:

						# If the field exists in the data
						if f in d:

							# If the value doesn't exist in the existing
							#	data, or it does but it's different
							if f not in lOldData[i] or \
								lOldData[i][f] != d[f]:

								# Update the field
								dUpdate[f] = d[f]

					# If we have anything to update
					if dUpdate:

						# Add it to the transaction
						lTA.update(dUpdate, {
							self._table._struct.key: d[self._table._struct.key]
						})

					# Go through each complex
					for f in self._complex:

						# Try to update the data
						mTemp = self._complex[f].update(
							d[self._table._struct.key],
							d[f],
							lTA
						)
						if mTemp:
							lOldData[i][f] = mTemp

				# If it doesn't have an ID, or the ID is invalid, it's going to
				#	be a new row
				else:

					# Generate a unique ID and set it
					d[self._table._struct.key] = self._table.uuid()

					# Add the parent
					d['_parent'] = _id

					# Add the create to the transactions
					lTA.insert(d)

			# If we had any swaps
			if lSwapIDs:

				# Add the swap statement
				lTA.append("UPDATE `%s`.`%s` SET %s WHERE `_id` IN ('%s')" % (
					self._table._struct.db,
					self._table._struct.name,
					', '.join([
						'`%(s)s` = ABS(`%(s)s`)' % {'s': s} for s in lSwapFields
					]),
					"','".join(lSwapIDs)
				))

			# Remove the ID and parent and then elevate the old data before
			#	returning it
			if lOldData:
				lOldData = self._elevate(
					without(lOldData, [self._table._struct.key, '_parent'])
				)

		# If we have a transaction passed in, extend it with ours
		if ta is not undefined:
			ta.extend(lTA)

		# Else, run everything
		else:

			# If we were not successful
			if not lTA.run():
				return None

		# Return the old data
		return lOldData or None

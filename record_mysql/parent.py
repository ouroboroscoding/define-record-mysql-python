# coding=utf8
"""Record Parent

Handles a structure which contains other nodes, including other sub-structures
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-03"

# Limit exports
__all__ = ['Parent']

# Ouroboros imports
import define
from jobject import jobject
from tools import merge, without

# Local imports
from record_mysql.base import Base
from record_mysql.table import Table
from record_mysql.transaction import Transaction

class Parent(Base):
	"""Row

	Represents a define Parent that contains other Nodes by name

	Extends:
		Base
	"""

	def __init__(self, name: str, parent: Base, details: define.Parent):
		"""Constructor

		Creates a new instance

		Arguments:
			name (str): The name of the structure in its parent, if it has one
			parent (Base): The instance of the parent this struct belongs to
			details (define.Parent): The define.Parent associated

		Returns:
			Parent
		"""

		# Call the Base constructor
		super(Parent, self).__init__(name, parent)

		# If we have a parent
		if parent:

			# Overwrite this instance's _get_ids method with the parent's one
			self._get_ids = parent._get_ids

		# Step through each one of the children in the details
		for f in details:

			# Get the class name
			sClass = details[f].class_name()

			# If it's a Node
			if sClass == 'Node':

				# Add the node to the columns list under its name
				self._columns[f] = details[f]

			# Else, it's a complex type
			else:

				# Store it for later
				self._complex[f] = sClass

		# If we have any columns
		if self._columns:

			# Init dParent just in case
			dParent = None

			# If there's a parent, add the ID to the keys
			if self._parent:

				# Get its structure
				dParent = self._parent.struct()

				# Add the ID to make the connection
				self._keys[dParent.key] = define.Node({
					'__type__': 'uuid'
				})

			# Init the base structure
			dStruct = jobject({
				'auto_key': False,
				'create': [
					*self._keys.keys(),
					*self._columns.keys()
				],
				'db': 'test',
				'key': '_id',
				'host': '_',
				'indexes': [],
				'revisions': False,
				'name': name
			})

			# Get the __mysql__ section
			dMySQL = details.special('mysql')

			# If there's a parent
			if self._parent:

				# Update the base data
				dStruct.db = dParent.db
				dStruct.host = dParent.host
				dStruct.key = dParent.key
				dStruct.name = '%s_%s' % (dParent.name, name)

			# If there's a special section, overwrite any values it has with
			#	the created ones
			dMySQL = details.special('mysql')
			if dMySQL:

				# If there's any additional indexes
				if 'indexes' in dMySQL:

					# Remove them and use them to extend the main struct
					dStruct.indexes.extend(
						dMySQL.pop('indexes')
					)

				# Merge whatever remains
				merge(dStruct, dMySQL)

			# Create a new columns with the ID
			dColumns = {**self._keys, **self._columns}

			# Create a table for them using the generate structure and the list
			#	of columns
			self._table = Table(
				dStruct,
				dColumns
			)

		# Go through the complex types
		for f in self._complex:

			# Try to create it and store it under its name
			self._complex[f] = self.create_type(
				self._complex[f],
				f,
				self,
				details[f]
			)

	def _get_ids(self, ids: list[str]) -> list[str]:
		"""Get IDs

		Returns the IDs associated with the ones given

		Arguments:
			ids (str[]): The IDs to find the parents for

		Returns:
			str[]
		"""

		# If this method is running it is because it has not been over written
		#	in the contructor by its parent's _get_ids method. If that is the
		#	case, then we are at the top of the structure, and whatever IDs that
		#	are passed to us, are the top most IDs, and can be returned as is.
		return ids

	def delete(self,
		_id: str,
		ta: Transaction = None
	) -> list | dict | None:
		"""Delete

		Deletes the row associated with the given ID and returns it

		Arguments:
			_id (str): The unique ID associated with rows to be deleted
			ta (Transaction): Optional, the open transaction to add new sql
				statements to

		Returns:
			dict | None
		"""

		# Init the return
		dOldData = {}

		# Init the local transaction
		lTA = []

		# If we have a table
		if self._table:

			# Create a transaction using our table
			lTA = self._table.transaction()

			# Get the existing data without special keys
			dOldData = self._table.select(
				fields = self._columns.keys(),
				where = { self._table._struct.key : _id },
				limit = 1
			)

			# If we got a record
			if dOldData:

				# Delete it
				lTA.delete(where = { self._table._struct.key: _id })

		# Go through each complex
		for f in self._complex:

			# Tell the child to delete the same ID, and pass our transaction
			#	list to it
			mTemp = self._complex[f].delete(_id, lTA)
			if mTemp:
				dOldData[f] = mTemp

		# If we have a transaction passed in, extend it with ours
		if ta:
			ta.extend(lTA)

		# Else, run everything
		else:

			# If we were not successful
			if not lTA.run():
				return None

		# Return the data
		return dOldData or None

	def filter(self, filter: dict) -> list[str]:
		"""Filter

		Returns the top level IDs filtered by the given field/value pairs

		Arguments:
			values (dict): The field and value pairs to filter by

		Returns:
			str[]
		"""

		# Find the IDs of the records with the given filter in the table, then
		#	try to find the top level IDs they correspond to
		return self.get_ids([
			d[self._table._struct.key] for d in
			self._table.select(
				distinct = True,
				fields = self._table._struct.key,
				where = filter
			)
		])

	def get(self, _id: str) -> list[dict]:
		"""Get

		Retrieves all the rows associated with the given ID

		Arguments:
			id (str): The ID to fetch rows for

		Returns:
			dict[]
		"""

		# Init the return value
		dRet = {}

		# If we have a table
		if self._table:

			# Find the rows
			dRow = self._table.select(
				where = { self._table._struct.key: _id },
				limit = 1
			)

			# If we got anything, update the return with the row minus the ID
			if dRow:
				dRet.update(without(dRow, self._table._struct.key))

		# Go through each complex record
		for f in self._complex:

			# Call the child get, passing along the ID, and store the results
			mComplex = self._complex[f].get(_id)

			# If we got anything, add it to the return
			if mComplex:
				dRet[f] = mComplex

		# Return the row data
		return dRet

	def set(self,
		_id: str,
		data: dict,
		ta: Transaction | None = None
	) -> dict | list | None:
		"""Set

		Sets the row associated with the given ID and returns the previous row \
		that was overwritten if there's any changes

		Arguments:
			_id (str): The ID of the parent
			data (dict): A dict representing a structure of data to be set \
				under the given ID
			ta (Transaction): Optional, the open transaction to add new sql \
				statements to

		Returns:
			dict | None
		"""

		# Init the return
		dOldData = {}

		# Set the local transaction by creating one from the table, or if there
		#	isn't one, use an empty list, we can pass it up either way
		try:
			lTA = self._table.transaction()
		except AttributeError:
			lTA = []

		# Go through each one of the columns to see if any are present
		bTableCheck = False
		for f in self._columns:
			if f in data:
				bTableCheck = True
				break

		# If any field in the table associated with this instance is present
		if bTableCheck:

			# Fetch the record associated with the ID
			dOldData = self._table.select(
				fields = self._columns.keys(),
				where = { self._table._struct.key: _id },
				limit = 1
			)

			# If it exists
			if dOldData:

				# Update the row in the table
				lTA.update(without(data, self._keys.keys()), {
					self._table._struct.key: _id
				})

			# Else, it's a new row
			else:

				# Create a new record from the data using the passed ID
				lTA.insert({
					**{ self._table._struct.key: _id },
					**without(data, list(self._keys.keys()))
				})

		# Go through each complex field
		for f in self._complex:

			# If we have any data
			if f in data:

				# Call its set method
				mRet = self._complex[f].set(
					_id,		# The same ID we got
					data[f],	# The values for that specific field
					lTA			# Our transaction object
				)

				# If we were successful
				if mRet:
					dOldData[f] = mRet

		# If we have a transaction passed in, extend it with ours
		if ta:
			ta.extend(lTA)

		# Else, run everything
		else:

			# If we were not successful
			if not lTA.run():
				return None

		# Return the old data
		return dOldData

	def update(self,
		_id: str,
		data: dict,
		ta: Transaction | None = None
	) -> dict | None:
		"""Update

		Updates the row associated with the given ID and returns the previous \
		row that was updated if there's any changes

		Arguments:
			_id (str): The ID to update records for
			data (list | dict): A list or dict representing a structure of \
				data to be updated under the given ID
			ta (Transaction): Optional, the open transaction to add new sql \
				statements to

		Returns:
			dict | None
		"""

		# Init the return
		dOldData = {}

		# Set the local transaction by creating one from the table, or if there
		#	isn't one, use an empty list, we can pass it up either way
		try:
			lTA = self._table.transaction()
		except AttributeError:
			lTA = []

		# Go through each one of the columns to see if any are present
		bTableCheck = False
		for f in self._columns:
			if f in data:
				bTableCheck = True
				break

		# If any field in the table associated with this instance is present
		if bTableCheck:

			# Make a copy of the data without the complex fields
			dData = without(data, list(self._complex.keys()))

			# Fetch the record associated with the ID
			dOldData = self._table.select(
				fields = self._columns.keys(),
				where = { self._table._struct.key: _id },
				limit = 1
			)

			# If we have the row
			if dOldData:

				# Keep a list of changes
				dUpdates = {}

				# Go through each possible field
				for f in self._columns:

					# If the field exists in the values
					if f in data:

						# If the value doesn't exist in the existing data, or it
						#	does but it's different
						if f not in dOldData or dOldData[f] != data[f]:
							dUpdates[f] = data[f]

				# If we have anything left to update
				if dUpdates:

					# Add the update to the transactions
					lTA.update(dUpdates, {
						self._table._struct.key: _id
					})

			# Else, we already have the row
			else:

				# Create a new record from the data using the passed ID
				lTA.insert({
					**{ self._table._struct.key: _id },
					**without(data, self._keys.keys())
				})

		# Go through each complex field
		for f in self._complex:

			# If it exists in the data
			if f in data:

				# Call its update method
				mRet = self._complex[f].update(
					_id,		# The same ID we got
					data[f],	# The values for that specific field
					lTA			# Our transaction object
				)

				# If we were successful
				if mRet:
					dOldData[f] = mRet

		# If we have a transaction passed in, extend it with ours
		if ta:
			ta.extend(lTA)

		# Else, run everything
		else:

			# If we were not successful
			if not lTA.run():
				return None

		# Return success or failure
		return dOldData

# Add the Parent type to the base types as 'Parent'
Parent.add_type('Parent')
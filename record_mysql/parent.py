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

# Python imports
from copy import copy

# Pip imports
import define
from jobject import jobject
from record.types import Limit
from tools import merge, without

# Local imports
from .base import Base
from .table import Table
from .transaction import Transaction

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
			sClass = details[f].class_name

			# If it's a Node
			if sClass == 'Node':

				# Add the node to the columns list under its name
				self._columns[f] = details[f]

			# Else, it's a complex type
			else:

				# Try to create it and store it under its name
				self._complex[f] = self.create_type(
					sClass,
					f,
					self,
					details[f]
				)

		# If we have any columns
		if self._columns:

			# Add the ID to the keys
			self._keys['_id'] = define.Node({ '__type__': 'uuid' })

			# Get the __mysql__ section
			dMySQL = details.special('mysql')

			# If there's a parent
			if self._parent:

				# Get its structure
				dParent = self._parent.struct()

				# Init the structure
				dStruct = jobject({
					'auto_key': False,
					'create': [
						*self._keys.keys(),
						*self._columns.keys()
					],
					'db': dParent.db,
					'host': dParent.host,
					'indexes': [],
					'key': '_id',
					'revisions': False,
					'name': '%s_%s' % (dParent.name, name)
				})

				# If there's a special section, overwrite any values it has
				#	with the created ones
				if dMySQL:
					merge(dStruct, dMySQL)

				# Create a new columns with the ID
				dColumns = {**self._keys, **self._columns}

			# Else, no parent, use config and columns as is
			else:
				dStruct = dMySQL
				dColumns = self._columns

			# Create a table for them using the generate structure and the list
			#	of columns
			self._table = Table(
				dStruct,
				dColumns
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
			d['_id'] for d in
			self._table.select(
				distinct = True,
				fields = '_id',
				where = filter
			)
		])

	def get(self, id: str) -> list[dict]:
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
				where = { '_id': id },
				limit = (1,)
			)

			# If we got anything, update the return with the row minus the ID
			if dRow:
				dRet.update(without(dRow, '_id'))

		# Go through each complex record
		for f in self._complex:

			# Call the child get, passing along the ID, and store the results
			dRet[f] = self._complex[f].get(id)

		# Return the row data
		return dRet

	def set(self,
		id: str,
		values: dict,
		return_revisions: bool,
		ta: Transaction = None
	) -> bool:
		"""Set

		Sets the row associated with the given ID and returns True if the row
		is inserted or updated. If `return_revisions` is True, the revisions
		dict will be returned instead of True. In all cases, nothing being set
		returns False

		Arguments:
			id (str): The ID of the parent
			values (dict): A dict representing a structure of data to be set
							under the given ID
			return_revisions (bool): Optional, if True returns a structure of
									values changed instead of True
			ta (Transaction): The Transaction instance to add statements to

		Returns:
			dict | bool
		"""

		# Init the return
		if return_revisions:
			mRet = {}
		else:
			mRet = False

		# Set the local transaction by creating one from the table, or if there
		#	isn't one, use an empty list, we can pass it up either way
		try:
			lTA = self._table.transaction()
		except AttributeError:
			lTA = []

		# Go through each one of the columns to see if any are present
		bTableCheck = False
		for f in self._columns:
			if f in values:
				bTableCheck = True
				break

		# Init the number of fields that changed and that also returned simple
		# old / new revisions indicating their entire value changed
		iFullChange = 0

		# If any field in the table associated with this instance is present
		if bTableCheck:

			# Make a copy of the values without the complex fields
			dValues = without(values, self._complex.keys())

			# Set any missing values to None
			for f in self._columns:
				if f not in dValues:
					dValues = None

			# Fetch the record associated with the ID
			dRow = self._table.select(
				where = { '_id': id },
				limit = (1,)
			)

			# If it doesn't exist
			if not dRow:

				# Create a new record from the data
				lTA.insert(dValues)

				# Set the return
				if return_revisions:
					for f in self._columns:
						mRet[f] = { 'old': None, 'new': dValues[f] }
						iFullChange += 1
				else:
					mRet = True

			# Else, we already have the row
			else:

				# Go through each column
				for f in self._columns:

					# If the values are different
					if dRow[f] != values[f]:

						# Set the return
						if return_revisions:
							mRet[f] = { 'old': dRow[f], 'new': dValues[f] }
							iFullChange += 1
						else:
							mRet = True

					# Else, remove the field from the values
					else:
						del dValues[f]

				# If there's anything left to update
				if dValues:

					# Update the row in the table
					lTA.update(dValues, {
						'_id': id
					})

		# Go through each complex field
		for f in self._complex:

			# Call its set method
			mComplexRet = self._complex[f].set(
				id,					# The same ID we got
				values[f],			# The values for that specific field
				return_revisions,	# The same return_revisions we got
				lTA					# Our transaction object
			)

			# If we were successful
			if mComplexRet:

				# If we want revisions, add them
				if return_revisions:
					mRet[f] = mComplexRet

					# If the revisions indicate the entire value changed
					if 'old' in mComplexRet and 'new' in mComplexRet:
						iFullChange += 1

				# Else, just mark that something was set
				else:
					mRet = True

		# If we want revisions
		if return_revisions:

			# If every single field in the instance changed completely
			if iFullChange == (len(self._columns) + len(self._complex)):

				# Reorder the result
				dReorder = { 'old': {}, 'new': {} }
				for f in mRet:
					dReorder['old'][f] = mRet[f]['old']
					dReorder['new'][f] = mRet[f]['new']
				mRet = dReorder

			# Else, if we have nothing, set the return to False
			elif not mRet:
				mRet = False

		# If we have a transaction passed in, extend it with ours
		if ta:
			ta.extend(lTA)

		# Else, run everything
		else:

			# If we were not successful
			if not lTA.run():
				return False

		# Return success or failure
		return mRet

	def update(self,
		id: str,
		values: dict,
		return_revisions: bool,
		ta: Transaction = None,
	) -> dict | bool:
		"""Update

		Updates the record associated with the given ID and returns True if
		something was updated. If `return_revisions` is True, the revisions dict
		will be returned instead of True. In all cases, nothing being updated
		returns False

		Arguments:
			id (str): The ID to update records for
			values (dict): A dict representing a structure of data to be
							updated under the given ID
			return_revisions (bool): if True returns a structure of values
										changed instead of True
			ta (Transaction): The Transaction instance to add statements to

		Returns:
			dict | bool
		"""

		# Init the return
		if return_revisions:
			mRet = {}
		else:
			mRet = False

		# Set the local transaction by creating one from the table, or if there
		#	isn't one, use an empty list, we can pass it up either way
		try:
			lTA = self._table.transaction()
		except AttributeError:
			lTA = []

		# Go through each one of the columns to see if any are present
		bTableCheck = False
		for f in self._columns:
			if f in values:
				bTableCheck = True
				break

		# Init the number of fields that changed and that also returned simple
		# old / new revisions indicating their entire value changed
		iFullChange = 0

		# If any field in the table associated with this instance is present
		if bTableCheck:

			# Make a copy of the values without the complex fields
			dValues = without(values, self._complex.keys())

			# Fetch the record associated with the ID
			dRow = self._table.select(
				where = { '_id': id },
				limit = (1,)
			)

			# If we don't have a row
			if not dRow:

				# Create a new record from the data
				lTA.insert(dValues)

				# Set the return
				if return_revisions:
					for f in self._columns:
						mRet[f] = { 'old': None, 'new': dValues[f] }
						iFullChange += 1
				else:
					mRet = True

			# Else, we already have the row
			else:

				# Go through each possible field
				for f in self._columns:

					# If the field exists in the values
					if f in dValues:

						# If the value doesn't exist in the existing data, or it
						#	does but it's different
						if f not in dRow or dRow[f] != dValues[f]:
							if return_revisions:
								mRet[f] = {
									'old': f in dRow and dRow[f] or None,
									'new': dValues[f]
								}
							else:
								mRet = True

						# Else,
						else:
							del dValues[f]

				# If we have anything left to update
				if dValues:

					# Add the update to the transactions
					lTA.update(dValues, { '_id': id })

		# Go through each complex field
		for f in self._complex:

			# Call its update method
			mComplexRet = self._complex[f].update(
				id,					# The same ID we got
				values[f],			# The values for that specific field
				return_revisions,	# The same return_revisions we got
				lTA					# Our transaction object
			)

			# If we were successful
			if mComplexRet:

				# If we want revisions, add them
				if return_revisions:
					mRet[f] = mComplexRet

					# If the revisions indicate the entire value changed
					if 'old' in mComplexRet and 'new' in mComplexRet:
						iFullChange += 1

				# Else, just mark that something was set
				else:
					mRet = True

		# If we want revisions
		if return_revisions:

			# If every single field in the instance changed completely
			if iFullChange == (len(self._columns) + len(self._complex)):

				# Reorder the result
				dReorder = { 'old': {}, 'new': {} }
				for f in mRet:
					dReorder['old'][f] = mRet[f]['old']
					dReorder['new'][f] = mRet[f]['new']
				mRet = dReorder

			# Else, if we have nothing, set the return to False
			elif not mRet:
				mRet = False

		# If we have a transaction passed in, extend it with ours
		if ta:
			ta.extend(lTA)

		# Else, run everything
		else:

			# If we were not successful
			if not lTA.run():
				return False

		# Return success or failure
		return mRet

# Add the Parent type to the base
Base.add_type('Parent')
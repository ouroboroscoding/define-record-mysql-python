# coding=utf8
"""Record Storage

Extends the record.Storage class to add MySQL / MariaDB capabilities
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-01"

# Limit exports
__all__ = ['Storage']

# Ouroboros imports
from record import Cache, CONFLICT, Data, Storage as _Storage
from record.exceptions import RecordStorageException
import undefined

# Python imports
from sys import stderr
from typing import List

# Local imports
from record_mysql.parent import Parent
from record_mysql.leveled import Leveled

# Add the Parent, Array, and Hash types to the base
Parent.add_type('Parent')
Leveled.add_type('Array')
Leveled.add_type('Hash')

class Storage(_Storage):
	"""Storage

	Represents the top level definition of one or more tables in a specific \
	database

	Extends record.Storage in order to add inserting, deleting, updating, and \
	selecting SQL rows

	Extends:
		record.Storage
	"""

	def __init__(self,
		details: dict | str,
		extend: dict = undefined,
		key_name: str = '_id'
	):
		"""Constructor

		Creates a new instance of a single table, or in the case of complex \
		records, multiple different tables, that contribute to storing and \
		retrieving records

		Arguments:
			details (dict | str): Definition or filepath to load
			extend (dict | False): Optional, a dictionary to extend the \
				definition
			key_name (str): Optional, the name of the primary key, defaults to \
				_id

		Raises:
			KeyError, ValueError

		Returns
			Storage
		"""

		# Call the parent constructor
		super().__init__(details, extend, key_name)

		# Create the top level parent for the record
		self._parent = Parent(self._name, key_name, self)

		# Store the key field name
		self._key = key_name

		# If the key name was overwritten in the special values, then we need
		#	to store the global one
		if self._parent._table._struct.key != key_name:
			self._key = self._parent._table._struct.key

		# Get the cache section
		oCache = self.special('cache', {
			'implementation': False
		})

		# If cache is enabled
		self._cache = oCache['implementation'] and \
			Cache.factory(self._name, oCache) or \
			False

	def add(self,
		value: dict,
		conflict: CONFLICT = 'error',
		revision_info: dict = undefined
	) -> str | list:
		"""Add

		Adds one raw record to the storage system

		Arguments:
			value (dict): A dictionary of fields to data
			conflict (CONFLICT): A string describing what to do in the case of \
				a conflict in adding the record
			revision_info (dict): Optional, additional information to store \
				with the revision record

		Raises:
			RecordDuplicate
			RecordServerException
			RecordStorageException
			ValueError

		Returns:
			The ID of the added record
		"""

		# If we have no key
		if self._key not in value:

			# Create and add one
			value[self._key] = self.uuid()

		# Validate the data
		if not self.valid(value):
			raise ValueError(self._validation_failures)

		# Create a transaction
		lTA = self._parent._table.transaction()

		# Take the incoming data, and pass it to the parent to set
		mData = self._parent.set(value[self._key], value, lTA)

		# If we store revisions
		if self._parent._table._struct.revisions:

			# If we have old data
			if mData:

				# Generate the revisions in the data
				dRevisions = self.revision_generate(mData, value)

			# Else, revisions are simple
			else:
				dRevisions = { 'old': None, 'new': value }

			# If revisions requires fields
			if isinstance(self._parent._table._struct.revisions, list):

				# If they weren't passed
				if not isinstance(revision_info, dict):
					raise ValueError('revision')

				# Else, add the extra fields
				for f in self._parent._table._struct.revisions:
					dRevisions[f] = revision_info[f]

			# Generate the SQL to add the revision record to the table and add
			#	it to the transaction list
			lTA.append(
				self._parent._table.revision_add(value[self._key], dRevisions)
			)

		# Run the transactions
		if not lTA.run():
			return None

		# If we have a cache
		if self._cache:

			# Get the actual data from the database
			dRecord = self._parent.get(value[self._key])

			# Store that data in the cache
			self._cache.set(value[self._key], dRecord)

		# Return the ID of the new record
		return value[self._key]

	def count(self,
		filter: dict = None
	) -> int:
		"""Count

		Returns the count of records, with or without a filter

		Arguments:
			filter (dict): Optional, data to filter the count of records by

		Returns:
			int
		"""
		return self._parent.count(filter)

	def exists(self, _id: str, index = undefined) -> bool:
		"""Exists

		Returns true if a record with the given ID exists

		Arguments:
			_id (str): The unique ID of the record to check for
			index (str): Optional, the name of the field to check against, \
				works best if the field is a unique field/index

		Returns:
			bool
		"""

		# If we got no index
		if index is undefined:
			index = self._key

		# Else, verify the index
		else:

			# If the given index doesn't exist, or is not unique
			if index not in self._parent._table._struct.indexes or \
				self._parent._table._struct.indexes.type != 'UNIQUE':

				# Raise an error
				raise RecordStorageException(
					'exists `index` must be a unique index in the table'
				)

		# Call the table directly and store the result
		mResult = self._parent._table.select(
			fields = [ self._key ],
			where = { index: _id }
		)

		# If we have multiple IDs
		if isinstance(_id, list):

			# Return true only if the counts match
			return len(_id) == len(mResult)

		# Else, return based on the result
		return mResult and True or False

	def filter(self,
		fields: dict,
		raw: bool | List[str] = False,
		options: dict = None
	) -> List[Data] | List[dict]:
		"""Filter

		Gets records based on specific data fields

		Arguments:
			fields (dict): Field and values to filter the data by
			raw (bool | str[]): Return raw data instead of Data instances
			options (dict): Custom options processed by the storage system

		Returns:
			Data[] | dict[]
		"""

		# Init the records
		lRecords = None

		# Call the parents filter in order to get the IDs
		lIDs = self._parent.filter(fields)

		# If we got anything
		if lIDs:

			# If all we wanted was IDs
			if raw and \
				raw is not True and \
				len(raw) == 1 and \
				raw[0] == self._key:

					# Return the IDs
					return [ { self._key: id_ } for id_ in lIDs ]

			# If we have a cache
			if self._cache:

				# Try to get them from the cache
				lRecords = self._cache.fetch(lIDs)

				# Go through each record by index
				for i in range(len(lRecords)):

					# If we didn't get the record
					if lRecords[i] is None:

						# Fetch it from the system
						dRecord = self._parent.get(lIDs[i])

						# If it doesn't exist
						if not dRecord:

							# Mark it as missing so we don't overload the
							#	system. Any future requests will return the
							#	record as False
							self._cache.add_missing(lIDs[i])

						# Else, we have it
						else:

							# Store it for next time
							self._cache.set(lIDs[i], dRecord)

					# Else, if it's False, set it to None and move on, we
					#	know this record does not exist
					elif lRecords[i] == False:
						lRecords[i] = None

			# Else, we have no cache
			else:

				# Get the full record for each ID
				lRecords = [
					self._parent.get(sID) \
					for sID in lIDs
				]

		# If we have nothing, return nothing
		if not lRecords:
			return []

		# If we want the records as is
		if raw:
			if raw is True:
				return lRecords
			else:
				return [
					{ k:v for k,v in d.items() if k in raw } \
					for d in lRecords
				]

		# Return a new Data
		return [m and Data(self, m) or None for m in lRecords]

	def get(self,
		_id: str | List[str] = undefined,
		index = undefined,
		raw = False,
		options: dict = undefined
	) -> Data | List[Data] | dict | List[dict]:
		"""Get

		Gets one, many, or all records from the storage system associated with \
		the class instance through checks against IDs, either primary, no \
		`index`, or secondary by passing the name to `index`. Passing no \
		arguments at all will return every record. Setting raw to True, or a \
		list of fields, will return a dict or dicts instead of Data objects

		Arguments:
			_id (str | str[] | tuple | tuple[]): The ID or IDs used to get the \
				records. Don't set to get all records
			index (str): The name of the index to use to fetch the data \
				instead of the primary key
			raw (bool | str[]): Return raw data instead of Data instances
			options (dict): Custom options processed by the storage system

		Returns:
			Data | Data[] | dict | dict[]
		"""

		# Init the Records and IDs
		lRecords = None
		lIDs = None

		# If there's no IDs
		if _id is undefined:

			# If we have no cache
			if not self._cache:

				# If we have no complex data
				if not self._parent._complex:

					# Store all records
					lRecords = self._parent._table.select()

		# Else, we have IDs
		else:
			lIDs = _id

		# If we don't have records
		if not lRecords:

			# If we don't have IDs
			if not lIDs:

				# Fetch all the IDs
				lIDs = self._parent._table.select(
					fields = [ self._key ]
				)

				# If nothing exists
				if not lIDs:
					return []

				# Flattern the list
				lIDs = [ d[self._key] for d in lIDs]

			# If we have just one
			if isinstance(lIDs, (str, tuple)):

				# Init the record
				dRecord = None

				# If we have a cache
				if self._cache:

					# Try to get it from the cache
					dRecord = self._cache.get(lIDs, index = index)

				# If we don't have the record
				if not dRecord:

					# If we have an index
					if index is not undefined:
						sID = self._get_secondary(lIDs, index)

					# Else, no index
					else:
						sID = lIDs

					# Fetch it from the system
					dRecord = self._parent.get(sID)

					# If it doesn't exist
					if not dRecord:
						return None

					# If we have a cache, and we have the record
					if self._cache and dRecord:

						# Store it in the cache under the ID
						self._cache.set(lIDs, dRecord)

				# If we want the record as is
				if raw:
					if raw is True:
						return dRecord
					else:
						return { k:v for k,v in dRecord.items() if k in raw }

				# Return a new Data
				return Data(self, dRecord)

			# If we have a cache
			if self._cache:

				# Try to get them from the cache
				lRecords = self._cache.get(lIDs, index = index)

				# Go through each record by index
				for i in range(len(lRecords)):

					# If we didn't get the record
					if lRecords[i] is None:

						# If we have an index
						if index is not undefined:
							sID = self._get_secondary(lIDs[i], index)

						# Else, no index
						else:
							sID = lIDs[i]

						# Fetch it from the system
						dRecord = self._parent.get(sID)

						# If it doesn't exist
						if not dRecord:

							# Mark it as missing so we don't overload the
							#	system. Any future requests will return the
							#	record as False
							self._cache.add_missing(sID)

						# Else, we have it
						else:

							# Store it for next time
							self._cache.set(sID, dRecord)

						# Set the record
						lRecords[i] = dRecord

					# Else, if it's False, set it to None and move on, we know
					#	this record does not exist
					elif lRecords[i] == False:
						lRecords[i] = None

			# Else, we have no cache
			else:

				# If an index was passed
				if index is not undefined:
					raise ValueError(
						'Record-MySQL does not currently support getting ' \
						'multiple Records by secondary index without a cache'
					)

				# If we have no complex, or are not looking for complex
				if not self._parent._complex or (
					raw and any(f not in self._parent._complex for f in raw) ):

					# Fetch and store all records by ID
					lRecords = self._parent._table.select(
						fields = raw or undefined,
						where = { self._key: lIDs }
					)

				# Else, get each record one at a time and add it to the list
				#	whether it exists or not. We print a warning because this
				#	really shouldn't be done.
				else:
					print(
						'WARNING: Fetching complex data with no cache',
						file = stderr
					)
					for sID in lIDs:
						lRecords.append(self._parent.get(sID))

		# If we have nothing, return nothing
		if not lRecords:
			return []

		# If we want the records as is
		if raw:
			if raw is True:
				return lRecords
			else:
				return [
					{ k:v for k,v in d.items() if k in raw } \
					for d in lRecords
				]

		# Return a new Data
		return [m and Data(self, m) or None for m in lRecords]

	def _get_secondary(self, _id: any, index: str) -> str:
		"""_ Get Secondary

		Gets the ID associated with a secondary unique index

		Arguments:
			_id (any): The value(s) used to make the unique index
			index (str): The name of the index to fetch

		Returns:
			mixed
		"""

		try:

			# Get the index from the parent
			dIndex = self._parent._table._struct.indexes[index]

		# If the index doesn't exist in the record
		except KeyError:
			raise IndexError(
				index,
				'Cache index "%s" does not exist in ' \
				'MySQL indexes' % index
			)

		# If it's not a unique index
		if dIndex['type'] != 'UNIQUE':
			raise IndexError(
				index,
				'Cache index "%s" is not UNIQUE in MySQL' %
					index
			)

		# If the indexes "fields" are a single string
		if len(dIndex['fields']) == 1:

			# If the value passed is a tuple, something is
			#	wrong
			if isinstance(_id, tuple):
				raise IndexError(
					index,
					'Index "%s" requires only one field ' \
					'but a tuple was passed' % index
				)

			# Create the filter using the one field
			dWhere = { dIndex['fields'][0]: _id }

		# Else, the index has multiple fields
		else:

			# If we didn't get a tuple
			if not isinstance(_id, tuple):
				raise IndexError(
					index,
					'Index "%s" requires multiple fields ' \
					'but no tuple was passed' % index
				)

			# If the counts do not match
			if len(_id) != len(dIndex['fields']):
				raise IndexError(
					index,
					'Index "%s" requires %d fields but' \
					'only received %d' % (
						index,
						len(dIndex['fields']),
						len(_id)
					)
				)

			# Init the filter
			dWhere = {}

			# Go through each field and add it
			for i in range(len(_id)):
				dWhere[dIndex['fields'][i]] = _id[i]

		# Fetch the ID using the filter
		dRecord = self._parent._table.select(
			fields = [ self._key ],
			where = dWhere,
			limit = 1
		)

		# If we got a record, return the primary key
		if dRecord:
			return dRecord[self._key]

		# Else, return nothing
		return None

	def insert(self,
		value: dict | list = {},
		conflict: str = 'error',
		revision_info: dict = undefined
	) -> Data | list:
		"""Insert

		Creates a new data object associated with the Storage instance

		Arguments:
			value (dict|dict[]): The initial values to set for the record
			conflict (str|list): Must be one of 'error', 'ignore', 'replace', \
				or a list of fields to update
			revision_info (dict): Optional, additional information to store \
				with the revision record

		Raises:
			RecordDuplicate
			RecordServerException
			RecordStorageException

		Returns:
			Data
		"""

		# If we have one
		if isinstance(value, dict):
			value['_id'] = self.add(
				value, conflict, revision_info
			)
			return Data(self, value)

		# Else, if it's numerous
		elif isinstance(value, list):
			l = []
			for d in value:
				d['_id'] = self.add(
					d, conflict, revision_info
				)
				l.append(Data(self, value))
			return l

	def install(self) -> bool:
		"""Install

		Installs or creates the location where the records will be stored and \
		retrieved from

		Returns:
			bool
		"""

		# Call the parent install and return the result
		return self._parent.install()

	def remove(self,
		_id: str | list[str] = undefined,
		filter: dict = undefined,
		revision_info = undefined
	) -> int:
		"""Remove

		Removes one or more records from storage by ID or filter, and returns \
		the record or records removed

		Arguments:
			_id (str): Optional, the ID(s) to remove
			filter (dict): Optional, data to filter what gets deleted
			revision_info (dict): Optional, additional information to store \
				with the revision record

		Raises:
			RecordServerException
			RecordStorageException

		Returns:
			dict | dict[]
		"""

		# Assume multiple
		bOne = False

		# The IDs to remove
		lIDs = _id

		# If we only got one
		if isinstance(_id, str):
			bOne = True
			lIDs = [_id]

		# Else, if we didn't get a
		elif not isinstance(_id, list):
			raise ValueError(
				'_id of Storage.remove must be a string or list of strings, ' \
				'not: "%s"' % str(_id)
			)

		# Create a new Transaction instance
		lTA = self._parent._table.transaction()

		# Keep track of the results
		lResults = []

		# Go through each one
		for sID in lIDs:

			# Delete the record using the parent and store it
			dRecord = self._parent.delete(sID, lTA)

			# If something was removed
			if dRecord:

				# If we store revisions
				if self._parent._table._struct.revisions:

					# Set the initial revisions record
					dRevisions = { 'old': dRecord, 'new': None }

					# If revisions requires fields
					if isinstance(self._parent._table._struct.revisions, list):

						# If they weren't passed
						if not isinstance(revision_info, dict):
							raise ValueError('revision')

						# Else, add the extra fields
						for f in self._parent._table._struct.revisions:
							dRevisions[f] = revision_info[f]

					# Generate the SQL for the revision and add it to the
					#	transactions
					lTA.append(
						self._parent._table.revision_add(sID, dRevisions)
					)

			# Add the record to the list of results
			lResults.append(dRecord)

		# Delete all the records at once
		if not lTA.run():
			return None

		# If we have a cache
		if self._cache:

			# Mark the records as missing to avoid subsequent hits trying to
			#	fetch the old records
			self._cache.add_missing(lIDs)

		# If there's only one
		if bOne:
			lResults = lResults[0]

		# Return the result or nothing
		return lResults or None

	def revision_add(self, _id: str, changes: dict) -> bool:
		"""Revision Add

		Adds data to the storage system associated with the record that \
		indicates the changes since the previous add/save

		Arguments:
			_id (str): The ID of the record the change is associated with
			changes (dict): The dictionary of changes to add

		Returns:
			bool
		"""

		# Throw an error if revisions aren't allowed on the record
		if not self._revisions:
			raise RuntimeError('Revisions not allowed')

		# Add the revision record to the table and return the result
		return self._parent._table.revision_add(_id, changes)

	def save(self,
		_id: str,
		value: dict,
		replace: bool = False,
		revision_info: dict = undefined,
		full: dict = undefined
	) -> bool:
		"""Save

		Takes existing data and updates it by ID

		Arguments:
			_id (str): The ID of the record to save
			value (dict): A dictionary of fields to data that has been changed
			replace (bool): Optional, set to True to completely replace the \
				the record
			revision_info (dict): Optional, a dict of additional data needed \
				to store a revision record, is dependant on the 'revision' \
				config value
			full (dict): Optional, the full data, used for revisions and \
				caching, saves processing cycles fetching data from the DB if \
				we already have it

		Raises:
			RecordDuplicate
			RecordServerException
			RecordStorageException

		Returns:
			True on success
		"""

		# If there's no value, return false
		if not value:
			return False

		# Create a new Transaction instance
		lTA = self._parent._table.transaction()

		# If we are replacing the record
		if replace:

			# Call the parent's set method
			mRes = self._parent.set(_id, value, lTA)

		# Else, we are updating
		else:

			# Call the parent's update method
			mRes = self._parent.update(_id, value, lTA)

		# Do we have changes
		if mRes:

			# If we need revisions
			if self._parent._table._struct.revisions:

				# Set the initial revisions record
				dRevisions = { 'old': mRes, 'new': value }

				# If revisions requires fields
				if isinstance(self._parent._table._struct.revisions, list):

					# If they weren't passed
					if not isinstance(revision_info, dict):
						raise RecordStorageException('revision')

					# Else, add the extra fields
					for f in self._parent._table._struct.revisions:
						dRevisions[f] = revision_info[f]

				# Generate the SQL to add the revision record to the table and add
				#	it to the transaction list
				lTA.append(
					self._parent._table.revision_add(_id, dRevisions)
				)

			# If we have a cache
			if self._cache:

				# Reset the cache
				self._cache.set(_id, full or self._parent.get(_id))

		# Run the transactions
		if not lTA.run():
			return None

		# Return the changes
		return mRes and True or False

	def uninstall(self) -> bool:
		"""Uninstall

		Uninstalls or deletes the location where the records will be stored \
		and retrieved from

		Returns:
			bool
		"""

		# Call the parent uninstall and return the result
		return self._parent.uninstall()

	def uuid(self) -> str:
		"""UUID

		Returns a universal unique ID from the host server associated with the \
		record

		Arguments:
			None

		Returns:
			str
		"""
		return self._parent._table.uuid()
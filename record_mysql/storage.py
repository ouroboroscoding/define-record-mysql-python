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
import undefined

# Local imports
from record_mysql.parent import Parent
from record_mysql.leveled import Leveled	# This is necessary only to make
											# sure the class is added to the
											# registry

# TESTING
from pprint import pprint
# TESTING

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

		# If the key name was overwritten in the special values, then we need
		#	to store the global one
		if self._parent._table._struct.key != key_name:
			self._key = self._parent._table._struct.key

		# Get the cache section
		oCache = self.special('cache', {
			'implementation': False
		})

		# If cache is enabled
		if oCache['implementation']:
			self._cache: Cache = Cache.generate(oCache)

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

		Returns:
			The ID of the added record
		"""

		# Add it to the value
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
			self._cache.store(value[self._key], value)

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

	def fetch(self,
		_id: str | list[str] = undefined,
		filter: dict = undefined,
		limit: int | tuple = undefined,
		raw: bool | list[str] = False,
		options: dict = None
	) -> None | Data | list[Data] | dict | list[dict]:
		"""Fetch

		Gets one, many, or all records from the storage system associated with \
		the class instance through one or more checks against IDs, filters, \
		and limits. Passing no arguments at all will return every record \
		available

		Arguments:
			_id: (str | str[]): The ID or IDs used to get the records
			filter (dict): Data to filter the count of records by
			limit (int | tuple | None): The limit to set for the fetch
			raw (bool): If true, dicts are returned instead of Data instances
			options (dict): Custom options processed by the storage system

		Returns:
			None | Data | Data[] | dict | dict[]
		"""

		# Init the records
		lRecords = None

		# If we got one or more IDs
		if _id is not undefined:

			# If we have just one
			if isinstance(_id, str):

				# Init the record
				dRecord = None

				# If we have a cache
				if self._cache:

					# Try to get it from the cache
					dRecord = self._cache.fetch(_id)

				# If we don't have the record
				if not dRecord:

					# Fetch it from the system
					dRecord = self._parent.get(_id)

					# If it doesn't exist
					if not dRecord:
						return None

					# If we have a cache, and we have the record
					if self._cache and dRecord:

						# Store it in the cache under the ID
						self._cache.store(_id, dRecord)

				# If we want the record as is
				if raw:
					return dRecord

				# Return a new Data
				return Data(self, _id, dRecord)

			# If we have a cache
			if self._cache:

				# Try to get them from the cache
				lRecords = self._cache.fetch(_id)

				# Go through each record by index
				for i in range(len(lRecords)):

					# If we didn't get the record
					if lRecords[i] is None:

						# Fetch it from the system
						dRecord = self._parent.get(_id[i])

						# If it doesn't exist
						if not dRecord:

							# Mark it as missing so we don't overload the
							#	system. Any future requests will return the
							#	record as False
							self._cache.add_missing(_id[i])

						# Else, we have it
						else:

							# Store it for next time
							self._cache.store(_id[i], dRecord)

					# Else, if it's False, set it to None and move on, we know
					#	this record does not exist
					elif lRecords[i] == False:
						lRecords[i] = None

			# Else, we have no cache, and want all records, this is super
			#	inefficient, but really there is no reason to not use a cache
			#	except in very early development
			else:

				# Init the return
				lRecords = []

				# Go through each ID
				for sID in _id:

					# Fetch the record from the DB, store it even if it's not
					#	found
					lRecords.append([
						sID,
						self._parent.get(sID)
					])

		# Else, if we have a filter
		elif filter is not undefined:
			pass

		# Else, fetch all or most records
		else:

			# Fetch all the IDs avaialble first
			lIDs = self._parent._table.select(
				fields = [self._key],
				limit = limit
			)

			# If we got anything
			if lIDs:

				# Flatten the list
				lIDs = [d[self._key] for d in lIDs]

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
								self._cache.store(lIDs[i], dRecord)

						# Else, if it's False, set it to None and move on, we know
						#	this record does not exist
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
			return None

		# If we want the records as is
		if raw:
			return lRecords

		# Return a new Data
		return [l and Data(self, l[0], l[1]) or None for l in lRecords]

	def exists(self, _id: str) -> bool:
		"""Exists

		Returns true if a record with the given ID exists

		Arguments:
			_id (str): The unique ID of the record to check for

		Returns:
			bool
		"""

		# Call the table directly
		return self._parent._table.select(
			fields = [ self._key ],
			where = { self._key: _id }
		) and True or False

	def insert(self,
		value: dict | list = {},
		conflict: str = 'error',
		revisions: dict = None
	) -> Data | list:
		"""Insert

		Creates a new data object associated with the Storage instance

		Arguments:
			value (dict|dict[]): The initial values to set for the record
			conflict (str|list): Must be one of 'error', 'ignore', 'replace', \
				or a list of fields to update
			revisions (dict): Data needed to store a change record, is \
				dependant on the 'revisions' config value

		Returns:
			Data
		"""

		# If we have one
		if isinstance(value, dict):
			value['_id'] = self.add(
				value, conflict, revisions
			)
			return Data(value)

		# Else, if it's numerous
		elif isinstance(value, list):
			l = []
			for d in value:
				d['_id'] = self.add(
					d, conflict, revisions
				)
				l.append(Data(value))
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
		the the record or records removed

		Arguments:
			_id (str): Optional, the ID(s) to remove
			filter (dict): Optional, data to filter what gets deleted
			revision_info (dict): Optional, additional data needed to store a \
				revision record. Is dependant on the 'revision' config value

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

				print('----------------------------------------')
				print('changes: ')
				pprint(mRes)
				print('----------------------------------------')

			# Generate the SQL to add the revision record to the table and add
			#	it to the transaction list
			#lTA.append(
			#	self._parent._table.revision_add(sID, dRevisions)
			#)

			# If we have a cache
			if self._cache:

				# Reset the cache
				self._cache.store(_id, full or self._parent.get(_id))

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
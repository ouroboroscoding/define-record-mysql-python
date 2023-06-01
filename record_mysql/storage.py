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

# Pip imports
from define import NOT_SET
from record import Storage as _Storage
from tools import merge

# Local imports
from .data import Data
from .parent import Parent
from .leveled import Leveled	# This is necessary only to make sure the class
								# is added to the registry

class Storage(_Storage):
	"""Storage

	Represents the top level definition of one or more tables in a specific
	database

	Extends record.Storage in order to add inserting, deleting, updating, and
	selecting SQL rows

	Extends:
		record.Storage
	"""

	def __init__(self, details: dict | str, extend: dict = NOT_SET):
		"""Constructor

		Creates a new instance of a single table, or in the case of complex
		records, multiple different tables, that contribute to storing and
		retrieving records

		Arguments:
			details (dict | str): Definition or filepath to load
			extend (dict | False): Optional, a dictionary to extend the
									definition

		Raises:
			KeyError, ValueError

		Returns
			Storage
		"""

		# Call the parent constructor
		super().__init__(details, extend)

		# Create the top level parent for the record
		self._parent = Parent(self._name, None, self)

	def add(self, value: dict, conflict: str = 'error', revision: dict = None) -> str:
		"""Add

		Adds one or more raw records to the mysql database table

		Arguments:
			value (dict): A dictionary of fields to data
			conflict (str|list): Must be one of 'error', 'ignore', 'replace',
				or a list of fields to update
			revision (dict): Data needed to store a change record, is
				dependant on the 'revision' config value

		Returns:
			The ID of the added record
		"""

		# Create a new ID for this record
		sID = self.uuid()

		# Add it to the value
		value[self._parent._table._struct.key] = sID

		print(value)

		# Validate the data
		if not self.valid(value):
			raise ValueError(self._validation_failures)

		# Take the incoming data, and pass it to the parent to set
		mData = self._parent.set(sID, value)

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
				if not isinstance(revision, dict):
					raise ValueError('revision')

				# Else, add the extra fields
				for f in self._parent._table._struct.revisions:
					dRevisions[f] = revision[f]

			# Add the data to the table using the same ID
			self._parent._table.revision_add(sID, dRevisions)

		# Return the ID of the new record
		return sID

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
		_id: str | list[str] = None,
		filter: dict = None,
		limit: int | tuple | None = None,
		fields: list[str] = None,
		raw: bool | list[str] = False,
		options: dict = None
	) -> Data | list[Data] | dict | list[dict]:
		"""Fetch

		Gets one, many, or all records from the storage system associated with
		the class instance through one or more checks against IDs, filters, and
		limits. Passing no arguments at all will return every record available

		Arguments:
			_id: (str | str[]): The ID or IDs used to get the records
			filter (dict): Data to filter the count of records by
			limit (int | tuple | None): The limit to set for the fetch
			fields (str[]): A list of the fields to be returned for each record
			raw (bool): If true, dicts are returned instead of Data instances
			options (dict): Custom options processed by the storage system

		Returns:
			Data | Data[] | dict | dict[]
		"""
		pass

	def exists(self,
		id: str) -> bool:
		"""Exists

		Returns true if a record with the given ID exists

		Arguments:
			id (str): The unique ID of the record to check for

		Returns:
			bool
		"""

		# Call the table directly
		return self._parent._table.select(
			fields = [ '_id' ],
			where = { '_id': id }
		) and True or False

	def insert(self,
		value: dict | list = {},
		conflict: str = 'error',
		revisions: dict = None
	) -> Data | list:
		"""Insert

		Creates a new data object associated with the Storage instance

		Arguments:
			value (dict): The initial values to set for the record
			conflict (str|list): Must be one of 'error', 'ignore', 'replace',
				or a list of fields to update
			revisions (dict): Data needed to store a change record, is
				dependant on the 'revisions' config value

		Returns:
			Data
		"""

		# If we didn't get a dictionary
		if not isinstance(value, dict):
			raise ValueError('value', value)

		# If we have one
		if isinstance(value, dict):
			value[self._parent._table._struct.key] = self.add(value, conflict, revisions)
			return Data(value)

		# Else, if it's numerous
		elif isinstance(value, list):
			l = []
			for d in value:
				d[self._parent._table._struct.key] = self.add(d, conflict, revisions)
				l.append(Data(value))
			return l

	def install(self) -> bool:
		"""Install

		Installs or creates the location where the records will be stored and
		retrieved from

		Returns:
			bool
		"""

		# Call the parent install and return the result
		return self._parent.install()

	def remove(self, _id: str | list[str] = None, filter: dict = None) -> int:
		"""Remove

		Removes one or more records from storage by ID or filter, and returns
		the count of records removed

		Arguments:
			_id (str): The ID(s) to remove
			filter (dict): Data to filter what gets deleted

		Returns:
			int
		"""
		pass

	def revision_add(cls, _id: str, changes: dict) -> bool:
		"""Revision Add

		Adds data to the storage system associated with the record that
		indicates the changes since the previous add/save

		Arguments:
			_id (str): The ID of the record the change is associated with
			changes (dict): The dictionary of changes to add

		Returns:
			bool
		"""
		pass

	def uninstall(self) -> bool:
		"""Uninstall

		Uninstalls or deletes the location where the records will be stored and
		retrieved from

		Returns:
			bool
		"""

		# Call the parent uninstall and return the result
		return self._parent.uninstall()

	def uuid(self) -> str:
		"""UUID

		Returns a universal unique ID from the host server associated with the
		record

		Arguments:
			None

		Returns:
			str
		"""
		return self._parent._table.uuid()
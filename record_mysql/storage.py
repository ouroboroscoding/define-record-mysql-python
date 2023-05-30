# coding=utf8
"""Record Storage

Extends the record.Storage class to add MySQL / MariaDB capabilities
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-01"

# Limit imports
__all__ = ['Limit', 'Storage']

# Python imports
import re

# Pip imports
from define import NOT_SET
from jobject import jobject
from record import Storage as _Storage
from record.types import Limit
from tools import merge

# Local imports
from .data import Data
from .parent import Parent

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

	def create(self,
		value: dict | list = {},
		conflict: str = 'error',
		revisions: dict = None
	) -> Data | list:
		"""Create

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

		# Validate the data
		if not self.valid(value):
			raise ValueError(self._validation_failures)

		# If it's ok, add it to the table
		value[self._struct['key']] = self.add(value, conflict, revisions)

		# Create a new data instance with the value and return it
		return Data(value)

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

	def uuid(self) -> str:
		"""UUID

		Returns a universal unique ID from the host server associated with the
		record

		Arguments:
			None

		Returns:
			str
		"""
		return self._table.uuid()
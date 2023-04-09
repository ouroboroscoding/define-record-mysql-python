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
from jobject import JObject
from record import Storage as _Storage
from record.types import Limit
from tools import merge

# Local imports
from .data import Data

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

		# Get the '__mysql__' special data and merge it on top of the default
		#	data
		self._struct = merge(JObject({
			'auto_key': True,
			'db': 'db',
			'host': '_',
			'indexes': [],
			'key': '_id',
			'revisions': False,
			'table': None
		}), self.special('mysql', {}))

		# If we're missing a table name
		if self._struct.table is None:
			raise ValueError('record_mysql.Storage __mysql__.table must be set')

	def add(self, value: dict, conflict: str = 'error', changes: dict = None) -> str:
		"""Add

		Adds one or more raw records to the mysql database table

		Arguments:
			value (dict): A dictionary of fields to data
			conflict (str|list): Must be one of 'error', 'ignore', 'replace',
				or a list of fields to update
			changes (dict): Data needed to store a change record, is
				dependant on the 'changes' config value

		Returns:
			The ID of the added record
		"""
		pass

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
		pass

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
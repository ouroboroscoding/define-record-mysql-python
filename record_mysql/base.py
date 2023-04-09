# coding=utf8
"""Record Base

Provides common ground for record_mysql classes
"""
from __future__ import annotations

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-03"

# Limit exports
__all__ = ['Base']

# Python imports
from abc import ABC, abstractmethod
from copy import copy

# Pip imports
import define
from jobject import JObject
from tools import without

# Local imports
from .table import Table
from .transaction import Transaction

class Base(ABC):
	"""Base

	An interface for mysql classes so they have a set of methods they know they
	can call on each other
	"""

	__types = {}
	"""Types

	Holds dictionary of define class names to the classes that handle them in
	record_mysql"""

	def __init__(self,
		name: str | None,
		parent: Base | None
	):
		"""Constructor

		Creates a new instance

		Arguments:
			name (str): The name of the instance in the parent
			parent (Base): The parent of this instance, if there is one

		Returns:
			Base
		"""

		# Store the name
		self._name: str | None = name

		# Store the parent
		self._parent: Base | None = parent

		# Init the field dicts
		self._keys: dict = {}
		self._columns: dict = {}
		self._complex: dict = {}

		# Store the table
		self._table: Table | None = None

	def __getattr__(self, name: str) -> any:
		"""Get Attribute

		Implements Python magic method __getattr__ to give object notation
		access to dictionaries

		Arguments:
			name (str): The dict key to get

		Raises:
			AttributeError

		Returns:
			any
		"""
		try:
			return self.__getitem__(name)
		except KeyError:
			raise AttributeError(name)

	def __getitem__(self, name: str) -> any:
		"""Get Item

		Implements Python magic method __getitem__ to give dict access to the
		instance

		Arguments:
			name (str): The dict key to get

		Raises:
			KeyError

		Returns:
			any
		"""

		# If it's in the list of complex data
		if name in self._complex:
			return self._complex[name]
		else:
			raise KeyError(name)

	@classmethod
	def add_type(cls, type_: str) -> None:
		"""Add Type

		Stores the calling class under the type so that it can be used to create
		class instances later on

		Arguments:
			type_ (str): The type of the type to add

		Returns:
			None
		"""

		# If the type already exists
		if type_ in cls.__types:
			raise ValueError('"%s" already added' % type_)

		# Store the new constructor
		cls.__types[type_] = cls

	@classmethod
	def create_type(cls,
		type_: str,
		name: str,
		parent: Base,
		details: define.Base
	) -> Base:
		"""Create Type

		Creates a new instance of a type previously added using .add_type()

		Arguments:
			type_ (str): The name of the type to create
			name (str): The name of the instance in the parent
			parent (Base): The parent of the instance that will be created
			details (define.Base): The define Node associated with the instance

		Raises:
			KeyError

		Returns:
			Base
		"""

		# Create and return a new instance of the type
		return cls.__types[type_](name, parent, details)

	@abstractmethod
	def _get_ids(self, ids: list[any]) -> list[any]:
		"""Get IDs

		Called by the child to get the IDs associated with its IDs

		Arguments:
			ids (str[]): The list of IDs to find IDs for

		Returns:
			str[]
		"""
		pass

	@abstractmethod
	def filter(self, values: dict) -> list[str]:
		"""Filter

		Returns the top level IDs filtered by the given field/value pairs

		Arguments:
			values (dict): The field and value pairs to filter by

		Returns:
			str[]
		"""
		pass

	@abstractmethod
	def get(self, id: str) -> list[dict]:
		"""Get

		Retrieves all the records associated with the given ID

		Arguments:
			id (str): The ID to fetch records for

		Returns:
			dict[]
		"""
		pass

	def install(self):
		"""Install

		Create the associated table if there is one, then ask each complex child
		to create its own tables

		"""

		# Assume eventual success
		bRes = True

		# If there's an associated table
		if self._table:
			if not self._table.create():
				bRes = False

		# Go through each complex type
		for f in self._complex:

			# And call its install method
			if not self._complex[f].install():
				bRes = False

		# Return the overall result
		return bRes

	def struct(self) -> dict:
		"""Structure

		Returns a copy of the structure associated with this object

		Returns:
			dict
		"""

		# Return the structure of the table
		try:
			return copy(self._table._struct)

		# If we have no table
		except AttributeError:

			# Return the structure of the parent
			try:
				return self._parent.struct()

			# If we have no parent
			except AttributeError:

				# Return an empty dict
				return JObject({})

	@abstractmethod
	def update(self,
		id: str,
		data: list | dict,
		ta: Transaction,
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
			ta (Transaction): The Transaction instance to add statements to
			return_revisions (bool): If True, returns a structure of values
									changed instead of True

		Returns:
			list | dict | bool
		"""
		pass

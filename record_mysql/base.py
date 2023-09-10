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

# Ouroboros imports
from define import Base as _Base
from jobject import jobject
import undefined

# Python imports
from abc import ABC, abstractmethod
from copy import copy

# Local imports
from .table import Func, Table
from .transaction import Transaction

class Base(ABC):
	"""Base

	An interface for mysql classes so they have a set of methods they know \
	they can call on each other
	"""

	__types = {}
	"""Types

	Holds dictionary of define class names to the classes that handle them in \
	record_mysql
	"""

	def __init__(self,
		name: str | None,
		parent: Base | str
	):
		"""Constructor

		Creates a new instance

		Arguments:
			name (str): The name of the instance in the parent
			parent (Base | str): The parent of this instance, if there is one,
				else the name of the primary key to use

		Returns:
			Base
		"""

		# Store the name
		self._name: str | None = name

		# Store the parent
		self._parent: Base | str = parent

		# Init the field dicts
		self._keys: dict = {}
		self._columns: dict = {}
		self._complex: dict = {}

		# Store the table
		self._table: Table | None = None

	def __getattr__(self, name: str) -> any:
		"""Get Attribute

		Implements Python magic method __getattr__ to give object notation \
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

		Implements Python magic method __getitem__ to give dict access to the \
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

		Stores the calling class under the type so that it can be used to \
		create class instances later on

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
		details: _Base
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

	def count(self, filter: dict = undefined) -> int:
		"""Count

		Returns the count of records, with or without a filter

		Arguments:
			filter (dict): Optional, filter to apply to records

		Returns:
			int
		"""

		# Call the table's count
		return self._table.select(
			fields = [Func('COUNT', '*')],
			where = filter
		)

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
	def delete(self,
		_id: str,
		ta: Transaction
	) -> list | dict | None:
		"""Delete

		Deletes one or more rows associated with the given ID and returns what \
		was deleted

		Arguments:
			_id (str): The unique ID associated with rows to be deleted
			ta (Transaction): Optional, the open transaction to add new sql \
				statements to

		Returns:
			list | dict | None
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

	def install(self) -> bool:
		"""Install

		Create the associated table if there is one, then asks each complex \
		child to create its own tables

		Returns:
			bool
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

	@abstractmethod
	def set(self,
		id: str,
		data: dict,
		ta: Transaction
	) -> dict | list | None:
		"""Set

		Sets the row or rows associated with the given ID and returns the \
		previous row or rows that were overwritten if there's any changes

		Arguments:
			id (str): The ID of the parent
			data (dict): A dict representing a structure of data to be set \
				under the given ID
			ta (Transaction): Optional, the open transaction to add new sql \
				statements to

		Returns:
			dict | list | None
		"""
		pass

	def struct(self) -> dict:
		"""Structure

		Returns a copy of the structure associated with this object

		Returns:
			jobject
		"""

		# Return the structure of the table
		try:
			return copy(self._table._struct)

		# If we have no table
		except AttributeError as e:

			# Return the structure of the parent
			try:
				return self._parent.struct()

			# If we have no parent
			except AttributeError:

				# Return an empty dict
				return jobject({})

	def uninstall(self) -> bool:
		"""Uninstall

		Drops the associated table if there is one, then asks each complex \
		child to drop its own tables

		Returns:
			bool
		"""

		# Assume eventual success
		bRes = True

		# If there's an associated table
		if self._table:
			if not self._table.drop():
				bRes = False

		# Go through each complex type
		for f in self._complex:

			# And call its install method
			if not self._complex[f].uninstall():
				bRes = False

		# Return the overall result
		return bRes

	@abstractmethod
	def update(self,
		id: str,
		data: list | dict,
		ta: Transaction
	) -> list | dict | None:
		"""Update

		Updates the row or rows associated with the given ID and returns the \
		previous row or rows that were overwritten if there's any changes

		Arguments:
			id (str): The ID to update records for
			data (list | dict): A list or dict representing a structure of \
				data to be updated under the given ID
			ta (Transaction): Optional, the open transaction to add new sql \
				statements to

		Returns:
			list | dict | None
		"""
		pass
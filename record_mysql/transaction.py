# coding=utf8
"""Record Transaction

Handles keeping track of multiple SQL statments from a single table and then \
running them all at once
"""
from __future__ import annotations

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-04-07"

# Limit exports
__all__ = ['Transaction']

# Ouroboros imports
import undefined

# Local imports
from record_mysql import server, table

class Transaction(list):
	"""Transaction

	Keeps track of several mysql commands so that they can be run all together
	"""

	def __init__(self, table: table.Table):
		"""Constructor

		Creates a new instance of the Transaction to keep track of SQL \
		statements and allow running them all at once

		Arguments:
			table (table.Table): The Table instance associated with the \
				transaction

		Returns:
			Transaction
		"""

		# Call the list constructor
		super(Transaction, self).__init__([])

		# Store the table instance
		self._table: table.Table = table

	def delete(self, where: dict = undefined) -> Transaction:
		"""Delete

		Deletes all or some records

		Arguments:
			where (dict): Optional, field/value pairs to decide what records \
				get deleted

		Returns:
			self for chaining
		"""
		self.append(
			self._table._delete(where)
		)
		return self

	def insert(self,
		values: dict,
		conflict: str = 'error'
	) -> Transaction:
		"""Insert

		Inserts a new record into the table

		Arguments:
			values (dict): The dictionary of fields to values to be inserted
			conflict (str | list): Must be one of 'error', 'ignore', \
				'replace', or a list of fields to update

		Returns:
			self for chaining
		"""
		self.append(
			self._table._insert(values, conflict)
		)
		return self

	def run(self) -> bool:
		"""Run

		Runs all the statements currently stored as a single transaction

		Returns:
			uint
		"""

		# Execute the query on the host and return the results
		return server.execute(self, self._table._struct.host)

	def update(self,
		values: dict,
		where: dict = None,
		conflict: str = 'error'
	) -> Transaction:
		"""Update

		Updates a specific field to the value for an ID, many IDs, or the \
		entire table

		Arguments:
			values (dict): The dictionary of fields to values to be updated
			where (dict): Optional, field/value pairs to decide what records \
				get updated
			conflict (str): Must be one of 'error', 'ignore'

		Returns:
			self for chaining
		"""
		self.append(
			self._table._update(values, where, conflict)
		)
		return self
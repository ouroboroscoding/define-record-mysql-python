# coding=utf8
"""Record MySQL

Record data structures using MySQL as a database
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-03-25"

# Limit imports
__all__ = [

	# Classes
	'Data', 'Literal', 'Storage',

	# Direct Server access
	'add_host', 'db_create', 'db_drop', 'escape', 'execute', 'insert', 'select',
	'timestamp_timezone', 'verbose'
]

# Ouroboros imports
from record import Data

# Local imports
from record_mysql.storage import Storage
from record_mysql.server import \
	add_host, db_create, db_drop, escape, execute, insert, select, \
	timestamp_timezone, verbose
from record_mysql.table import Literal
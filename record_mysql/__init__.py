# coding=utf8
"""Define Record MySQL

Define Record data structures using MySQL as a database
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

# Local imports
from .data import Data
from .storage import Storage
from .server import add_host, db_create, db_drop, escape, execute, insert, \
    				select, timestamp_timezone, verbose
from .table import Literal
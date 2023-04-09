# coding=utf8
"""Define Record MySQL

Define Record data structures using MySQL as a database
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-03-25"

# Limit imports
__all__ = ['Data', 'Literal', 'Storage']

# Local imports
from .data import Data
from .storage import Storage
from .table import Literal
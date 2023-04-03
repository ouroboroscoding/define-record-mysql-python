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

class Literal(object):
	"""Literal

	Used as a value that won't be escaped or parsed
	"""

	def __init__(self, text):
		if not isinstance(text, str):
			raise ValueError('first argument to Literal must be a string')
		self._text = text;
	def __str__(self):
		return self._text
	def get(self):
		return self._text

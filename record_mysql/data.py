
from record import Data as _Data

class Data(_Data):

	def add(self):

		self._storage.add(self._value)

		# Clear changed fields
		self._changed = []

	def save(self):

		self._storage.save

		# Clear changed fields
		self._changed = []
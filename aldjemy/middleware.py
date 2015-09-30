
from aldjemy.orm import close_session

class CloseSaSession:
	def _close(self):
		close_session()
	
	def process_response(self, req, response):
		self._close()
		return response
	
	def process_exception(self, req, exc):
		self._close()
		return None
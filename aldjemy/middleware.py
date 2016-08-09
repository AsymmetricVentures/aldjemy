
from aldjemy.orm import close_session

class CloseSaSession:
	def __init__(self, get_response = None):
		self.get_response = get_response
	
	def __call__(self, request):
		response = None
		if self.get_response is not None:
			response = self.get_response(request)
		return self.process_response(request, response)
		
	
	def _close(self):
		close_session()
	
	def process_response(self, req, response):
		self._close()
		return response
	
	def process_exception(self, req, exc):
		self._close()
		return None

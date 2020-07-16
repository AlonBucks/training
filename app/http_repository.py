import requests
from app import config


class HTTPRepository:

    def get_url_response(self, url):
        return requests.get(url).json()

    def get_all_documents(self):
        return self.get_url_response(config.DOCUMENTS_URL)

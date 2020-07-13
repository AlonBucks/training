import requests


DOCUMENTS_URL = 'http://intezer-documents-store.westeurope.cloudapp.azure.com/documents'


def ger_results_by_url(url):
    return requests.get(url).json()


def get_documents():
    return ger_results_by_url(DOCUMENTS_URL)

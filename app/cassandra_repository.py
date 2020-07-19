import os
import uuid
from typing import List
from cassandra.cluster import Cluster
from cassandra import query
from app import config
from injector import singleton


@singleton
class CassandraRepository:

    def __init__(self):
        self._session = None
        self._keyspace = 'words_db'
        self._cluster = Cluster([config.CASSANDRA_HOST_NAME])

    def create_session(self):
        self._session = self._cluster.connect()
        self.execute(f'CREATE KEYSPACE if not exists "{self._keyspace}" WITH REPLICATION = ' +
                     "{'class': 'SimpleStrategy','replication_factor': 1};")
        self._session = self._cluster.connect(self._keyspace)

    @property
    def session(self):
        if self._session is None:
            self._session = self._cluster.connect(self._keyspace)
        return self._session

    def set_keyspace(self, keyspace):
        self._keyspace = keyspace

    def execute(self, query_to_run, params=None):
        return self.session.execute(query_to_run, params)

    def get_words_data_by_words_list(self, words: List[str], case: bool):
        table = 'lower_words' if case else 'words'
        query_state = 'SELECT ' + f'word, docs FROM {table} WHERE word IN %s'
        rows = self.execute(query_state, (query.ValueSequence(words),))
        return rows

    def init(self):

        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'init_keyspace.cql'), 'r') as f:
            keyspace_statements = (line for line in f.read().split(';') if line.strip())
        for statement in keyspace_statements:
            self.execute(statement)

    def upsert_word(self, word: str, doc_id: str, details):
        self.execute('UPDATE ' + 'words SET docs = docs + {%s:%s} where word = %s',
                     (doc_id, details['indexes'], word))

        self.execute('UPDATE ' + 'lower_words SET docs = docs + {%s:%s} where word = %s',
                     (doc_id, details['indexes'], word.lower()))

    def insert_doc(self, doc):
        doc_id = uuid.uuid4()
        self.execute('INSERT INTO ' + 'documents(id, title, author) VALUES(%s, %s, %s)',
                     (doc_id, doc['title'], doc['author']))
        return doc_id

    def get_documents_by_ids(self, docs):
        return self.execute('SELECT ' + 'id, title, author FROM documents WHERE id IN %s', (query.ValueSequence(docs),))

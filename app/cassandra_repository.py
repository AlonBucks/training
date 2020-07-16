import os
import uuid
from cassandra.cluster import Cluster
from cassandra import query
from app import config


class CassandraRepository:

    def __init__(self):
        self._session = None
        self._key_space = 'words'

    @property
    def session(self):
        if self._session is None:
            cluster = Cluster([config.CASSANDRA_HOST_NAME])
            self._session = cluster.connect()
            self.execute('CREATE KEYSPACE if not exists "' + self._key_space + '" WITH REPLICATION = {' +
                         "'class': 'SimpleStrategy','replication_factor': 1};")
            self._session = cluster.connect(self._key_space)
        return self._session

    def set_key_space(self, key_space):
        self._key_space = key_space

    def execute(self, query_to_run, params=None):
        return self.session.execute(query_to_run, params)

    def get_rows_by_words(self, words, table):
        query_state = 'SELECT ' + f'word, docs FROM {table} WHERE word IN %s'
        rows = self.execute(query_state, (query.ValueSequence(words),))
        return rows

    def init(self):

        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'init_key_space.cql'), 'r') as f:
            key_space_statements = (line for line in f.read().split(';') if line.strip())
        for statement in key_space_statements:
            self.execute(statement)

    def upsert_word(self, word, doc_id, details):
        self.execute('UPDATE ' + 'words SET docs = docs + {%s:(%s,%s,%s)} where word = %s',
                     (doc_id, details['count'], details['idx'], details['next'], word))

        self.execute('UPDATE ' + 'lower_words SET docs = docs + {%s:(%s,%s)} where word = %s',
                     (doc_id, details['count'], details['idx'], word.lower()))

    def insert_doc(self, doc):
        doc_id = uuid.uuid4()
        self.execute('INSERT INTO ' + 'documents(id, title, author) VALUES(%s, %s, %s)',
                     (doc_id, doc['title'], doc['author']))
        return doc_id

    def get_documents_by_ids(self, docs):
        return self.execute('SELECT ' + 'id, title, author FROM documents WHERE id IN %s', (query.ValueSequence(docs),))

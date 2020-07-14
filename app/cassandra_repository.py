import os
import uuid
from cassandra.cluster import Cluster
from cassandra import query

cluster = Cluster(['cassandra'])
session = cluster.connect()
keyspace = 'words'


def execute(query_to_run, params=None):
    return session.execute(query_to_run, params)


def get_rows_by_words(words, table):
    query_state = 'SELECT ' + 'word, docs FROM ' + table + ' WHERE word IN %s'
    go_to_keyspace()
    rows = execute(query_state, (query.ValueSequence(words),))
    return rows


def go_to_keyspace():
    execute('USE ' + keyspace)


def init():

    keyspace_create_query = 'CREATE KEYSPACE if not exists "' + keyspace + '" WITH REPLICATION = {' + \
                            "'class': 'SimpleStrategy','replication_factor': 1};"

    execute(keyspace_create_query)

    go_to_keyspace()

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'init_keyspace.cql'), 'r') as f:
        keyspace_statements = (line for line in f.read().split(';') if line.strip())
    for statement in keyspace_statements:
        execute(statement)


def clear_test():
    execute('DROP ' + 'KEYSPACE IF EXISTS test')


def upsert_word(word, doc_id, details):
    execute('UPDATE ' + 'words SET docs = docs + {%s:(%s,%s,%s)} where word = %s',
            (doc_id, details['count'], details['idx'], details['next'], word))

    execute('UPDATE ' + 'lower_words SET docs = docs + {%s:(%s,%s)} where word = %s',
            (doc_id, details['count'], details['idx'], word.lower()))


def insert_doc(doc):
    doc_id = uuid.uuid4()
    execute('INSERT INTO ' + 'documents(id, title, author) VALUES(%s, %s, %s)', (doc_id, doc['title'], doc['author']))
    return doc_id


def get_documents_by_ids(docs):
    return execute('SELECT ' + 'id, title, author FROM documents WHERE id IN %s', (query.ValueSequence(docs),))

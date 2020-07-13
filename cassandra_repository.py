from cassandra.cluster import Cluster
from cassandra import query

cluster = Cluster(['cassandra'])
session = cluster.connect()
keyspace = 'words'


def execute(query_to_run, params=None):
    return session.execute(query_to_run, params)


def get_rows_by_words(words, table):
    query_state = "SELECT " + "word, docs FROM " + table + " WHERE word IN %s"
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

    execute("DROP " + """
            TABLE if exists words;
            """)

    execute("DROP " + """
            TABLE if exists lower_words;
            """)

    execute("CREATE " + """
            TABLE IF NOT EXISTS words(word text PRIMARY KEY, docs map<frozen<tuple<text,text>>, frozen<tuple<int,int,set<text>>>>);
            """)

    execute("CREATE " + """
            TABLE IF NOT EXISTS lower_words(word text PRIMARY KEY, docs map<frozen<tuple<text,text>>, frozen<tuple<int,int>>>);
            """)


def clear_test():
    execute("DROP " + "KEYSPACE IS EXISTS test")
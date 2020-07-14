from app import cassandra_repository, http_repository
import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker


# rabbitmq_broker = RabbitmqBroker(url='amqp://guest:guest@rabbit:5672/')
# dramatiq.set_broker(rabbitmq_broker)

db_rep = cassandra_repository
network_rep = http_repository

bad_chars = {',', '.', ':', ':', ')', '(', '{', '}'}

common = {'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by', 'for', 'if', 'in', 'into', 'is', 'it', 'no', 'not',
          'of', 'on', 'or', 'such', 'that', 'the', 'their', 'then', 'there', 'these', 'they', 'this', 'to', 'was',
          'will', 'with'}


def init():
    db_rep.init()


def run_index_async():
    index.send()


@dramatiq.actor
def index():
    db_rep.go_to_keyspace()

    res = network_rep.get_documents()
    docs_set = set()
    for doc in res['documents']:
        if (doc['title'], doc['author']) not in docs_set:
            docs_set.add((doc['title'], doc['author']))
            index_document(doc)


def index_document(doc):
    doc_id = cassandra_repository.insert_doc(doc)
    counter = 0
    words_dict = {}
    words = doc['content'].split()
    for idx, word in enumerate(words):
        if '<' not in word:
            word = fix_word(word)
            if word not in common and len(word) >= 3:

                next_word = None if idx + 1 >= len(words) else words[idx + 1]

                if word not in words_dict:
                    words_dict[word] = {}
                    words_dict[word]['count'] = 1
                    words_dict[word]['idx'] = counter
                    words_dict[word]['next'] = set()
                else:
                    words_dict[word]['count'] += 1

                if next_word:
                    words_dict[word]['next'].add(fix_word(next_word))

            counter += 1

    for word in words_dict:
        db_rep.upsert_word(word, doc_id, words_dict[word])


def fix_word(word):
    if word:
        for char in bad_chars:
            word = word.replace(char, '')

    return word


def search(str_arg, case=False):
    table = 'lower_words' if case else 'words'

    res = {}

    rows = cassandra_repository.get_rows_by_words(str_arg.split(), table)

    for row in rows.current_rows:
        for doc_id, details in row.docs.items():
            if doc_id in res:
                res[doc_id]['score'] += details[0]
            else:
                res[doc_id] = {}
                res[doc_id]['score'] = details[0]
                res[doc_id]['idx'] = []

            res[doc_id]['idx'].append((row.word, details[1]))

    docs_rows = cassandra_repository.get_documents_by_ids(list(res.keys())).current_rows
    docs_dict = {x.id: x for x in docs_rows}
    res = {str((docs_dict[doc].title, docs_dict[doc].author)): res[doc] for doc in res}

    return res


def exact(str_arg):
    words = str_arg.split()
    rows = db_rep.get_rows_by_words(words, 'words')
    res_docs = []

    if len(words) == len(rows.current_rows):
        words_dict = {}
        for row in rows.current_rows:
            words_dict[row.word] = row.docs

        for doc, details in words_dict[words[0]].items():
            check_doc(words, words_dict, doc, details[2], 1, res_docs)

    docs_rows = cassandra_repository.get_documents_by_ids(res_docs).current_rows

    res = [str((doc.title, doc.author)) for doc in docs_rows]

    return str(res)


def check_doc(words, words_dict, doc, next_words, idx, res):
    if idx == len(words):
        res.append(doc)
        return

    if words[idx] in next_words:
        check_doc(words, words_dict, doc, words_dict[words[idx]][doc][2], idx+1, res)

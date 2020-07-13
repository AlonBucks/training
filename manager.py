import cassandra_repository
import http_repository
import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker


rabbitmq_broker = RabbitmqBroker(url="amqp://guest:guest@rabbit:5672/")
dramatiq.set_broker(rabbitmq_broker)

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
    counter = 0
    words_dict = {}
    words = doc['content'].split()
    for idx, word in enumerate(words):
        if '<' not in word:
            fixed = fix_word(word)
            if fixed not in common and len(fixed) >= 3:

                next_word = None if idx + 1 >= len(words) else words[idx + 1]

                if fixed not in words_dict:
                    words_dict[fixed] = {}
                    words_dict[fixed]['count'] = 1
                    words_dict[fixed]['idx'] = counter
                    words_dict[fixed]['next'] = set()
                else:
                    words_dict[fixed]['count'] += 1

                if next_word:
                    words_dict[fixed]['next'].add(fix_word(next_word))

            counter += 1

    for word in words_dict:
        db_rep.execute("UPDATE " + "words SET docs = docs + {(%s,%s):(%s,%s,%s)} where word = %s",
                                     (doc['title'], doc['author'], words_dict[word]['count'], words_dict[word]['idx'],
                                      words_dict[word]['next'], word))

        db_rep.execute("UPDATE " + "lower_words SET docs = docs + {(%s,%s):(%s,%s)} where word = %s",
                                     (doc['title'], doc['author'], words_dict[word]['count'], words_dict[word]['idx'],
                                      word.lower()))


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
        for doc, details in row.docs.items():
            key = str((doc[0], doc[1]))
            if key in res:
                res[key]['score'] += details[0]
            else:
                res[key] = {}
                res[key]['score'] = details[0]
                res[key]['idx'] = []

            res[key]['idx'].append((row.word, details[1]))

    return res


def exact(str_arg):
    words = str_arg.split()
    rows = db_rep.get_rows_by_words(words, 'words')
    res = []

    if len(words) == len(rows.current_rows):
        words_dict = {}
        for row in rows.current_rows:
            words_dict[row.word] = row.docs

        for doc, details in words_dict[words[0]].items():
            check_doc(words, words_dict, doc, details[2], 1, res)

    return str(res)


def check_doc(words, words_dict, doc, next_words, idx, res):
    if idx == len(words):
        res.append(str(doc))
        return

    if words[idx] in next_words:
        check_doc(words, words_dict, doc, words_dict[words[idx]][doc][2], idx+1, res)

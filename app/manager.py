import dramatiq
from injector import inject, singleton, Injector
from app import config, index_task
from app.cassandra_repository import CassandraRepository
from app.http_repository import HTTPRepository
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq import GenericActor


rabbitmq_broker = RabbitmqBroker(url=config.RABBIT_URL)
dramatiq.set_broker(rabbitmq_broker)


@singleton
class Manager:
    @inject
    def __init__(self, cassandra_repository: CassandraRepository, http_repository: HTTPRepository):
        self._cassandra_repository = cassandra_repository
        self._http_repository = http_repository
        index_task.manager = self

    def init(self):
        self._cassandra_repository.init()

    @staticmethod
    def run_index_async():
        IndexTask.send()

    @dramatiq.actor()
    def index(self):
        res = self._http_repository.get_all_documents()
        docs_set = set()
        for doc in res['documents']:
            if (doc['title'], doc['author']) not in docs_set:
                docs_set.add((doc['title'], doc['author']))
                self.index_document(doc)

    def index_document(self, doc):
        doc_id = self._cassandra_repository.insert_doc(doc)
        counter = 0
        words_dict = {}
        words = doc['content'].split()
        for idx, word in enumerate(words):
            if '<' not in word:
                word = self.fix_word(word)
                if word not in config.COMMON and len(word) >= 3:

                    next_word = None if idx + 1 >= len(words) else words[idx + 1]

                    if word not in words_dict:
                        words_dict[word] = {}
                        words_dict[word]['count'] = 1
                        words_dict[word]['idx'] = counter
                        words_dict[word]['next'] = set()
                    else:
                        words_dict[word]['count'] += 1

                    if next_word:
                        words_dict[word]['next'].add(self.fix_word(next_word))

                counter += 1

        for word in words_dict:
            self._cassandra_repository.upsert_word(word, doc_id, words_dict[word])

    @staticmethod
    def fix_word(word):
        if word:
            for char in config.BAD_CHARS:
                word = word.replace(char, '')

        return word

    def search(self, str_arg, case=False):
        table = 'lower_words' if case else 'words'

        res = {}

        rows = self._cassandra_repository.get_rows_by_words(str_arg.split(), table)

        for row in rows.current_rows:
            for doc_id, details in row.docs.items():
                if doc_id in res:
                    res[doc_id]['score'] += details[0]
                else:
                    res[doc_id] = {}
                    res[doc_id]['score'] = details[0]
                    res[doc_id]['idx'] = []

                res[doc_id]['idx'].append((row.word, details[1]))

        docs_rows = self._cassandra_repository.get_documents_by_ids(list(res.keys())).current_rows
        docs_dict = {x.id: x for x in docs_rows}
        res = {str((docs_dict[doc].title, docs_dict[doc].author)): res[doc] for doc in res}

        return res

    def exact(self, str_arg):
        words = str_arg.split()
        rows = self._cassandra_repository.get_rows_by_words(words, 'words')
        res_docs = []

        if len(words) == len(rows.current_rows):
            words_dict = {}
            for row in rows.current_rows:
                words_dict[row.word] = row.docs

            for doc, details in words_dict[words[0]].items():
                self.check_doc(words, words_dict, doc, details[2], 1, res_docs)

        docs_rows = self._cassandra_repository.get_documents_by_ids(res_docs).current_rows

        res = [str((doc.title, doc.author)) for doc in docs_rows]

        return str(res)

    def check_doc(self, words, words_dict, doc, next_words, idx, res):
        if idx == len(words):
            res.append(doc)
            return

        if words[idx] in next_words:
            self.check_doc(words, words_dict, doc, words_dict[words[idx]][doc][2], idx + 1, res)


class BaseTask(GenericActor):
    class Meta:
         abstract = True
         queue_name = "tasks"
         max_retries = 20

    def get_task_name(self):
        raise NotImplementedError

    def perform(self):
        manager = Injector().get(Manager)
        manager.index(manager)


class IndexTask(BaseTask):

    def get_task_name(self):
        return 'Index'

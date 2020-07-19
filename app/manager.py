import dramatiq
from injector import inject
from injector import singleton
from injector import Injector
from app import config
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

    def create_session(self):
        self._cassandra_repository.create_session()

    @staticmethod
    def run_index_async():
        IndexTask.send()

    def index(self):
        all_documents = self._http_repository.get_all_documents()
        docs_set = set()
        for doc in all_documents['documents']:
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
                word = self.remove_special_chars_from_word(word)
                if word not in config.COMMON and len(word) >= 3:
                    if word not in words_dict:
                        words_dict[word] = {}
                        words_dict[word]['indexes'] = set()

                    words_dict[word]['indexes'].add(counter)

                counter += 1

        for word in words_dict:
            self._cassandra_repository.upsert_word(word, doc_id, words_dict[word])

    @staticmethod
    def remove_special_chars_from_word(word):
        if word:
            for char in config.BAD_CHARS:
                word = word.replace(char, '')

        return word

    def search(self, str_arg, case=False):
        res = {}

        rows = self._cassandra_repository.get_words_data_by_words_list(str_arg.split(), case)

        for row in rows.current_rows:
            for doc_id, indexes in row.docs.items():
                if doc_id in res:
                    res[doc_id]['score'] += len(indexes)
                else:
                    res[doc_id] = {}
                    res[doc_id]['score'] = len(indexes)
                    res[doc_id]['idx'] = []

                res[doc_id]['idx'].append((row.word, list(indexes)))

        docs_rows = self._cassandra_repository.get_documents_by_ids(list(res.keys())).current_rows
        docs_dict = {x.id: x for x in docs_rows}
        res = {str((docs_dict[doc].title, docs_dict[doc].author)): res[doc] for doc in res}

        return res

    def exact(self, str_arg):
        words = str_arg.split()
        rows = self._cassandra_repository.get_words_data_by_words_list(words, False)
        res_docs = []

        if len(words) == len(rows.current_rows):
            words_dict = {}
            for row in rows.current_rows:
                words_dict[row.word] = row.docs

            for doc, indexes in words_dict[words[0]].items():
                self.check_doc(words, words_dict, doc, indexes, res_docs)

        docs_rows = self._cassandra_repository.get_documents_by_ids(res_docs).current_rows

        res = [str((doc.title, doc.author)) for doc in docs_rows]

        return str(res)

    def check_doc(self, words, words_dict, doc, indexes, res):
        doc_contain_word = True
        for index in indexes:
            for idx, word in enumerate(words):
                if doc not in words_dict[word] or index+idx not in words_dict[word][doc]:
                    doc_contain_word = False
                    break
            if doc_contain_word:
                res.append(doc)
            doc_contain_word = True


class BaseTask(GenericActor):
    class Meta:
         abstract = True
         queue_name = "tasks"
         max_retries = 20

    def get_task_name(self):
        return 'Index'

    def perform(self):
        manager = Injector().get(Manager)
        manager.index()


class IndexTask(BaseTask):

    def get_task_name(self):
        return 'Index'


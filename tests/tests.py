import unittest
from unittest.mock import Mock
from app.cassandra_repository import CassandraRepository
from app.manager import Manager


def get_documents_mock():
    return {
      'documents': [
        {
          'author': 'Alon',
          'content': 'test1 test2',
          'title': 'tables'
        }, 
        {
          'author': 'Alon',
          'content': 'test4 test5 test4, test9',
          'title': 'chairs'
        },  
        {
          'author': 'Dani',
          'content': 'test3 test2 test3 test4',
          'title': 'tables'
        }, 
    ]}


class TestApp(unittest.TestCase):

    def setUp(self):
        self.cassandra_repository = CassandraRepository()
        self.cassandra_repository.set_keyspace('test')
        self.cassandra_repository.create_session()
        self.cassandra_repository.init()
        http_mock = Mock()
        http_mock.get_all_documents = get_documents_mock
        self.manager = Manager(self.cassandra_repository, http_mock)
        self.manager.index()

    def test_index(self):
        rows = self.cassandra_repository.get_words_data_by_words_list(['test1', 'test4'], False)
        self.assertEqual(len(rows.current_rows), 2)
        for row in rows.current_rows:
            if row.word == 'test1':
                docs_rows = self.cassandra_repository.get_documents_by_ids([x for x in row.docs]).current_rows
                self.assertEqual(len(row.docs), 1)
                self.assertEqual(len(docs_rows), 1)
                self.assertEqual(docs_rows[0].title, 'tables')
                self.assertEqual(docs_rows[0].author, 'Alon')
                key = list(row.docs.keys())[0]
                self.assertEqual(len(row.docs[key]), 1)
                self.assertIn(0, row.docs[key])

            if row.word == 'test4':
                docs_rows = self.cassandra_repository.get_documents_by_ids([x for x in row.docs]).current_rows
                self.assertEqual(len(row.docs), 2)
                self.assertEqual(len(docs_rows), 2)
                first_key = [doc.id for doc in docs_rows if doc.title == 'chairs' and doc.author == 'Alon']
                second_key = [doc.id for doc in docs_rows if doc.title == 'tables' and doc.author == 'Dani']
                self.assertEqual(len(first_key), 1)
                self.assertEqual(len(second_key), 1)
                first_key = first_key[0]
                second_key = second_key[0]
                self.assertEqual(len(row.docs[first_key]), 2)
                self.assertIn(0, row.docs[first_key])
                self.assertIn(2, row.docs[first_key])
                self.assertEqual(len(row.docs[second_key]), 1)
                self.assertIn(3, row.docs[second_key])

    def test_search(self):
        res = self.manager.search('test1 test4')
        self.assertEqual(len(res), 3)
        self.assertEqual(res[str(('tables', 'Alon'))]['score'], 1)
        self.assertEqual(res[str(('tables', 'Alon'))]['idx'], [('test1', [0])])
        self.assertEqual(res[str(('chairs', 'Alon'))]['score'], 2)
        self.assertEqual(res[str(('chairs', 'Alon'))]['idx'], [('test4', [0, 2])])
        self.assertEqual(res[str(('tables', 'Dani'))]['score'], 1)
        self.assertEqual(res[str(('tables', 'Dani'))]['idx'], [('test4', [3])])

    def test_exact(self):
        res = self.manager.exact('test2 test3 test4')
        self.assertEqual(res, "[\"('tables', 'Dani')\"]")

    def test_fix_word(self):
        self.assertEqual(self.manager.remove_special_chars_from_word('AbCd'), 'AbCd')
        self.assertEqual(self.manager.remove_special_chars_from_word(None), None)
        self.assertEqual(self.manager.remove_special_chars_from_word('A:()bC,..d'), 'AbCd')

    def test_check_doc(self):
        words = ['abcd', 'popo', 'kal', 'kjkj']
        words_dict = {
            'abcd': {
                'ag': {0, 10},
                'ab': {4}},
            'popo': {
                'ag': {1, 20, 50},
                'ab': {5, 13, 9}},
            'kal': {
                'ag': {2},
                'ab': {6, 20, 50}},
            'kjkj': {
                'ag': {70},
                'ab': {7, 56}}
        }
        ab_indexes = words_dict[words[0]]['ab']
        ag_indexes = words_dict[words[0]]['ag']
        res = []
        self.manager.check_doc(words, words_dict, 'ab', ab_indexes, res)
        self.manager.check_doc(words, words_dict, 'ag', ag_indexes, res)
        self.assertEqual(res, ['ab'])

    def tearDown(self):
        self.cassandra_repository.execute('DROP ' + 'KEYSPACE IF EXISTS test')


if __name__ == '__main__':
    unittest.main()
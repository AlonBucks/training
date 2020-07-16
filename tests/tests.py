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
        self.cassandra_repository.set_key_space('test')
        self.cassandra_repository.init()
        http_mock = Mock()
        http_mock.get_all_documents = get_documents_mock
        self.manager = Manager(self.cassandra_repository, http_mock)
        self.manager.index(self.manager)

    def test_index(self):
        rows = self.cassandra_repository.get_rows_by_words(['test1', 'test4'], 'words')
        self.assertEqual(len(rows.current_rows), 2)
        for row in rows.current_rows:
            if row.word == 'test1':
                docs_rows = self.cassandra_repository.get_documents_by_ids([x for x in row.docs]).current_rows
                self.assertEqual(len(row.docs), 1)
                self.assertEqual(len(docs_rows), 1)
                self.assertEqual(docs_rows[0].title, 'tables')
                self.assertEqual(docs_rows[0].author, 'Alon')
                key = list(row.docs.keys())[0]
                self.assertEqual(row.docs[key][0], 1)
                self.assertEqual(row.docs[key][1], 0)
                self.assertEqual(row.docs[key][2], {'test2'})

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
                self.assertEqual(row.docs[first_key][0], 2)
                self.assertEqual(row.docs[first_key][1], 0)
                self.assertEqual(row.docs[first_key][2], {'test5', 'test9'})
                self.assertEqual(row.docs[second_key][0], 1)
                self.assertEqual(row.docs[second_key][1], 3)
                self.assertEqual(row.docs[second_key][2], {})

    def test_search(self):
        res = self.manager.search('test1 test4')
        self.assertEqual(len(res), 3)
        self.assertEqual(res[str(('tables', 'Alon'))]['score'], 1)
        self.assertEqual(res[str(('tables', 'Alon'))]['idx'], [('test1', 0)])
        self.assertEqual(res[str(('chairs', 'Alon'))]['score'], 2)
        self.assertEqual(res[str(('chairs', 'Alon'))]['idx'], [('test4', 0)])
        self.assertEqual(res[str(('tables', 'Dani'))]['score'], 1)
        self.assertEqual(res[str(('tables', 'Dani'))]['idx'], [('test4', 3)])

    def test_exact(self):
        res = self.manager.exact('test2 test3 test4')
        self.assertEqual(res, "[\"('tables', 'Dani')\"]")

    def test_fix_word(self):
        self.assertEqual(self.manager.fix_word('AbCd'), 'AbCd')
        self.assertEqual(self.manager.fix_word(None), None)
        self.assertEqual(self.manager.fix_word('A:()bC,..d'), 'AbCd')

    def test_check_doc(self):
        words = ['abcd', 'popo', 'kal', 'kjkj']
        words_dict = {
            'abcd': {
                ('a', 'g'): (10, 5, {'abc123', 'abcdk'}),
                ('a', 'b'): (10, 5, {'kjkj', 'popo'})},
            'popo': {
                ('a', 'g'): (10, 5, {'abc1fcfc23', 'abcdk'}),
                ('a', 'b'): (10, 5, {'kal', 'bal'})},
            'kal': {
                ('a', 'g'): (10, 5, {'abc1fcfc23', 'abcdk'}),
                ('a', 'b'): (10, 5, {'kjkj', 'plplplpl'})},
            'kjkj': {
                ('a', 'g'): (10, 5, {'abc1fcfc23', 'abcdk'}),
                ('a', 'b'): (10, 5, {'kjkj', 'plpfflplpl'})}
        }
        idx = 1
        ab_next_words = words_dict[words[0]][('a', 'b')][2]
        ag_next_words = words_dict[words[0]][('a', 'g')][2]
        res = []
        self.manager.check_doc(words, words_dict, ('a', 'b'), ab_next_words, idx, res)
        self.manager.check_doc(words, words_dict, ('a', 'g'), ag_next_words, idx, res)
        self.assertEqual(res, [('a', 'b')])

    def tearDown(self):
        self.cassandra_repository.execute('DROP ' + 'KEYSPACE IF EXISTS test')


if __name__ == '__main__':
    unittest.main()
import unittest
import manager
import cassandra_repository
from unittest.mock import Mock


def get_documents_mock():
    return {
      "documents": [
        {
          "author": "Alon", 
          "content": "test1 test2", 
          "title": "tables"
        }, 
        {
          "author": "Alon", 
          "content": "test4 test5 test4, test9",
          "title": "chairs"
        },  
        {
          "author": "Dani", 
          "content": "test3 test2 test3 test4",
          "title": "tables"
        }, 
    ]}


class TestApp(unittest.TestCase):

    def setUp(self):
        cassandra_repository.keyspace = 'test'
        cassandra_repository.init()
        http_mock = Mock()
        http_mock.get_documents = get_documents_mock
        manager.network_rep = http_mock
        manager.index()

    def test_index(self):
        rows = cassandra_repository.get_rows_by_words(['test1', 'test4'], 'words')
        self.assertEqual(len(rows.current_rows), 2)
        for row in rows.current_rows:
            if row.word == 'test1':
                self.assertEqual(len(row.docs), 1)
                self.assertIn(("tables", "Alon"), row.docs)
                self.assertEqual(row.docs[("tables", "Alon")][0], 1)
                self.assertEqual(row.docs[("tables", "Alon")][1], 0)
                self.assertEqual(row.docs[("tables", "Alon")][2], {'test2'})

            if row.word == 'test4':
                self.assertEqual(len(row.docs), 2)
                self.assertIn(("chairs", "Alon"), row.docs)
                self.assertIn(("tables", "Dani"), row.docs)
                self.assertEqual(row.docs[("chairs", "Alon")][0], 2)
                self.assertEqual(row.docs[("chairs", "Alon")][1], 0)
                self.assertEqual(row.docs[("chairs", "Alon")][2], {'test5', 'test9'})
                self.assertEqual(row.docs[("tables", "Dani")][0], 1)
                self.assertEqual(row.docs[("tables", "Dani")][1], 3)
                self.assertEqual(row.docs[("tables", "Dani")][2], {})

    def test_search(self):
        res = manager.search('test1 test4')
        self.assertEqual(len(res), 3)
        self.assertEqual(res[str(('tables', 'Alon'))]['score'], 1)
        self.assertEqual(res[str(('tables', 'Alon'))]['idx'], [('test1', 0)])
        self.assertEqual(res[str(('chairs', 'Alon'))]['score'], 2)
        self.assertEqual(res[str(('chairs', 'Alon'))]['idx'], [('test4', 0)])
        self.assertEqual(res[str(('tables', 'Dani'))]['score'], 1)
        self.assertEqual(res[str(('tables', 'Dani'))]['idx'], [('test4', 3)])

    def test_exact(self):
        res = manager.exact('test2 test3 test4')
        self.assertEqual(res, "[\"('tables', 'Dani')\"]")

    def test_fix_word(self):
        self.assertEqual(manager.fix_word('AbCd'), 'AbCd')
        self.assertEqual(manager.fix_word(None), None)
        self.assertEqual(manager.fix_word('A:()bC,..d'), 'AbCd')

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
        manager.check_doc(words, words_dict, ('a', 'b'), ab_next_words, idx, res)
        manager.check_doc(words, words_dict, ('a', 'g'), ag_next_words, idx, res)
        self.assertEqual(res, [str(('a', 'b'))])

    def tearDown(self):
        cassandra_repository.clear_test()


if __name__ == '__main__':
    unittest.main()
import unittest
from main import search


class SearchTest(unittest.TestCase):
    def test_search_success(self):
        result = search("trophies.xml")
        self.assertEqual(len(result), 8)

    def test_search_fail(self):
        result = search("trophies.xml")
        self.assertEqual(len(result), 4)


if __name__ == '__main__':
    unittest.main()

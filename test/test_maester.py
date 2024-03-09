from yaat.maester import Entry, Maester
from yaat.util import path
import unittest, os

class TestEntry(unittest.TestCase):

    def setUp(self): self.e = Entry()
    def tearDown(self): del self.e

    def test_normal_attr(self):
        self.e.val1 = 1
        self.assertEqual(self.e.val1, 1)

    def test_regattr1(self):
        self.e.regattr('val1', 1)
        self.assertEqual(self.e.val1, 1)

    def test_regattr2(self):
        self.val1 = 1
        self.e.regattr('val1', 2)
        self.assertEqual(self.e.val1, 2)

    def test_readonly_regattr(self):
        self.e.regattr('val1', 1, readonly=True)
        with self.assertRaises(AssertionError): self.e.val1 = 1

    def test_dir_regattr(self):
        self.e.regattr('dir_name', 'trash_dir', is_dir=True, exists_ok=True)
        self.assertTrue(self.e.dir_name in os.listdir(Entry.local_root_dir))
        os.rmdir(path(Entry.local_root_dir, self.e.dir_name))

    def test_set_error(self): pass # TODO
    def test_to_json(self): pass # TODO

    def test_from_json(self):
        self.assertSetEqual(Entry.from_json( "{}")._attrs, set())

    def test_from_csv(self): pass # TODO
    def test_to_pddf(self): pass # TODO
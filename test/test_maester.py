from yaat.maester import Entry, Maester
from yaat.util import path
import unittest, os

class TestEntry(unittest.TestCase):

    def setUp(self): self.e = Entry()
    def tearDown(self): del self.e

    # helper
    def rm(self, p:str):
        p = path(Maester.local, self.e.root, p)
        if os.path.isdir(p): os.rmdir(p)
        if os.path.isfile(p): os.remove(p)

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

    def test_dir_regattr1(self):
        self.e.regattr('dir_name', 'trash_dir', is_dir=True, exists_ok=True)
        self.assertTrue(self.e.dir_name in os.listdir(dp:=path(Maester.local, Entry.root)))
        os.rmdir(path(dp, self.e.dir_name))

    def test_dir_regattr2(self):
        dirnam = 'trash_dir'
        self.rm(dirnam)
        self.e.regattr('dir_name', dirnam, is_dir=True, exists_ok=False)
        self.assertTrue(self.e.dir_name in os.listdir(Maester.local))
        os.rmdir(path(Maester.local, self.e.dir_name))

    def test_dir_regattr2(self):
        dirnam = 'trash_dir'
        self.rm(dirnam)
        self.e.regattr('dir_name', dirnam, is_dir=True, exists_ok=True)
        with self.assertRaises(FileExistsError):
            self.e.regattr('dir_name', dirnam, is_dir=True, exists_ok=False)
        os.rmdir(path(Maester.local, Entry.root, self.e.dir_name))

    def test_set_error(self): pass # TODO
    def test_to_json(self): pass # TODO

    def test_from_json(self):
        self.assertSetEqual(Entry.from_json( "{}")._attrs, set())

    def test_from_csv(self): pass # TODO
    def test_to_pddf(self): pass # TODO

    def test_append_file1(self):
        fn, data = 'mydata', '12234'
        self.rm(fn)
        self.e.regattr(fn, data, append=True)
        self.assertEqual(self.e.mydata, data)
        with open(path(Maester.local, self.e.root, fn)) as f:
            self.assertEqual(f.read(), data)

    def test_append_file2(self):
        fn, data = 'mydata', '12234'
        self.rm(fn)
        self.e.regattr(fn, data, append=True)
        self.assertEqual(self.e.mydata, data)
        with open(path(Maester.local, self.e.root, fn)) as f:
            self.assertEqual(f.read(), data)
        self.e.mydata = data
        with open(path(Maester.local, self.e.root, fn)) as f:
            self.assertEqual(f.read(), data+data)
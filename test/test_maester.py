from yaat.maester import Entry, Maester
from yaat.util import path
import unittest, os

class TestEntry(unittest.TestCase):
    test_num:int=0

    def setUp(self):
        self.e = Entry("test"+str(self.test_num))
        self.test_num+=1
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

    def test_append_file1(self):
        fn, data = 'mydata', '12234'
        self.rm(fn)
        self.e.regattr(fn, data, append=True)
        self.assertEqual(self.e.mydata, data)
        with open(path(Maester.local, self.e.root, fn)) as f:
            self.assertEqual(f.read(), data)
        self.e.mydata = data
        with open(path(Maester.local, self.e.root, fn)) as f:
            self.assertEqual(f.read(), data+data)

    def test_write_file1(self):
        fn, data = 'mydata', '12234'
        self.rm(fn)
        self.e.regattr(fn, data, write=True)
        self.assertEqual(self.e.mydata, data)
        with open(path(Maester.local, self.e.root, fn)) as f:
            self.assertEqual(f.read(), data)
        self.e.mydata = data
        with open(path(Maester.local, self.e.root, fn)) as f:
            self.assertEqual(f.read(), data)

    def test_getattr_file(self):
        old_thresh = Entry.mem_threshold
        Entry.mem_threshold = 10
        fn, data = 'mydata', str([str(i) for i in range(11)])
        self.e.regattr(fn, data, write=True)
        self.assertEqual(self.e.mydata, data)
        self.assertTrue(fn not in self.__dict__)
        Entry.mem_threshold = old_thresh

    def test_set_error(self):
        errm = "foo"
        self.assertEqual(self.e.status, self.e.Status.created)
        self.e.set_error(errm)
        self.assertEqual(self.e.status, self.e.Status.error)
        with open(path(Maester.local, self.e.root, self.e.name+"_error"), 'r') as f:
            self.assertEqual(f.read(), errm)
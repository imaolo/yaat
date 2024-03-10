import unittest, os
os.environ['ROOT'] = 'twork'

from yaat.maester import Entry, Maester
from yaat.util import path, runcmd

class TestEntry(unittest.TestCase):
    test_num:int=0
    root = path(Maester.root, Entry.root)

    @staticmethod
    def rm(p:str):
        if os.path.isdir(p): runcmd(f"rm -rf {p}")
        elif os.path.isfile(p): os.remove(p)

    @classmethod
    def setUpClass(cls) -> None:
        cls.rm(Entry.root)
        return super().setUpClass()

    # @classmethod
    # def tearDownClass(cls) -> None:
    #     runcmd(f"rm -rf {path(Maester.root, Entry.root)}")
    #     return super().tearDownClass()

    def setUp(self, mem_th=Entry.def_mem_threshold):
        self.e = Entry("test"+str(self.test_num), mem_th=mem_th)
        self.test_num+=1
    def tearDown(self): del self.e

    ### Tests ### 

    def test_normal_attr(self):
        self.e.val1 = 1
        self.assertEqual(self.e.val1, 1)

    def test_regattr1(self):
        self.e.regattr('val1', 1)
        self.assertEqual(self.e.val1, '1')

    def test_regattr2(self):
        self.val1 = 1
        self.e.regattr('val1', 2)
        self.assertEqual(self.e.val1, '2')

    def test_readonly_regattr(self):
        self.e.regattr('val1', 1, readonly=True)
        with self.assertRaises(AssertionError): self.e.val1 = 1

    def test_dir_regattr1(self):
        self.rm(path(self.root, trash_dir:='trash_dir'))
        self.e.regattr(trash_dir, trash_dir, is_dir=True, exists_ok=False)
        self.assertTrue(self.e.trash_dir in os.listdir(p:=path(Maester.root, Entry.root)))

    def test_dir_regattr2(self):
        self.rm(path(self.root, trash_dir:='trash_dir'))
        self.e.regattr(trash_dir, trash_dir, is_dir=True, exists_ok=False)
        self.assertTrue(self.e.trash_dir in os.listdir(Maester.root))

    def test_dir_regattr2(self):
        self.rm(path(self.root, trash_dir:='trash_dir'))
        self.e.regattr(trash_dir, trash_dir, is_dir=True, exists_ok=True)
        with self.assertRaises(FileExistsError):
            self.e.regattr(trash_dir, trash_dir, is_dir=True, exists_ok=False)

    def test_append_file1(self):
        self.rm(attr_fp:=path(self.root, attr:='attr'))
        self.e.regattr(attr, data:='12234', append=True)
        self.assertEqual(self.e.attr, data)
        with open(attr_fp) as f: self.assertEqual(f.read(), data)
        self.e.attr = data
        with open(attr_fp) as f: self.assertEqual(f.read(), data+data)

    def test_write_file1(self):
        self.rm(attr_fp:=path(self.root, attr:='attr'))
        self.e.regattr(attr, data:='12234')
        self.e.attr = data
        with open(attr_fp) as f: self.assertEqual(f.read(), data)

    @unittest.skip("to be fixed soon")
    def test_getattr_file(self):
        self.tearDown()
        self.setUp(mem_th=10)
        fn, data = 'mydata', str([str(i) for i in range(11)])
        self.e.regattr(fn, data)
        self.assertEqual(self.e.mydata, data)
        self.assertTrue(fn not in self.__dict__)

    @unittest.skip("need Entry attributes")
    def test_set_error(self):
        errm = "foo"
        self.assertEqual(self.e.status, self.e.Status.created)
        self.e.set_error(errm)
        self.assertEqual(self.e.status, self.e.Status.error)
        with open(path(Maester.root, self.e.root, self.e.name+"_error"), 'r') as f:
            self.assertEqual(f.read(), errm)
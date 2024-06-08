from yaat.util import killproc, gettime, DEBUG
from yaat.maester import Maester, TimeRange, TIME_FORMAT, DATE_FORMAT
from datetime import date, time, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import unittest, atexit, functools, shutil, pymongo.errors as mongoerrs

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]

class TestTimeRange(unittest.TestCase):
    start, end, times = date(2020, 1, 1), date(2020, 1, 1), [time(), time(1)]
    start_str, end_str, times_str = start.strftime(DATE_FORMAT), date(2020, 1, 2).strftime(DATE_FORMAT), [t.strftime(TIME_FORMAT) for t in times]

    # should probably test clean_date and clean_time too but this mostly covers it

    def test_bad_arg_types(self):
        with self.assertRaises(RuntimeError): TimeRange(1, 1, 1)
        with self.assertRaises(RuntimeError): TimeRange(self.start, 1, 1)
        with self.assertRaises(RuntimeError): TimeRange(1, self.end, 1)
        with self.assertRaises(RuntimeError): TimeRange(self.start, self.end, 1)
        with self.assertRaises(RuntimeError): TimeRange(self.start, self.end, [1])

    def test_good_arg_types(self):
        for s in (self.start, self.start_str):
            for e in (self.end, self.end_str):
                for ts in (self.times, self.times_str):
                    TimeRange(s, e, ts)

    def test_start_greater_end(self):
        with self.assertRaises(RuntimeError): TimeRange(date(2020, 1, 2), date(2020, 1, 1), self.times)

    def test_times_w_tz(self):
        with self.assertRaises(RuntimeError): TimeRange(self.start, self.end, [time(tzinfo=ZoneInfo('UTC'))])

    def test_bad_calendar_name(self):
        with self.assertRaises(RuntimeError): TimeRange(self.start, self.end, self.times, 'non existant calendar name')

    def test_days(self):
        start = date(2024, 6, 3)
        for i in range(5):
            self.assertEqual(len(TimeRange(start, start + timedelta(days=i), self.times).days), i+1) # inclusive
    
        self.assertEqual(len(TimeRange(start, start + timedelta(days=5), self.times).days), 5) # saturday
        self.assertEqual(len(TimeRange(start, start + timedelta(days=6), self.times).days), 5) # sunday
        self.assertEqual(len(TimeRange(start, start + timedelta(days=7), self.times).days), 6) # monday

    def test_timestamps(self):
        start = date(2024, 6, 3)
        for i in range(5):
            self.assertEqual(len(TimeRange(start, start + timedelta(days=i), self.times).timestamps), (i+1) * len(self.times)) # inclusive
    
        self.assertEqual(len(TimeRange(start, start + timedelta(days=5), self.times).timestamps), 5 * len(self.times)) # saturday
        self.assertEqual(len(TimeRange(start, start + timedelta(days=6), self.times).timestamps), 5 * len(self.times)) # sunday
        self.assertEqual(len(TimeRange(start, start + timedelta(days=7), self.times).timestamps), 6 * len(self.times)) # monday


class TestMaesterDB(unittest.TestCase):

    # setup

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = Path(f"twork/twork_{cls.__name__}_{gettime()}")
        cls.dp.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: shutil.rmtree(cls.dp)

    def setUp(self) -> None:
        self.m = None
        return super().setUp()

    def tearDown(self) -> None:
        if hasattr(self, 'm'): del self.m
        return super().tearDown()
    
    # tests

    def test_connstr_and_dbdir(self):
        with self.assertRaises(RuntimeError): Maester('some connstr', 'some dbdir')

    def test_local_db(self):
        self.m = Maester(dbdir=self.dp / (getid(self) + '_db'))

    def test_cleanup(self):
        self.m = Maester(dbdir=self.dp / (getid(self) + '_db'))
        proc = self.m.mongo_proc
        del self.m
        self.assertEqual(proc.poll(), 0)

    def test_2_dbs(self):
        self.m = Maester(dbdir=self.dp / (getid(self) + '_db1'))
        with self.assertRaises(RuntimeError):  Maester(dbdir=self.dp / (getid(self) + '_db2'))
    
    def test_connstr(self):
        _, proc = Maester.startlocdb(self.dp / (getid(self) + '_db'))
        atexit.register(functools.partial(killproc, proc))
        self.m = Maester('localhost:27017')
        del self.m
        self.assertNotEqual(proc.poll(), 0)
        killproc(proc)


class TestMaester(unittest.TestCase):
    start, end, times = date(2024, 6, 3), date(2024, 6, 3), [time(), time(1)]

    # setup

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = Path(f"twork/twork_{cls.__name__}_{gettime()}")
        cls.dp.mkdir(parents=True, exist_ok=True)
        cls.maester = Maester(None, cls.dp / 'dbdir')

    @classmethod
    def tearDownClass(cls) -> None:
        del cls.maester
        if not DEBUG: shutil.rmtree(cls.dp)
    
    def setUp(self) -> None:
        self.maester.tickers.delete_many({})
        return super().setUp()

    def tearDown(self) -> None:
        self.maester.tickers.delete_many({})
        return super().tearDown()

    # tests

    def test_tickers_coll_info(self):
        self.assertIn('tickers', self.maester.db.list_collection_names())
        self.assertDictEqual(self.maester.tickers.index_information(),{
            '_id_': {'key': [('_id', 1)], 'v': 2},
            'timestamp_1': {'key': [('timestamp', 1)], 'v': 2},
            'symbol_1': {'key': [('symbol', 1)], 'v': 2},
            'symbol_1_timestamp_1': {'key': [('symbol', 1), ('timestamp', 1)],'unique': True,'v': 2}})

    def test_tickers_schema_bad_doc(self):
        with self.assertRaises(mongoerrs.WriteError): self.maester.tickers.insert_one({'dummy': 'doc'})

    def test_tickers_duplicate_doc(self):
        self.maester.tickers.insert_one(doc:={'symbol': 'some field', 'timestamp': datetime.now(), 'open': 1.0, 'close': 1.0, 'high': 1.0, 'low': 1.0, 'volume': 1})
        with self.assertRaises(mongoerrs.DuplicateKeyError): self.maester.tickers.insert_one(doc)
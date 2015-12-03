from pyiso import client_factory, BALANCING_AUTHORITIES
from pyiso.base import BaseClient
from pyiso.eu import EUClient
from unittest import TestCase
import pytz
from datetime import datetime, timedelta


class TestBaseLoad(TestCase):
    def setUp(self):
        # set up expected values from base client
        bc = BaseClient()
        self.MARKET_CHOICES = bc.MARKET_CHOICES
        self.FREQUENCY_CHOICES = bc.FREQUENCY_CHOICES

        # set up other expected values
        self.BA_CHOICES = BALANCING_AUTHORITIES.keys()

    def _run_test(self, ba_name, expect_data=True, **kwargs):
        # set up
        c = client_factory(ba_name)

        # get data
        data = c.get_load(**kwargs)

        # test number
        if expect_data:
            self.assertGreaterEqual(len(data), 1)
        else:
            self.assertEqual(data, [])

        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['load_MW', 'ba_name',
                                  'timestamp', 'freq', 'market']),
                             set(dp.keys()))

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric gen
            self.assertGreaterEqual(dp['load_MW']+1, dp['load_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

        # return
        return data

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_load)


class TestBPALoad(TestBaseLoad):
    def test_latest(self):
        # basic test
        data = self._run_test('BPA', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('BPA', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('BPA', start_at=today-timedelta(days=20),
                              end_at=today-timedelta(days=10))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestCAISOLoad(TestBaseLoad):
    def test_latest(self):
        # basic test
        data = self._run_test('CAISO', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('CAISO', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('CAISO', start_at=today + timedelta(hours=20),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestERCOTLoad(TestBaseLoad):
    def test_latest(self):
        # basic test
        data = self._run_test('ERCOT', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('ERCOT', start_at=today + timedelta(hours=20),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=20))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


class TestISONELoad(TestBaseLoad):
    def test_latest(self):
        # basic test
        data = self._run_test('ISONE', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('ISONE', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_forecast(self):
        # basic test
        data = self._run_test('ISONE', forecast=True, market='DAHR', freq='1hr')

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestMISOLoad(TestBaseLoad):
    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('MISO', start_at=today + timedelta(hours=10),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=10))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


class TestNEVPLoad(TestBaseLoad):
    def test_latest(self):
        # basic test
        data = self._run_test('NEVP', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NEVP', start_at=today-timedelta(days=1),
                              end_at=today)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NEVP', start_at=today-timedelta(days=35),
                              end_at=today-timedelta(days=33))


class TestNYISOLoad(TestBaseLoad):
    def test_latest(self):
        # basic test
        data = self._run_test('NYISO', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NYISO', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NYISO', start_at=today + timedelta(hours=20),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=20))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


class TestPJMLoad(TestBaseLoad):
    def test_latest(self):
        # basic test
        data = self._run_test('PJM', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('PJM', start_at=today + timedelta(hours=20),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=20))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


class TestSPPLoad(TestBaseLoad):
    def test_failing(self):
        self._run_notimplemented_test('SPP')


class TestSPPCLoad(TestBaseLoad):
    def test_latest(self):
        # basic test
        data = self._run_test('SPPC', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('SPPC', start_at=today-timedelta(days=1),
                              end_at=today)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('SPPC', start_at=today-timedelta(days=35),
                              end_at=today-timedelta(days=33))


class TestSVERILoad(TestBaseLoad):
    def setUp(self):
        super(TestSVERILoad, self).setUp()
        self.bas = [k for k, v in BALANCING_AUTHORITIES.items() if v['module'] == 'sveri']

    def test_latest_all(self):
        for ba in self.bas:
            self._test_latest(ba)

    def test_date_range_all(self):
        for ba in self.bas:
            self._test_date_range(ba)

    def _test_latest(self, ba):
        # basic test
        data = self._run_test(ba, latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def _test_date_range(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test(ba, start_at=today - timedelta(days=3),
                              end_at=today - timedelta(days=2), market=self.MARKET_CHOICES.fivemin)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)


class TestEULoad(TestBaseLoad):
    def setUp(self):
        super(TestEULoad, self).setUp()
        self.BA_CHOICES = EUClient.CONTROL_AREAS.keys()

    def test_latest(self):
        # basic test
        data = self._run_test('EU', latest=True, market=self.MARKET_CHOICES.hourly,
                              control_area='IT')

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('EU', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1),
                              control_area='IT')

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('EU', start_at=today+timedelta(hours=20),
                              end_at=today+timedelta(days=1),
                              control_area='IT')

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

from pyiso import client_factory
from pyiso.base import BaseClient
from unittest import TestCase
import pytz
from datetime import datetime, timedelta
import logging


class TestBaseTrade(TestCase):
    def setUp(self):
        # set up expected values from base client
        bc = BaseClient()
        self.MARKET_CHOICES = bc.MARKET_CHOICES
        self.FREQUENCY_CHOICES = bc.FREQUENCY_CHOICES

        # set up other expected values
        self.BA_CHOICES = ['ISONE', 'MISO', 'SPP', 'BPA', 'CAISO', 'NYISO', 'ERCOT', 'PJM']

    def create_client(self, ba_name):
        # set up client with logging
        c = client_factory(ba_name)
        handler = logging.StreamHandler()
        c.logger.addHandler(handler)
        c.logger.setLevel(logging.INFO)
        return c

    def _run_test(self, ba_name, **kwargs):
        # set up
        c = self.create_client(ba_name)

        # get data
        data = c.get_trade(**kwargs)

        # test number
        self.assertGreaterEqual(len(data), 1)

        # test contents
        for dp in data:
            # test key names
            for key in ['ba_name', 'timestamp', 'freq', 'market']:
                self.assertIn(key, dp.keys())
            self.assertEqual(len(dp.keys()), 5)

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric value
            self.assertGreaterEqual(dp['net_exp_MW']+1, dp['net_exp_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

        # return
        return data

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = self.create_client(ba_name)

        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_trade)

    def _run_failing_test(self, ba_name, **kwargs):
        # set up
        c = self.create_client(ba_name)

        # method not implemented yet
        self.assertRaises(ValueError, c.get_trade)


class TestBPATrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('BPA')


class TestCAISOTrade(TestBaseTrade):
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

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('CAISO', start_at=today+timedelta(hours=10),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

       # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.dam)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)


class TestNYISOTrade(TestBaseTrade):
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

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range_short(self):
        # basic test
        data = self._run_test('NYISO', start_at=datetime.now()-timedelta(minutes=10),
                              end_at=datetime.now()-timedelta(minutes=5))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range_future(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_failing_test('NYISO', start_at=today+timedelta(days=1),
                               end_at=today+timedelta(days=2))


class TestERCOTTrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('ERCOT')


class TestISONETrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('ISONE')


class TestMISOTrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('MISO')


class TestPJMTrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('PJM')


class TestSPPTrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('SPP')

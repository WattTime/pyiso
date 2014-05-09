from pyiso import client_factory
from pyiso.base import BaseClient
from unittest import TestCase
import pytz
from datetime import datetime, timedelta
import logging


class TestBaseLoad(TestCase):
    def setUp(self):
        # set up expected values from base client
        bc = BaseClient()
        self.MARKET_CHOICES = bc.MARKET_CHOICES
        self.FREQUENCY_CHOICES = bc.FREQUENCY_CHOICES

        # set up other expected values
        self.BA_CHOICES = ['ISONE', 'MISO', 'SPP', 'BPA', 'CAISO', 'ERCOT', 'PJM']

    def create_client(self, ba_name):
        # set up client with logging
        c = client_factory(ba_name)
        handler = logging.StreamHandler()
        c.logger.addHandler(handler)
        c.logger.setLevel(logging.DEBUG)
        return c

    def _run_test(self, ba_name, **kwargs):
        # set up
        c = self.create_client(ba_name)

        # get data
        data = c.get_load(**kwargs)
        
        # test number
        self.assertGreaterEqual(len(data), 1)
                
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

            # test earlier than now
            self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))
            
        # return
        return data

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = self.create_client(ba_name)

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
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestCAISOLoad(TestBaseLoad):
    def test_failing(self):
        self._run_notimplemented_test('CAISO')


class TestERCOTLoad(TestBaseLoad):
    def test_failing(self):
        self._run_notimplemented_test('ERCOT')


class TestISONELoad(TestBaseLoad):
    def test_failing(self):
        self._run_notimplemented_test('ISONE')


class TestMISOLoad(TestBaseLoad):
    def test_failing(self):
        self._run_notimplemented_test('MISO')


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


class TestSPPLoad(TestBaseLoad):
    def test_failing(self):
        self._run_notimplemented_test('SPP')

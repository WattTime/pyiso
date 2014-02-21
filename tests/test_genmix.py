from apps.clients import client_factory
from django.test import TestCase
from apps.gridentities.models import FuelType, BalancingAuthority
from apps.griddata.models import DataPoint
import pytz
from datetime import datetime, timedelta

class TestGenMix(TestCase):
    fixtures = ['isos.json', 'gentypes.json']
    
    def _run_test(self, ba_name, **kwargs):
        # get data
        c = client_factory(ba_name)
        data = c.get_generation(**kwargs)
        
        # test number
        self.assertGreater(len(data), 1)
                
        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['gen_MW', 'ba_name', 'fuel_name',
                                  'timestamp', 'freq', 'market']),
                             set(dp.keys()))
    
            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertEqual(FuelType.objects.filter(name=dp['fuel_name']).count(), 1)
            self.assertEqual(BalancingAuthority.objects.filter(abbrev=dp['ba_name']).count(), 1)
            
        # return
        return data
        
    def test_isne_latest(self):
        # basic test
        data = self._run_test('ISONE', latest=True)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)
                
    def test_isne_date_range(self):
        # basic test
        data = self._run_test('ISONE', start_at=datetime.today()-timedelta(days=2),
                              end_at=datetime.today()-timedelta(days=1))
        
        # test multiple
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_miso_latest(self):
        # basic test
        data = self._run_test('MISO', latest=True)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)
                
    def test_spp_latest_hr(self):
        # basic test
        data = self._run_test('SPP', latest=True, market=DataPoint.RTHR)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)
        
        # test flags
        for dp in data:
            self.assertEqual(dp['market'], DataPoint.RTHR)
            self.assertEqual(dp['freq'], DataPoint.HOURLY)                
        
    def test_spp_date_range_hr(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('SPP', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1),
                                market=DataPoint.RTHR)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)
        
        # test flags
        for dp in data:
            self.assertEqual(dp['market'], DataPoint.RTHR)
            self.assertEqual(dp['freq'], DataPoint.HOURLY)                
        
    def test_spp_latest_5min(self):
        # basic test
        data = self._run_test('SPP', latest=True, market=DataPoint.RT5M)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)
        
        # test flags
        for dp in data:
            self.assertEqual(dp['market'], DataPoint.RT5M)
            self.assertEqual(dp['freq'], DataPoint.FIVEMIN)                
        
    def test_bpa_latest(self):
        # basic test
        data = self._run_test('BPA', latest=True, market=DataPoint.RT5M)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)
        
        # test flags
        for dp in data:
            self.assertEqual(dp['market'], DataPoint.RT5M)
            self.assertEqual(dp['freq'], DataPoint.FIVEMIN)                

    def test_bpa_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('BPA', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_bpa_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('BPA', start_at=today-timedelta(days=20),
                              end_at=today-timedelta(days=10))
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)
        
    def test_caiso_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('CAISO', start_at=today-timedelta(days=3),
                              end_at=today-timedelta(days=2), market=DataPoint.RTHR)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)
        
        # test flags
        for dp in data:
            self.assertEqual(dp['market'], DataPoint.RTHR)
            self.assertEqual(dp['freq'], DataPoint.HOURLY)                

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['solarpv', 'solarth', 'geo', 'smhydro', 'wind', 'biomass', 'biogas',
                          'thermal', 'hydro', 'nuclear']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)

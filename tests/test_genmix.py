from apps.clients import client_factory
from django.test import TestCase
from apps.gridentities.models import FuelType, BalancingAuthority
import pytz
from datetime import datetime

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
            self.assertEqual(set(['gen_MW', 'ba_name', 'fuel_name', 'timestamp']),
                             set(dp.keys()))
    
            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertEqual(FuelType.objects.filter(name=dp['fuel_name']).count(), 1)
            self.assertEqual(BalancingAuthority.objects.filter(abbrev=dp['ba_name']).count(), 1)
            
        # return
        return data
        
    def test_isne_latest(self):
        # basic test
        data = self._run_test('ISNE', latest=True)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)
                
    def test_isne_date_range(self):
        # basic test
        data = self._run_test('ISNE', start_at=datetime(2014, 2, 1), end_at=datetime(2014, 2, 2))
        
        # test multiple
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_miso_latest(self):
        # basic test
        data = self._run_test('MISO', latest=True)
        
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)
                
        
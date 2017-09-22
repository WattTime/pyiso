from pyiso import client_factory
from unittest import TestCase
from datetime import datetime, timedelta
import pytz
import dateutil.parser


class IntegrationTestISONE(TestCase):
    def setUp(self):
        self.c = client_factory('ISONE')

    def test_genmix_json_format(self):
        data = self.c.fetch_data('/genfuelmix/current.json', self.c.auth)

        # one item, GenFuelMixes
        self.assertIn('GenFuelMixes', data.keys())
        self.assertEqual(len(data.keys()), 1)

        # one item, GenFuelMix
        self.assertIn('GenFuelMix', data['GenFuelMixes'].keys())
        self.assertEqual(len(data['GenFuelMixes'].keys()), 1)

        # multiple items
        self.assertGreater(len(data['GenFuelMixes']['GenFuelMix']), 1)
        self.assertEqual(sorted(data['GenFuelMixes']['GenFuelMix'][0].keys()),
                         sorted(['FuelCategory', 'BeginDate', 'MarginalFlag', 'FuelCategoryRollup', 'GenMw']))

    def test_load_realtime_json_format(self):
        data = self.c.fetch_data('/fiveminutesystemload/current.json', self.c.auth)

        # one item, FiveMinSystemLoad
        self.assertIn('FiveMinSystemLoad', data.keys())
        self.assertEqual(len(data.keys()), 1)

        # several items, including date and load
        self.assertEqual(len(data['FiveMinSystemLoad']), 1)
        self.assertIn('BeginDate', data['FiveMinSystemLoad'][0].keys())
        self.assertIn('LoadMw', data['FiveMinSystemLoad'][0].keys())

        # values
        date = dateutil.parser.parse(data['FiveMinSystemLoad'][0]['BeginDate'])
        now = pytz.utc.localize(datetime.utcnow())
        self.assertLessEqual(date, now)
        self.assertGreaterEqual(date, now - timedelta(minutes=6))
        self.assertEqual(date.minute % 5, 0)
        self.assertGreater(data['FiveMinSystemLoad'][0]['LoadMw'], 0)

    def test_load_past_json_format(self):
        data = self.c.fetch_data('/fiveminutesystemload/day/20150610.json', self.c.auth)

        # one item, FiveMinSystemLoads
        self.assertIn('FiveMinSystemLoads', data.keys())
        self.assertEqual(len(data.keys()), 1)
        self.assertIn('FiveMinSystemLoad', data['FiveMinSystemLoads'].keys())
        self.assertEqual(len(data['FiveMinSystemLoads'].keys()), 1)

        # 288 elements: every 5 min
        self.assertEqual(len(data['FiveMinSystemLoads']['FiveMinSystemLoad']), 288)

        # several items, including date and load
        self.assertIn('BeginDate', data['FiveMinSystemLoads']['FiveMinSystemLoad'][0].keys())
        self.assertIn('LoadMw', data['FiveMinSystemLoads']['FiveMinSystemLoad'][0].keys())

    def test_load_forecast_json_format(self):
        # two days in future
        day_after_tomorrow = pytz.utc.localize(datetime.utcnow()) + timedelta(days=2)
        date_str = day_after_tomorrow.astimezone(pytz.timezone('US/Eastern')).strftime('%Y%m%d')
        data = self.c.fetch_data('/hourlyloadforecast/day/%s.json' % date_str, self.c.auth)

        # one item, u'HourlyLoadForecasts
        self.assertIn('HourlyLoadForecasts', data.keys())
        self.assertEqual(len(data.keys()), 1)
        self.assertIn('HourlyLoadForecast', data['HourlyLoadForecasts'].keys())
        self.assertEqual(len(data['HourlyLoadForecasts'].keys()), 1)

        # 24 elements: every 1 hr
        self.assertEqual(len(data['HourlyLoadForecasts']['HourlyLoadForecast']), 24)

        # several items, including date and load
        self.assertIn('BeginDate', data['HourlyLoadForecasts']['HourlyLoadForecast'][0].keys())
        self.assertIn('LoadMw', data['HourlyLoadForecasts']['HourlyLoadForecast'][0].keys())

    def test_lmp_realtime_json_format(self):
        data = self.c.fetch_data('/fiveminutelmp/current/location/4001.json', self.c.auth)

        # one item, FiveMinLmp
        self.assertIn('FiveMinLmp', data.keys())
        self.assertEqual(len(data.keys()), 1)

        # several items, including date and lmp
        self.assertEqual(len(data['FiveMinLmp']), 1)
        self.assertIn('BeginDate', data['FiveMinLmp'][0].keys())
        self.assertIn('LmpTotal', data['FiveMinLmp'][0].keys())

        # values
        date = dateutil.parser.parse(data['FiveMinLmp'][0]['BeginDate'])
        now = pytz.utc.localize(datetime.utcnow())
        self.assertLessEqual(date, now)
        self.assertGreaterEqual(date, now - timedelta(minutes=6))
        self.assertEqual(date.minute % 5, 0)
        self.assertGreater(data['FiveMinLmp'][0]['LmpTotal'], -150)

    def test_lmp_past_json_format(self):
        data = self.c.fetch_data('/fiveminutelmp/day/20150610/location/4001.json', self.c.auth)

        # one item, FiveMinLmps
        self.assertIn('FiveMinLmps', data.keys())
        self.assertEqual(len(data.keys()), 1)
        self.assertIn('FiveMinLmp', data['FiveMinLmps'].keys())
        self.assertEqual(len(data['FiveMinLmps'].keys()), 1)

        # 288 elements: every 5 min
        self.assertEqual(len(data['FiveMinLmps']['FiveMinLmp']), 288)

        # several items, including date and lmp
        self.assertIn('BeginDate', data['FiveMinLmps']['FiveMinLmp'][0].keys())
        self.assertIn('LmpTotal', data['FiveMinLmps']['FiveMinLmp'][0].keys())

    def test_get_lmp_hist(self):
        start_at = datetime(2015, 1, 1, 1, 0, 0, 0,
                            tzinfo=pytz.timezone('US/Eastern')).astimezone(pytz.utc)
        end_at = start_at + timedelta(minutes=55)

        prices = self.c.get_lmp('NEMASSBOST', latest=False, start_at=start_at, end_at=end_at)
        self.assertEqual(len(prices), 11)
        self.assertGreaterEqual(prices[0]['timestamp'], start_at)
        self.assertLessEqual(prices[0]['timestamp'], start_at + timedelta(minutes=5))
        self.assertEqual(prices[0]['lmp'], 56.92)

    def test_get_lmp_latest(self):
        prices = self.c.get_lmp('NEMASSBOST')
        self.assertEqual(len(prices), 1)
        self.assertLess(prices[0]['lmp'], 1500)
        self.assertGreater(prices[0]['lmp'], -300)

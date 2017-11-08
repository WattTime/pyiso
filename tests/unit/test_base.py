from unittest import TestCase
from pyiso.base import BaseClient
from datetime import datetime, timedelta
import pytz
import pandas as pd


class TestBaseClient(TestCase):
    def test_init(self):
        """init creates empty options dict"""
        bc = BaseClient()
        self.assertIsNotNone(bc)
        self.assertEqual(len(bc.options.keys()), 0)

    def test_market_choices(self):
        """Market choices have expected values."""
        bc = BaseClient()

        self.assertEqual('RTHR', bc.MARKET_CHOICES.hourly)
        self.assertEqual('RT5M', bc.MARKET_CHOICES.fivemin)
        self.assertEqual('DAHR', bc.MARKET_CHOICES.dam)

    def test_freq_choices(self):
        """Frequency choices have expected values."""
        bc = BaseClient()

        self.assertEqual('1hr', bc.FREQUENCY_CHOICES.hourly)
        self.assertEqual('5m', bc.FREQUENCY_CHOICES.fivemin)
        self.assertEqual('10m', bc.FREQUENCY_CHOICES.tenmin)
        self.assertEqual('n/a', bc.FREQUENCY_CHOICES.na)

    def test_handle_options_set(self):
        """Can set options"""
        bc = BaseClient()
        bc.handle_options(test='a', another=20)
        self.assertEqual(bc.options['test'], 'a')
        self.assertEqual(bc.options['another'], 20)
        self.assertFalse(bc.options['sliceable'])

    def test_handle_options_latest(self):
        """Correct processing of time-related options for latest"""
        bc = BaseClient()
        bc.handle_options(latest=True)
        self.assertTrue(bc.options['latest'])
        self.assertFalse(bc.options['sliceable'])
        self.assertFalse(bc.options['forecast'])

    def test_handle_options_past(self):
        """Correct processing of time-related options for historical start and end times"""
        bc = BaseClient()
        bc.handle_options(start_at='2014-01-01', end_at='2014-02-01')
        self.assertTrue(bc.options['sliceable'])
        self.assertEqual(bc.options['start_at'], datetime(2014, 1, 1, 0, tzinfo=pytz.utc))
        self.assertEqual(bc.options['end_at'], datetime(2014, 2, 1, 0, tzinfo=pytz.utc))
        self.assertFalse(bc.options['forecast'])

    def test_handle_options_inverted_start_end(self):
        """Raises error if end before start"""
        bc = BaseClient()
        self.assertRaises(AssertionError, bc.handle_options, start_at='2014-02-01', end_at='2014-01-01')

    def test_handle_options_future(self):
        """Correct processing of time-related options for future start and end times"""
        bc = BaseClient()
        bc.handle_options(start_at='2100-01-01', end_at='2100-02-01')
        self.assertTrue(bc.options['sliceable'])
        self.assertEqual(bc.options['start_at'], datetime(2100, 1, 1, 0, tzinfo=pytz.utc))
        self.assertEqual(bc.options['end_at'], datetime(2100, 2, 1, 0, tzinfo=pytz.utc))
        self.assertTrue(bc.options['forecast'])

    def test_handle_options_yesterday(self):
        """Correct auto-setup of time-related options for yesterday"""
        bc = BaseClient()
        bc.handle_options(yesterday=True)
        self.assertTrue(bc.options['sliceable'])
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.utc)
        midnight_today = datetime(local_now.year, local_now.month, local_now.day, 0, tzinfo=pytz.utc)
        self.assertEqual(bc.options['start_at'], midnight_today - timedelta(days=1))
        self.assertEqual(bc.options['end_at'], midnight_today)
        self.assertFalse(bc.options['forecast'])

    def test_handle_options_forecast(self):
        """Correct auto-setup of time-related options for forecast"""
        bc = BaseClient()
        bc.handle_options(forecast=True)
        self.assertTrue(bc.options['sliceable'])
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.utc).replace(microsecond=0)
        self.assertEqual(bc.options['start_at'], local_now)
        self.assertEqual(bc.options['end_at'], local_now + timedelta(days=2))
        self.assertTrue(bc.options['forecast'])

    def test_handle_options_twice(self):
        """Overwrite options on second call"""
        bc = BaseClient()
        bc.handle_options(forecast=True)
        self.assertTrue(bc.options['forecast'])

        bc.handle_options(yesterday=True)
        self.assertFalse(bc.options['forecast'])

    def test_handle_options_set_forecast(self):
        bc = BaseClient()
        start = datetime(2020, 5, 26, 0, 0, tzinfo=pytz.utc)
        bc.handle_options(start_at=start, end_at=start+timedelta(days=2))
        self.assertTrue(bc.options['forecast'])

    def test_bad_zipfile(self):
        bc = BaseClient()
        badzip = 'I am not a zipfile'
        result = bc.unzip(badzip)
        self.assertIsNone(result)

    def test_slice_empty(self):
        bc = BaseClient()
        indf = pd.DataFrame()
        outdf = bc.slice_times(indf, {'latest': True})
        self.assertEqual(len(outdf), 0)

    def test_timeout(self):
        bc = BaseClient(timeout_seconds=30)
        self.assertEqual(bc.timeout_seconds, 30)

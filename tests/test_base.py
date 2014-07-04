from unittest import TestCase
from pyiso.base import BaseClient
import logging
from datetime import datetime
import pytz


class TestBaseClient(TestCase):
    def test_init(self):
        """init creates empty options dict"""
        bc = BaseClient()
        self.assertIsNotNone(bc)
        self.assertEqual(len(bc.options.keys()), 0)
        
    def test_has_logger(self):
        """BaseClient has logger attribute that acts like logger"""
        bc = BaseClient()
        
        # attribute exists
        logger = getattr(bc, 'logger', None)
        self.assertIsNotNone(logger)
        
        # can accept handler
        handler = logging.StreamHandler()
        logger.addHandler(handler)

    def test_market_choices(self):
        """Market choices have expected values."""
        bc = BaseClient()

        self.assertEqual('RTHR', bc.MARKET_CHOICES.hourly)
        self.assertEqual('RT5M', bc.MARKET_CHOICES.fivemin)
        self.assertEqual('DAM', bc.MARKET_CHOICES.dam)

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
        self.assertEqual(bc.options['start_at'], datetime(local_now.year, local_now.month, local_now.day-1, 0, tzinfo=pytz.utc))
        self.assertEqual(bc.options['end_at'], datetime(local_now.year, local_now.month, local_now.day, 0, tzinfo=pytz.utc))
        self.assertFalse(bc.options['forecast'])

    def test_handle_options_forecast(self):
        """Correct auto-setup of time-related options for forecast"""
        bc = BaseClient()
        bc.handle_options(forecast=True)
        self.assertTrue(bc.options['sliceable'])
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.utc)
        self.assertEqual(bc.options['start_at'], datetime(local_now.year, local_now.month, local_now.day, 0, tzinfo=pytz.utc))
        self.assertEqual(bc.options['end_at'], datetime(local_now.year, local_now.month, local_now.day+2, 0, tzinfo=pytz.utc))
        self.assertTrue(bc.options['forecast'])

    def test_bad_zipfile(self):
        bc = BaseClient()
        badzip = 'I am not a zipfile'
        result = bc.unzip(badzip)
        self.assertIsNone(result)

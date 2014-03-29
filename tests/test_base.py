from unittest import TestCase
from grid_clients.base import BaseClient
import logging


class TestBaseClient(TestCase):
    def test_init(self):
        bc = BaseClient()
        self.assertIsNotNone(bc)
        
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

    def test_freq_choices(self):
        """Frequency choices have expected values."""
        bc = BaseClient()

        self.assertEqual('1hr', bc.FREQUENCY_CHOICES.hourly)
        self.assertEqual('5m', bc.FREQUENCY_CHOICES.fivemin)
        self.assertEqual('10m', bc.FREQUENCY_CHOICES.tenmin)
        self.assertEqual('n/a', bc.FREQUENCY_CHOICES.na)

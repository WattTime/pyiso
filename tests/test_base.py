from django.test import TestCase
from apps.clients.base import BaseClient
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
        
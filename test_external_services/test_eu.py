from pyiso import client_factory
from unittest import TestCase
from datetime import datetime
import mock
import pytz

class TestEU(TestCase):
    def setUp(self):
        self.c = client_factory('EU')

    def test_auth(self):
        self.assertTrue(self.c.auth())

from unittest import TestCase

from pyiso import client_factory


class TestIESO(TestCase):
    def setUp(self):
        self.c = client_factory('IESO')


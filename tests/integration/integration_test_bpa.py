from unittest import TestCase

from pyiso import client_factory


class IntegrationTestBPABase(TestCase):

    def test_request_latest(self):
        c = client_factory('BPA')
        response = c.request('http://transmission.bpa.gov/business/operations/wind/baltwg.txt')
        self.assertIn('BPA Balancing Authority Load & Total Wind Generation', response.text)

import os
from io import StringIO

import requests_mock

from pyiso import client_factory
from unittest import TestCase, skip
import pandas as pd
import pytz
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup

fixtures_base_path = os.path.join(os.path.dirname(__file__), '../fixtures/caiso')


def read_fixture(filename):
    return open(os.path.join(fixtures_base_path, filename), 'r').read()


class TestCAISOBase(TestCase):
    def setUp(self):
        self.c = client_factory('CAISO')
        self.ren_report_tsv = read_fixture('ren_report.csv')
        self.sld_fcst_xml = read_fixture('sld_forecast.xml')
        self.ene_slrs_xml = read_fixture('ene_slrs.xml')
        self.sld_ren_fcst_xml = read_fixture('sld_ren_forecast.xml')
        self.systemconditions_html = read_fixture('systemconditions.html')
        self.todays_outlook_renewables = read_fixture('todays_outlook_renewables.html')

    def test_parse_ren_report_top(self):
        # top half
        top_df = self.c.parse_to_df(self.ren_report_tsv, skiprows=1, nrows=24, header=0, delimiter='\t+',
                                    engine='python')
        self.assertEqual(list(top_df.columns), ['Hour', 'GEOTHERMAL', 'BIOMASS', 'BIOGAS', 'SMALL HYDRO', 'WIND TOTAL',
                                                'SOLAR PV', 'SOLAR THERMAL'])
        self.assertEqual(len(top_df), 24)

    def test_parse_ren_report_bottom(self):
        # bottom half
        bot_df = self.c.parse_to_df(self.ren_report_tsv, skiprows=29, nrows=24, header=0, delimiter='\t+',
                                    engine='python')
        self.assertEqual(list(bot_df.columns), ['Hour', 'RENEWABLES', 'NUCLEAR', 'THERMAL', 'IMPORTS', 'HYDRO'])
        self.assertEqual(len(bot_df), 24)

    def test_dt_index(self):
        df = self.c.parse_to_df(self.ren_report_tsv, skiprows=1, nrows=24, header=0, delimiter='\t+', engine='python')
        indexed = self.c.set_dt_index(df, date(2014, 3, 12), df['Hour'])
        self.assertEqual(type(indexed.index).__name__, 'DatetimeIndex')
        self.assertEqual(indexed.index[0].hour, 7)

    def test_pivot(self):
        df = self.c.parse_to_df(self.ren_report_tsv, skiprows=1, nrows=24, header=0, delimiter='\t+', engine='python')
        indexed = self.c.set_dt_index(df, date(2014, 3, 12), df['Hour'])
        indexed.pop('Hour')
        pivoted = self.c.unpivot(indexed)

        # no rows with 'Hour'
        hour_rows = pivoted[pivoted['level_1'] == 'Hour']
        self.assertEqual(len(hour_rows), 0)

        # number of rows is from number of columns
        self.assertEqual(len(pivoted), 24*len(indexed.columns))

    def test_oasis_payload(self):
        self.c.handle_options(start_at='2014-01-01', end_at='2014-02-01', market=self.c.MARKET_CHOICES.fivemin,
                              data='load')
        constructed = self.c.construct_oasis_payload('SLD_FCST')
        expected = {'queryname': 'SLD_FCST',
                    'market_run_id': 'RTM',
                    'startdatetime': (datetime(2014, 1, 1, 8)).strftime(self.c.oasis_request_time_format),
                    'enddatetime': (datetime(2014, 2, 1, 8)).strftime(self.c.oasis_request_time_format),
                    'version': 1,
                    }
        self.assertEqual(constructed, expected)

    def test_parse_oasis_demand_rtm(self):
        # set up list of data
        soup = BeautifulSoup(self.sld_fcst_xml, 'xml')
        data = soup.find_all('REPORT_DATA')

        # parse
        self.c.handle_options(market=self.c.MARKET_CHOICES.fivemin, freq=self.c.FREQUENCY_CHOICES.fivemin)
        parsed_data = self.c.parse_oasis_demand_forecast(data)

        # test
        self.assertEqual(len(parsed_data), 1)
        expected = {'ba_name': 'CAISO',
                    'timestamp': datetime(2014, 5, 8, 18, 55, tzinfo=pytz.utc),
                    'freq': '5m', 'market': 'RT5M',
                    'load_MW': 26755.0}
        self.assertEqual(expected, parsed_data[0])

    def test_parse_todays_outlook_renewables(self):
        # set up soup and ts
        soup = BeautifulSoup(self.todays_outlook_renewables, 'lxml')
        ts = self.c.utcify('2014-05-08 12:00')

        # set up options
        self.c.handle_options()

        # parse
        parsed_data = self.c.parse_todays_outlook_renewables(soup, ts)

        # test
        expected = [{'ba_name': 'CAISO',
                     'freq': '10m',
                     'fuel_name': 'renewable',
                     'gen_MW': 6086.0,
                     'market': 'RT5M',
                     'timestamp': datetime(2014, 5, 8, 19, 0, tzinfo=pytz.utc)}]
        self.assertEqual(parsed_data, expected)

    def test_parse_systemconditions(self):
        """Test for a newly discovered edge case: an extra, empty `docdata` cell."""
        soup = BeautifulSoup(self.systemconditions_html, 'lxml')
        self.c.handle_options()
        parsed_data = self.c.todays_outlook_time(soup)
        expected = datetime(2017, 1, 5, 21, 50, tzinfo=pytz.utc)
        self.assertEqual(parsed_data, expected)

    def test_parse_oasis_slrs_gen_rtm(self):
        # set up list of data
        soup = BeautifulSoup(self.ene_slrs_xml, 'xml')
        data = soup.find_all('REPORT_DATA')

        # parse
        self.c.handle_options(data='gen', market=self.c.MARKET_CHOICES.fivemin, freq=self.c.FREQUENCY_CHOICES.fivemin)
        parsed_data = self.c.parse_oasis_slrs(data)

        # test
        self.assertEqual(len(parsed_data), 2)
        expected = {'ba_name': 'CAISO',
                    'timestamp': datetime(2013, 9, 19, 17, 0, tzinfo=pytz.utc),
                    'freq': '5m', 'market': 'RT5M', 'fuel_name': 'other',
                    'gen_MW': 23900.79}
        self.assertEqual(expected, parsed_data[0])

    def test_parse_oasis_slrs_trade_dam(self):
        # set up list of data
        soup = BeautifulSoup(self.ene_slrs_xml, 'xml')
        data = soup.find_all('REPORT_DATA')

        # parse
        self.c.handle_options(data='trade', market=self.c.MARKET_CHOICES.dam, freq=self.c.FREQUENCY_CHOICES.dam)
        parsed_data = self.c.parse_oasis_slrs(data)

        # test
        self.assertEqual(len(parsed_data), 3)
        expected = {'ba_name': 'CAISO',
                    'timestamp': datetime(2013, 9, 19, 7, 0, tzinfo=pytz.utc),
                    'freq': '1hr', 'market': 'DAHR',
                    'net_exp_MW': -5014.0}
        self.assertEqual(expected, parsed_data[0])

    def test_parse_oasis_renewables_dam(self):
        # set up list of data
        soup = BeautifulSoup(self.sld_ren_fcst_xml, 'xml')
        data = soup.find_all('REPORT_DATA')

        # parse
        self.c.handle_options(data='gen', market=self.c.MARKET_CHOICES.dam, freq=self.c.FREQUENCY_CHOICES.dam)
        parsed_data = self.c.parse_oasis_renewable(data)

        # test
        self.assertEqual(len(parsed_data), 6)
        expected = {'ba_name': 'CAISO',
                    'timestamp': datetime(2013, 9, 20, 6, 0, tzinfo=pytz.utc),
                    'freq': '1hr', 'market': 'DAHR', 'fuel_name': 'wind',
                    'gen_MW': 580.83}
        self.assertEqual(expected, parsed_data[0])

    @skip('Not ready yet')
    def test_lmp_loc(self):
        loc_data = self.c.get_lmp_loc()

        # one entry for each node
        self.assertGreaterEqual(len(loc_data), 4228)

        # check keys
        self.assertItemsEqual(loc_data[0].keys(),
                              ['node_id', 'latitude', 'longitude', 'area'])

    @requests_mock.Mocker()
    def test_bad_data(self, exptected_request):
        expected_url = 'http://oasis.caiso.com/oasisapi/SingleZip?node=CAISO_AS&version=1&startdatetime=20150301T10%3A00-0000&market_run_id=RTM&queryname=PRC_INTVL_LMP&resultformat=6&enddatetime=20150301T12%3A00-0000'
        exptected_request.get(expected_url, content='bad data'.encode('utf-8'))

        ts = pytz.utc.localize(datetime(2015, 3, 1, 12))
        start = ts - timedelta(hours=2)
        df = self.c.get_lmp_as_dataframe('CAISO_AS', latest=False, start_at=start, end_at=ts)

        self.assertIsInstance(df, pd.DataFrame)

    @requests_mock.Mocker()
    def test_bad_data_lmp_only(self, exptected_request):
        expected_url = 'http://oasis.caiso.com/oasisapi/SingleZip?node=CAISO_AS&version=1&startdatetime=20150301T10%3A00-0000&market_run_id=RTM&queryname=PRC_INTVL_LMP&resultformat=6&enddatetime=20150301T12%3A00-0000'
        exptected_request.get(expected_url, content='bad data'.encode('utf-8'))

        ts = pytz.utc.localize(datetime(2015, 3, 1, 12))
        start = ts - timedelta(hours=2)
        df = self.c.get_lmp_as_dataframe('CAISO_AS', latest=False, start_at=start, end_at=ts, lmp_only=False)
        self.assertIsInstance(df, pd.DataFrame)

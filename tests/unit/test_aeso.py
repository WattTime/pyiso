import os
from datetime import timedelta
from unittest import TestCase

import dateutil.parser
import pytz
import requests_mock

from pyiso import client_factory
from pyiso.base import BaseClient

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures/aeso')


class TestAESOClient(TestCase):
    def setUp(self):
        self.aeso_client = client_factory('AESO')
        self.mtn_tz = pytz.timezone('Canada/Mountain')

    def test_aeso_retrievable_from_client_factory(self):
        self.assertIsInstance(self.aeso_client, BaseClient)

    @requests_mock.Mocker()
    def test_nominal_get_generation(self, req_expectation):
        csv_content = open(FIXTURES_DIR + '/latest_electricity_market_report.csv').read().encode('ascii')

        req_expectation.get(self.aeso_client.LATEST_REPORT_URL, content=csv_content)

        generation_ts = self.aeso_client.get_generation(latest=True)

        self.assertEqual(len(generation_ts), 5)  # Five fuels
        for row in generation_ts:
            fuel_name = row.get('fuel_name', None)
            self.assertNotEqual(fuel_name, None)
            if fuel_name == 'COAL':
                self.assertEqual(row.get('gen_MW', None), 4504)
            elif fuel_name == 'GAS':
                self.assertEqual(row.get('gen_MW', None), 4321)
            elif fuel_name == 'HYDRO':
                self.assertEqual(row.get('gen_MW', None), 426)
            elif fuel_name == 'OTHER':
                self.assertTrue(row.get('gen_MW', None), 261)
            elif fuel_name == 'WIND':
                self.assertTrue(row.get('gen_MW', None), 542)
            else:
                self.fail('Unexpected fuel name found in generation timeseries')

    @requests_mock.Mocker()
    def test_nominal_get_trade(self, req_expectation):
        csv_content = open(FIXTURES_DIR + '/latest_electricity_market_report.csv').read().encode('ascii')

        req_expectation.get(self.aeso_client.LATEST_REPORT_URL, content=csv_content)

        trade_ts = self.aeso_client.get_trade(latest=True)

        self.assertEqual(len(trade_ts), 1)
        self.assertEqual(trade_ts[0].get('net_exp_MW', None), -216)

    @requests_mock.Mocker()
    def test_nominal_get_load(self, req_expectation):
        csv_content = open(FIXTURES_DIR + '/latest_electricity_market_report.csv').read().encode('ascii')

        req_expectation.get(self.aeso_client.LATEST_REPORT_URL, content=csv_content)

        load_ts = self.aeso_client.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('load_MW', None), 10270)

    def test_datetime_from_actual_forecast_date_column_hour_ending_same_day_standard_time(self):
        expected_dt = dateutil.parser.parse('2016-11-08T01:00:00.000-07:00')
        date_col_str = '11/08/2016 01'

        row_dt = self.aeso_client._datetime_from_actual_forecast_date_column(date_col=date_col_str)

        time_diff = expected_dt - row_dt
        self.assertEqual(time_diff, timedelta(microseconds=0))

    def test_datetime_from_actual_forecast_date_column_hour_ending_24_prior_day_standard_time(self):
        expected_dt = dateutil.parser.parse('2016-11-08T00:00:00.000-07:00')
        date_col_str = '11/07/2016 24'

        row_dt = self.aeso_client._datetime_from_actual_forecast_date_column(date_col=date_col_str)

        time_diff = expected_dt - row_dt
        self.assertEqual(time_diff, timedelta(microseconds=0))

    def test_datetime_from_actual_forecast_date_column_hour_ending_same_day_dst(self):
        expected_dt = dateutil.parser.parse('2016-11-05T01:00:00.000-06:00')
        date_col_str = '11/05/2016 01'

        row_dt = self.aeso_client._datetime_from_actual_forecast_date_column(date_col=date_col_str)

        time_diff = expected_dt - row_dt
        self.assertEqual(time_diff, timedelta(microseconds=0))

    def test_datetime_from_actual_forecast_date_column_hour_ending_24_prior_day_dst(self):
        expected_dt = dateutil.parser.parse('2016-11-05T00:00:00.000-06:00')
        date_col_str = '11/04/2016 24'

        row_dt = self.aeso_client._datetime_from_actual_forecast_date_column(date_col=date_col_str)

        time_diff = expected_dt - row_dt
        self.assertEqual(time_diff, timedelta(microseconds=0))

    def test_datetime_from_actual_forecast_date_column_hour_before_change_into_standard_time(self):
        # See example at:
        # http://ets.aeso.ca/ets_web/ip/Market/Reports/ActualForecastWMRQHReportServlet?contentType=csv&beginDate=11062016&endDate=11072016
        expected_dt = dateutil.parser.parse('2016-11-06T02:00:00.000-06:00')
        date_col_str = '11/06/2016 02'

        row_dt = self.aeso_client._datetime_from_actual_forecast_date_column(date_col=date_col_str)

        time_diff = expected_dt - row_dt
        self.assertEqual(time_diff, timedelta(microseconds=0))

    def test_datetime_from_actual_forecast_date_column_hour_of_change_into_standard_time(self):
        # See example at:
        # http://ets.aeso.ca/ets_web/ip/Market/Reports/ActualForecastWMRQHReportServlet?contentType=csv&beginDate=11062016&endDate=11072016
        expected_dt = dateutil.parser.parse('2016-11-06T02:00:00.000-07:00')
        date_col_str = '11/06/2016 02*'

        row_dt = self.aeso_client._datetime_from_actual_forecast_date_column(date_col=date_col_str)

        time_diff = expected_dt - row_dt
        self.assertEqual(time_diff, timedelta(microseconds=0))

    def test_datetime_from_actual_forecast_date_column_hour_before_change_into_dst(self):
        # See example at:
        # http://ets.aeso.ca/ets_web/ip/Market/Reports/ActualForecastWMRQHReportServlet?contentType=csv&beginDate=03122017&endDate=03132017
        expected_dt = dateutil.parser.parse('2017-03-12T01:00:00.000-07:00')
        date_col_str = '03/12/2017 01'

        row_dt = self.aeso_client._datetime_from_actual_forecast_date_column(date_col=date_col_str)

        time_diff = expected_dt - row_dt
        self.assertEqual(time_diff, timedelta(microseconds=0))

    def test_datetime_from_actual_forecast_date_column_hour_of_change_into_dst(self):
        # See example at:
        # http://ets.aeso.ca/ets_web/ip/Market/Reports/ActualForecastWMRQHReportServlet?contentType=csv&beginDate=03122017&endDate=03132017
        expected_dt = dateutil.parser.parse('2017-03-12T03:00:00.000-06:00')
        date_col_str = '03/12/2017 03'

        row_dt = self.aeso_client._datetime_from_actual_forecast_date_column(date_col=date_col_str)

        time_diff = expected_dt - row_dt
        self.assertEqual(time_diff, timedelta(microseconds=0))

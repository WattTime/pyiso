from pyiso import client_factory
from unittest import TestCase
import pandas as pd
from datetime import datetime, timedelta
import pytz
import unittest


class TestPJM(TestCase):
    def setUp(self):
        self.edata_inst_load = """
             <h1>Instantaneous Load</h1>
    <p>As of <span id="ctl00_ContentPlaceHolder1_DateAndTime">12.11.2015 17:15</span> EDT <a href="javascript:location.reload();">Refresh</a></p>
            <table class="edata-table stripped">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th class="right">MW</th>
                    </tr>
                </thead>
            <tbody>
            <tr>
                <td>PJM RTO Total</td>
                <td class="right">91,419</td>
            </tr>
            <tr>
                <td>Mid-Atlantic Region</td>
                <td class="right">33,280</td>
            </tr>
            <tr>
                <td>AE Zone</td>
                <td class="right">1,212</td>
            </tr>
                </tbody>
            </table>
            """

        self.edata_forecast_load = """
<h1>Forecasted Load History</h1>
<p>As of <span id="ctl00_ContentPlaceHolder1_DateAndTime">12.11.2015 17:24</span> EDT <a href="javascript:location.reload();">Refresh</a></p>
<dl class="no-margin">
    <dt>Name: </dt>
    <dd>PJM RTO Total</dd>
</dl>
<div class="table-tabs">
    <ul>
        <li id="tableView" class="selected"><a href="javascript:void(0);">Table View</a></li>
        <li id="chartView"><a href="javascript:void(0);">Chart View</a></li>
    </ul>
    <div id="table-view">
        <table class="edata-table stripped">
            <thead>
                <th>Time Stamp</th>
                <th class="right">MW</th>
            </thead>
            <tbody>
                <tr id="ctl00_ContentPlaceHolder1_lvForecastedLoadHistoryTable_ctrl0_trRow1">
                    <td>12.11.2015 17:00 EST</td>
                    <td class="right">92,725</td>
                </tr>
                <tr id="ctl00_ContentPlaceHolder1_lvForecastedLoadHistoryTable_ctrl1_trRow1">
                    <td>12.11.2015 18:00 EST</td>
                    <td class="right">93,358</td>
                </tr>
                <tr id="ctl00_ContentPlaceHolder1_lvForecastedLoadHistoryTable_ctrl2_trRow1">
                    <td>12.11.2015 19:00 EST</td>
                    <td class="right">91,496</td>
                </tr>
            </tbody>
        </table>
    </div>
</div>
"""

        self.c = client_factory('PJM')

    def test_utcify_pjmlike_edt(self):
        ts_str = '04/13/14 21:45 EDT'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 4)
        self.assertEqual(ts.day, 13+1)
        self.assertEqual(ts.hour, 21-20)
        self.assertEqual(ts.minute, 45)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_utcify_pjmlike_est(self):
        ts_str = '11/13/14 21:45 EST'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 11)
        self.assertEqual(ts.day, 13+1)
        self.assertEqual(ts.hour, 21-19)
        self.assertEqual(ts.minute, 45)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_time_as_of(self):
        ts = self.c.time_as_of(self.edata_inst_load)
        self.assertEqual(ts, datetime(2015, 12, 11, 17, 15, tzinfo=pytz.utc) + timedelta(hours=5))

    def test_parse_inst_load(self):
        dfs = pd.read_html(self.edata_inst_load, header=0, index_col=0)
        df = dfs[0]
        self.assertEqual(df.columns, 'MW')
        self.assertEqual(df.loc['PJM RTO Total']['MW'], 91419)

    @unittest.skip('No longer using self.utcify_index')
    def test_parse_forecast_load(self):
        dfs = pd.read_html(self.edata_forecast_load, header=0, index_col=0, parse_dates=True)
        # pandas date parser recognizes the timezone (EST), but returns a naive datetime
        # in UTC, why?
        df = self.c.utcify_index(dfs[0])
        self.assertEqual(df.columns, 'MW')
        self.assertEqual(df.shape, (3, 1))

        # times
        # first is 12.11.2015 17:00 EST
        self.assertEqual(df.index[0], pytz.utc.localize(datetime(2015, 12, 11, 22, 00)))
        # last is 12.11.2015 19:00 EST
        self.assertEqual(df.index[-1], pytz.utc.localize(datetime(2015, 12, 12, 0, 00)))

    def test_fetch_edata_series_timezone(self):
        data = self.c.fetch_edata_series('ForecastedLoadHistory', {'name': 'PJM RTO Total'})

        # check that latest forecast is within 1 hour, 1 minute of now
        td = data.index[0] - pytz.utc.localize(datetime.utcnow())
        self.assertLessEqual(td, timedelta(hours=1, minutes=1))

    def test_missing_time_is_none(self):
        ts = self.c.time_as_of('')
        self.assertIsNone(ts)

    def test_bad_url(self):
        ts, val = self.c.fetch_edata_point('badtype', 'badkey', 'badheader')
        self.assertIsNone(ts)
        self.assertIsNone(val)

    def test_get_lmp_datasnapshot(self):
        start_at = pytz.timezone('US/Eastern').localize(datetime(2015, 1, 1)
                                                       ).astimezone(pytz.utc)
        end_at = start_at + timedelta(days=1)

        # node 33092371 is COMED
        data = self.c.get_lmp(start_at=start_at, end_at=end_at, node_id='COMED')
        timestamps = [d['timestamp'] for d in data]

        self.assertLessEqual(min(timestamps), start_at)
        self.assertGreaterEqual(max(timestamps), end_at)

    def test_get_lmp_oasis(self):
        now = datetime.now(pytz.utc)
        data = self.c.get_lmp(node_id=33092371, market='RT5M')

        timestamps = [d['timestamp'] for d in data]

        # no historical data
        self.assertEqual(len(set(timestamps)), 1)
        self.assertLessEqual(abs((timestamps[0] - now).total_seconds()), 60*10)

    def test_fetch_historical_load(self):
        df = self.c.fetch_historical_load(2015)
        self.assertEqual(df['load_MW'][0], 94001.713000000003)
        self.assertEqual(df['load_MW'][-1], 79160.809999999998)
        self.assertEqual(len(df), 365*24 - 2)

        est = pytz.timezone(self.c.TZ_NAME)
        start_at = est.localize(datetime(2015, 1, 1))
        end_at = est.localize(datetime(2015, 12, 31, 23, 0))

        self.assertEqual(df.index[0], start_at)
        self.assertEqual(df.index[-1], end_at)

        # Manually checked form xls file
        # These tests should fail if timezones are improperly handled
        tz_func = lambda x: pd.tslib.Timestamp(pytz.utc.normalize(est.localize(x)))
        self.assertEqual(df.ix[tz_func(datetime(2015, 2, 2, 14))]['load_MW'], 105714.638)
        self.assertEqual(df.ix[tz_func(datetime(2015, 6, 4, 2))]['load_MW'], 64705.985)
        self.assertEqual(df.ix[tz_func(datetime(2015, 12, 15, 23))]['load_MW'], 79345.672)


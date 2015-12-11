from pyiso import client_factory
from unittest import TestCase
import pandas as pd
from datetime import datetime, timedelta
import pytz


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

    def test_parse_forecast_load(self):
        dfs = pd.read_html(self.edata_forecast_load, header=0, index_col=0, parse_dates=True)
        df = self.c.utcify_index(dfs[0])
        self.assertEqual(df.columns, 'MW')
        self.assertEqual(df.shape, (3, 1))

        # times
        # first is 12.11.2015 17:00 EST
        self.assertEqual(df.index[0], pytz.utc.localize(datetime(2015, 12, 11, 22, 00)))
        # last is 12.11.2015 19:00 EST
        self.assertEqual(df.index[-1], pytz.utc.localize(datetime(2015, 12, 12, 0, 00)))

    def test_missing_time_is_none(self):
        ts = self.c.time_as_of('')
        self.assertIsNone(ts)

    def test_bad_url(self):
        ts, val = self.c.fetch_edata_point('badtype', 'badkey', 'badheader')
        self.assertIsNone(ts)
        self.assertIsNone(val)

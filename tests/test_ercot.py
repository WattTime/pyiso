from pyiso import client_factory
from unittest import TestCase
import pytz
from datetime import datetime, timedelta
import pandas as pd


class TestERCOT(TestCase):
    def setUp(self):
        self.c = client_factory('ERCOT')

        self.rtm_html = """
 <html>
<head>
<script>
  setTimeout("window.location.reload(true);",60000);
</script>
<title>Real-Time System Conditions</title>
<meta http-equiv='pragma'  content='no-cache'>
<link rel="stylesheet" type="text/css" href="/content/styles/cdr_reports.css" />
<script>
function open_window(url) {
  window.open(url,"help",'toolbar=0,location=0,directories=0,status=0,menubar=0,scrollbars=yes,resizable=0,width=750,height=825');
};
</script>
<!-- Google Analytics -->
<script>
(function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
(i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
})(window,document,'script','//www.google-analytics.com/analytics.js','ga');

ga('create', 'UA-460876-10', 'auto');
ga('send', 'pageview');
</script>
<!-- End Google Analytics -->
</head>
<body class="bodyStyle">
<div style="display: inline-block;">
<div>
  <div class='header1'>Real-Time System Conditions</div>
  <div class='helpLink'><a href="javascript:open_window('/help/popup/rtSysCondHelp')">Help?</a></div>
</div>
<br>
<div class="schedTime rightAlign">Last Updated: Apr 14, 2016 18:38:40</div>
<table class="tableStyle">
<tbody>
  <tr>
    <td class="headerValueClass" colSpan="2">Frequency</td>
  </tr>
    <td class="tdLeft">Current Frequency</td>
    <td class="labelClassCenter">59.998</td>
  </tr>
  </tr>
    <td class="tdLeft">Instantaneous Time Error</td>
    <td class="labelClassCenter">-17.468</td>
  </tr>
  <tr>
    <td class="headerValueClass" colSpan="2">Real-Time Data</td>
  </tr>
  </tr>
    <td class="tdLeft">Actual System Demand</td>
    <td class="labelClassCenter">38850</td>
  </tr>
  </tr>
    <td class="tdLeft">Total System Capacity (not including Ancillary Services)</td>
    <td class="labelClassCenter">42514</td>
  </tr>
  </tr>
    <td class="tdLeft">Total Wind Output</td>
    <td class="labelClassCenter">5242</td>
  </tr>
  <tr>
    <td class="headerValueClass" colSpan="2">DC Tie Flows</td>
  </tr>
    </tr>
    <td class="tdLeft">DC_E (East)</td>
    <td class="labelClassCenter">-31</td>
  </tr>
    </tr>
    <td class="tdLeft">DC_L (Laredo VFT)</td>
    <td class="labelClassCenter">0</td>
  </tr>
    </tr>
    <td class="tdLeft">DC_N (North)</td>
    <td class="labelClassCenter">0</td>
  </tr>
    </tr>
    <td class="tdLeft">DC_R (Railroad)</td>
    <td class="labelClassCenter">0</td>
  </tr>
    </tr>
    <td class="tdLeft">DC_S (Eagle Pass)</td>
    <td class="labelClassCenter">1</td>
  </tr>
  </tbody>
</table>
</div>
</body>
</html>
"""

    def test_utcify(self):
        ts_str = '05/03/2014 02:00'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 5)
        self.assertEqual(ts.day, 3)
        self.assertEqual(ts.hour, 2+5)
        self.assertEqual(ts.minute, 0)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_parse_load(self):
        self.c.handle_options(data='load', latest=True)
        data = self.c.parse_rtm(self.rtm_html)
        self.assertEqual(len(data), 1)
        expected_keys = ['timestamp', 'ba_name', 'load_MW', 'freq', 'market']
        self.assertEqual(sorted(data[0].keys()), sorted(expected_keys))
        self.assertEqual(data[0]['timestamp'], pytz.utc.localize(datetime(2016, 4, 14, 23, 38, 40)))
        self.assertEqual(data[0]['load_MW'], 38850.0)

    def test_parse_genmix(self):
        self.c.handle_options(data='gen', latest=True)
        data = self.c.parse_rtm(self.rtm_html)

        self.assertEqual(len(data), 2)
        expected_keys = ['timestamp', 'ba_name', 'gen_MW', 'fuel_name', 'freq', 'market']
        self.assertEqual(sorted(data[0].keys()), sorted(expected_keys))

        self.assertEqual(data[0]['timestamp'], pytz.utc.localize(datetime(2016, 4, 14, 23, 38, 40)))
        self.assertEqual(data[0]['gen_MW'], 5242.0)
        self.assertEqual(data[0]['fuel_name'], 'wind')

        self.assertEqual(data[1]['timestamp'], pytz.utc.localize(datetime(2016, 4, 14, 23, 38, 40)))
        self.assertEqual(data[1]['gen_MW'], 38850 - 5242 + 31 - 1)
        self.assertEqual(data[1]['fuel_name'], 'nonwind')

    def test_request_report_gen_hrly(self):
        # get data as list of dicts
        df = self.c._request_report('gen_hrly')

        # test for expected data
        self.assertEqual(len(df), 1)
        for key in ['SE_EXE_TIME_DST', 'SE_EXE_TIME', 'SE_MW']:
            self.assertIn(key, df.columns)

    def test_request_report_wind_hrly(self):
        # get data as list of dicts
        df = self.c._request_report('wind_hrly')

        # test for expected data
        self.assertLessEqual(len(df), 96)
        for key in ['DSTFlag', 'ACTUAL_SYSTEM_WIDE', 'HOUR_BEGINNING']:
            self.assertIn(key, df.columns)

    def test_request_report_load_7day(self):
        # get data as list of dicts
        df = self.c._request_report('load_7day')

        # test for expected data
        # subtract 1 hour for DST
        self.assertGreaterEqual(len(df), 8*24-1)
        for key in ['SystemTotal', 'HourEnding', 'DSTFlag', 'DeliveryDate']:
            self.assertIn(key, df.columns)

    def test_request_report_dam_hrl_lmp(self):
        now = datetime.now(pytz.timezone(self.c.TZ_NAME))
        df = self.c._request_report('dam_hrly_lmp', now)
        s = pd.to_datetime(df['DeliveryDate'], utc=True)

        self.assertEqual(s.min().date(), now.date())
        self.assertEqual(s.max().date(), now.date())

        node_counts = [612, 613, 614, 615, 616]
        self.assertIn(len(df), [n*24 for n in node_counts])  # nodes * 24 hrs/day

    def test_request_report_rt5m_lmp(self):
        now = datetime.now(pytz.utc) - timedelta(minutes=5)
        df = self.c._request_report('rt5m_lmp', now)
        df.index = df['SCEDTimestamp']
        s = pd.to_datetime(df.index).tz_localize('Etc/GMT+5').tz_convert('utc')

        self.assertLess((s.min() - now).total_seconds(), 8*60)
        self.assertLess((s.max() - now).total_seconds(), 8*60)

        node_counts = [612, 613, 614, 615, 616]
        self.assertIn(len(df), node_counts)

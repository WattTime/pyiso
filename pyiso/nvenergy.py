from pyiso.base import BaseClient
import pandas as pd
from datetime import time, datetime, timedelta
try:
    from urllib2 import HTTPError
except ImportError:
    from urllib.error import HTTPError
import logging
import pytz
import calendar


logger = logging.getLogger(__name__)


class NVEnergyClient(BaseClient):
    BASE_URL = 'http://www.oasis.oati.com/NEVP/NEVPdocs/inetloading/'
    TZ_NAME = 'America/Los_Angeles'
    NAME = 'NVEnergy'

    def get_load(self, latest=False,
                 start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # set up storage
        parsed_data = []

        # collect data
        for this_date in self.dates():
            # fetch
            try:
                df, mode = self.fetch_df(this_date)
            except (HTTPError, ValueError):
                logger.warn('No data available in NVEnergy at %s' % this_date)
                continue

            # store
            try:
                parsed_data += self.parse_df(df, this_date, mode)
            except KeyError:
                logger.warn('Unparseable data available in NVEnergy at %s: %s' % (this_date, df))
                continue

        # return
        return self.time_subset(parsed_data)

    def data_url(self, ts):
        # today's date in local time
        today = pytz.timezone(self.TZ_NAME).localize(datetime.now()).date()
        tomorrow = today + timedelta(days=1)
        try:
            this_day = ts.date()
        except AttributeError:  # already date object not datetime
            this_day = ts

        # file is tomorrow, this month, or past
        if this_day > tomorrow:
            raise ValueError('No data available in NVEnergy at %s' % ts)
        if this_day == tomorrow:
            url_file = ts.strftime('tomorrow.htm')
            mode = 'tomorrow'
        elif ts.month == today.month and ts.year == today.year:
            url_file = ts.strftime('native_system_load_and_ties_for_%m_%d_%Y_.html')
            mode = 'recent'
        else:
            dummy, month_length = calendar.monthrange(ts.year, ts.month)
            url_file = 'Monthly_Ties_and_Loads_L_from_%02d_%02d_%04d_to_%02d_%02d_%04d_.html' % (
                ts.month, 1, ts.year,
                ts.month, month_length, ts.year
            )
            mode = 'historical'

        # return
        return self.BASE_URL + url_file, mode

    def fetch_df(self, this_date, url=None, mode=None):
        # set up request
        if not url:
            url, mode = self.data_url(this_date)

        # carry out request and parse html tables
        dfs = pd.read_html(url, index_col=0)

        # choose df based on mode
        if mode == 'recent':
            df = dfs[1]
        elif mode == 'tomorrow':
            df = dfs[0]
        else:  # historical
            # set up date string
            try:
                datestr = pytz.timezone(self.TZ_NAME).localize(this_date).strftime('%Y-%m-%d')
            except AttributeError:  # already date not datetime, assume local
                datestr = this_date.strftime('%Y-%m-%d')

            # choose based on date string
            df = None
            for xdf in dfs[1:]:
                if xdf.index[0] == datestr:
                    df = xdf
                    break

            # if no df, no data in range
            if df is None:
                raise ValueError('No data available in NVEnergy at %s' % this_date)

        # set and slice header
        df.columns = df.iloc[1]
        df = df[2:]

        # return
        return df, mode

    def parse_df(self, df, this_date, mode='recent'):
        # set up storage
        data = []

        # pull out actual or forecast data
        if self.options['forecast'] or mode == 'tomorrow':
            series = df.loc['Forecast System Load']
        else:
            series = df.loc['Actual System Load']

        # store
        for shour, value in series.iteritems():
            # skip if no load data (in future)
            if not value:
                continue

            # create local time
            try:
                ts = self.idx2ts(this_date, shour)
            except ValueError:
                continue

            # set up datapoint
            dp = {
                'timestamp': ts,
                'load_MW': value,
                'ba_name': self.NAME,
                'market': self.MARKET_CHOICES.hourly,
                'freq': self.FREQUENCY_CHOICES.hourly,
            }

            # add to storage
            data.append(dp)

        # return
        return data

    def time_subset(self, data):
        # if sliceable, return inclusive of dates
        if self.options['sliceable']:
            f = lambda x: x['timestamp'] >= self.options['start_at'] and x['timestamp'] <= self.options['end_at']
            return filter(f, data)

        # if latest, only return most recent
        elif self.options['latest']:
            latest = sorted(data, key=lambda x: x['timestamp'])[-1]
            return [latest]

        # if neither, return all data
        else:
            return data

    def idx2ts(self, this_date, shour):
        """
        Takes a date object and a local hour string between '01' and '24',
        and returns a UTC datetime object.
        Raises ValueError if shour is not actually an hour string.
        """
        ihour = int(shour) - 1
        local_time = datetime.combine(this_date, time(hour=ihour))
        return self.utcify(local_time)

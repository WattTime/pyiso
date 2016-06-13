from pyiso.base import BaseClient
from pyiso import LOGGER
import pandas as pd
import numpy as np
from datetime import time, datetime, timedelta
try:
    from urllib2 import HTTPError
except ImportError:
    from urllib.error import HTTPError
import pytz
import calendar


class NVEnergyClient(BaseClient):
    BASE_URL = 'http://www.oasis.oati.com/NEVP/NEVPdocs/inetloading/'
    TZ_NAME = 'America/Los_Angeles'
    NAME = 'NVEnergy'
    TRADE_BAS = {
        'BPA': 'BPA',
        'CAISO': 'CAISO',
        'LADWP': 'LDWP',
        'IPCO': 'IPCO',
        'PACE': 'PACE',
        'NVE': 'NEVP',
        'WALC': 'WALC',
    }

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
                LOGGER.warn('No data available in NVEnergy at %s' % this_date)
                continue

            # store
            try:
                parsed_data += self.parse_load(df, this_date, mode)
            except KeyError:
                LOGGER.warn('Unparseable data available in NVEnergy at %s for mode %s: %s' % (this_date, mode, df))
                continue

        # return
        return self.time_subset(parsed_data)

    def get_trade(self, latest=False,
                  start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='trade', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # set up storage
        parsed_data = []

        # collect data
        for this_date in self.dates():
            # fetch
            try:
                df, mode = self.fetch_df(this_date)
            except (HTTPError, ValueError):
                LOGGER.warn('No data available in NVEnergy at %s' % this_date)
                continue

            # store
            try:
                parsed_data += self.parse_trade(df, this_date, mode)
            except KeyError:
                LOGGER.warn('Unparseable data available in NVEnergy at %s: %s' % (this_date, df))
                continue

        # return
        return self.time_subset(parsed_data)

    def data_url(self, ts, mode=None):
        # today's date in local time
        today = pytz.timezone(self.TZ_NAME).localize(datetime.utcnow()).date()
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
        elif mode == 'alternate':
            url_file = ts.strftime('native_system_load_and_ties_Y_for_%m_%d_%Y_.html')
            mode = 'recent'
        elif (ts.month == today.month and ts.year == today.year) or (today-this_day).days < 2:
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
            url, mode = self.data_url(this_date, mode=mode)

        # carry out request
        response = self.request(url)
        if not response:
            return pd.DataFrame(), 'error'

        # parse html tables
        dfs = pd.read_html(response.content, index_col=0)

        # choose df based on mode
        if mode == 'recent':
            try:
                df = dfs[1]
            except IndexError:  # try alternate
                return self.fetch_df(this_date, mode='alternate')
        elif mode == 'tomorrow':
            df = dfs[0]
        else:  # historical
            # set up date string
            try:
                datestr = pytz.timezone(self.TZ_NAME).localize(this_date).strftime('%Y-%m-%d')
            except AttributeError:  # already date not datetime, assume local
                datestr = this_date.strftime('%Y-%m-%d')

            # pull one day of data out of full df
            full_df = dfs[1]
            date_row_idx = np.where(full_df.index == datestr)[0][0]
            df = full_df.iloc[date_row_idx:date_row_idx+13]

        # set and slice header
        df.columns = df.iloc[1]
        df = df[2:]

        # return
        return df, mode

    def parse_load(self, df, this_date, mode='recent'):
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

    def parse_trade(self, df, this_date, mode='recent'):
        # set up storage
        data = []

        # set index as counterparty bas
        df.index = df['Counterparty']

        # store for all counterparty bas
        for iso in self.TRADE_BAS:
            # pull out data
            series = df.loc[iso]

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
                # negative exports = imports
                dp = {
                    'timestamp': ts,
                    'export_MW': value,
                    'dest_ba_name': self.TRADE_BAS[iso],
                    'source_ba_name': self.NAME,
                    'market': self.MARKET_CHOICES.hourly,
                    'freq': self.FREQUENCY_CHOICES.hourly,
                }

                # add to storage
                data.append(dp)

        # return
        return data

    def time_subset(self, data):
        # if no data, empty list
        if len(data) == 0:
            return []

        # if sliceable, return inclusive of dates
        elif self.options['sliceable']:
            f = lambda x: x['timestamp'] >= self.options['start_at'] and x['timestamp'] <= self.options['end_at']
            filtered = filter(f, data)
            return list(filtered)

        # if latest, only return most recent
        elif self.options['latest']:
            latest_ts = max([x['timestamp'] for x in data])
            latest = filter(lambda x: x['timestamp'] == latest_ts, data)
            return list(latest)

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

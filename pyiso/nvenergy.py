from pyiso.base import BaseClient
import pandas as pd
from datetime import time, datetime
try:
    from urllib2 import HTTPError
except ImportError:
    from urllib import HTTPError
import logging


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
            # set up request
            url_file = this_date.strftime('native_system_load_and_ties_for_%m_%d_%Y_.html')
            url = self.BASE_URL + url_file

            # fetch
            try:
                df = self.fetch_df(url)
            except HTTPError:
                logger.warn('No data available in NVEnergy at %s' % this_date)
                continue

            # store
            parsed_data += self.parse_df(df, this_date)

        # if latest, only return most recent
        if self.options['latest']:
            parsed_data = [parsed_data[-1]]

        # return
        return parsed_data

    def fetch_df(sel, url):
        # carry out request and parse html table
        dfs = pd.read_html(url, index_col=0, header=1)
        df = dfs[1]
        return df

    def parse_df(self, df, this_date):
        # set up storage
        data = []

        # pull out actual or forecast data
        if self.options['forecast']:
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
            to_store = True
            if self.options['sliceable']:
                if dp['timestamp'] < self.options['start_at'] or dp['timestamp'] > self.options['end_at']:
                    to_store = False
            if to_store:
                data.append(dp)

        # return
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

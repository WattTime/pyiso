from pyiso.base import BaseClient
import pandas as pd
from datetime import time, datetime
from urllib2 import HTTPError
import logging


logger = logging.getLogger(__name__)


class NVENERGYClient(BaseClient):
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

            # carry out request and parse html table
            try:
                dfs = pd.read_html(url, index_col=0, header=1)
            except HTTPError:
                logger.warn('No data available in NVEnergy at %s' % this_date)
                continue
            df = dfs[1]

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
                    ihour = int(shour) - 1
                    local_time = datetime.combine(this_date, time(hour=ihour))
                except ValueError:
                    continue

                # set up datapoint
                dp = {
                    'timestamp': self.utcify(local_time),
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
                    parsed_data.append(dp)

        # if latest, only return most recent
        if self.options['latest']:
            parsed_data = [parsed_data[-1]]

        # return
        return parsed_data

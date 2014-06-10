import copy
from pyiso.base import BaseClient
import pytz


class MISOClient(BaseClient):
    NAME = 'MISO'
    
    base_url = 'https://www.misoenergy.org/ria/'
    
    fuels = {
        'Coal': 'coal',
        'Natural Gas': 'natgas',
        'Nuclear': 'nuclear',
        'Other': 'other',
        'Wind': 'wind',
    }

    TZ_NAME = 'America/New_York'
                
    def utcify(self, local_ts, **kwargs):
        # MISO is always on Eastern Standard Time, even during DST
        # ie UTC offset = -5 always
        utc_ts = super(MISOClient, self).utcify(local_ts, is_dst=False)
        utc_ts += utc_ts.astimezone(pytz.timezone(self.TZ_NAME)).dst() # adjust for EST
        return utc_ts

    def get_generation(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, **kwargs)

        # process args
        request_urls = []
        if latest:
            request_urls.append('FuelMix.aspx?CSV=True')

        else:
            raise ValueError('Latest must be True.')
            
        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        for request_url in request_urls:
            # set up request
            url = copy.deepcopy(self.base_url)
            url += request_url
            
            # carry out request
            response = self.request(url)
            if not response:
                return parsed_data
            
            # test for valid content
            if 'The page cannot be displayed' in response.text:
                self.logger.error('MISO: Error in source data for generation')
                return parsed_data
            
            # preliminary parsing
            rows = response.text.split('\n')
            header = self.parse_row(rows[0])
            for row in rows[1:]:
                raw_data.append(dict(zip(header, self.parse_row(row))))
            
        # parse data
        for raw_dp in raw_data:
            # process timestamp
            aware_utc_timestamp = self.utcify(raw_dp['INTERVALEST'])

            # set up storage
            parsed_dp = {}
            
            # add values
            try:
                parsed_dp['timestamp'] = aware_utc_timestamp
                parsed_dp['gen_MW'] = float(raw_dp['ACT'])
                parsed_dp['fuel_name'] = self.fuels[raw_dp['CATEGORY']]
                parsed_dp['ba_name'] = self.NAME
                parsed_dp['market'] = self.MARKET_CHOICES.fivemin
                parsed_dp['freq'] = self.FREQUENCY_CHOICES.fivemin
            except KeyError: # blank last line
                continue
            
            # add to full storage
            parsed_data.append(parsed_dp)
            
        return parsed_data

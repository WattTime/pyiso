from datetime import timedelta
from pyiso.base import BaseClient
from os import environ


class ISONEClient(BaseClient):
    NAME = 'ISONE'
    
    base_url = 'https://webservices.iso-ne.com/api/v1.1'
    TZ_NAME = 'America/New_York'
    
    fuels = {
        'Coal': 'coal',
        'Hydro': 'hydro',
        'Natural Gas': 'natgas',
        'Nuclear': 'nuclear',
        'Oil': 'oil',
        'Solar': 'solar',
        'Wind': 'wind',
        'Wood': 'biomass',
        'Refuse': 'refuse',
        'Landfill Gas': 'biogas',
    }

    def __init__(self, *args, **kwargs):
        super(ISONEClient, self).__init__(*args, **kwargs)
        try:
            self.auth = (environ['ISONE_USERNAME'], environ['ISONE_PASSWORD'])
        except KeyError:
            self.auth = ()

    def fetch_data(self, endpoint, auth):
        url = self.base_url + endpoint
        response = self.request(url, auth=auth)
        return response.json()

    def get_generation(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # process args
        request_urls = []
        if latest:
            request_urls.append('current')

        elif start_at and end_at:
            for date in self.dates():
                request_urls.append(date.strftime('day/%Y%m%d'))
        else:
            raise ValueError('Either latest must be True, or start_at and end_at must both be provided.')
            
        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        for request_url in request_urls:
            # set up request
            endpoint = '/genfuelmix/%s.json' % (request_url)
            
            # carry out request
            data = self.fetch_data(endpoint, self.auth)
            raw_data += data['GenFuelMixes']['GenFuelMix']

        # parse data
        for raw_dp in raw_data:
            # set up storage
            parsed_dp = {}
            
            # add values
            parsed_dp['timestamp'] = self.utcify(raw_dp['BeginDate'])
            parsed_dp['gen_MW'] = raw_dp['GenMw']
            parsed_dp['fuel_name'] = self.fuels[raw_dp['FuelCategory']]
            parsed_dp['ba_name'] = self.NAME
            parsed_dp['market'] = self.MARKET_CHOICES.na
            parsed_dp['freq'] = self.FREQUENCY_CHOICES.na
            
            # add to full storage
            parsed_data.append(parsed_dp)
 
        return parsed_data

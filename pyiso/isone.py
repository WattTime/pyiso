from pyiso.base import BaseClient
from pyiso import LOGGER
from os import environ
from datetime import datetime
import pytz
import pandas as pd


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

    locations = {
        'INTERNALHUB': 4000,
        'MAINE': 4001,
        'NEWHAMPSHIRE': 4002,
        'VERMONT': 4003,
        'CONNECTICUT': 4004,
        'RHODEISLAND': 4005,
        'SEMASS': 4006,
        'WCMASS': 4007,
        'NEMASSBOST': 4008,
    }

    def __init__(self, *args, **kwargs):
        super(ISONEClient, self).__init__(*args, **kwargs)
        try:
            self.auth = (environ['ISONE_USERNAME'], environ['ISONE_PASSWORD'])
        except KeyError:
            msg = 'Must define environment variables ISONE_USERNAME and ISONE_PASSWORD to use ISONE client.'
            raise RuntimeError(msg)

    def get_generation(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        for endpoint in self.request_endpoints():
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
            parsed_dp['market'] = self.options['market']
            parsed_dp['freq'] = self.options['frequency']

            # add to full storage
            parsed_data.append(parsed_dp)

        return parsed_data

    def get_load(self, latest=False, start_at=False, end_at=False,
                 forecast=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest, forecast=forecast,
                            start_at=start_at, end_at=end_at, **kwargs)

        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        for endpoint in self.request_endpoints():
            # carry out request
            data = self.fetch_data(endpoint, self.auth)

            # pull out data
            try:
                raw_data += self.parse_json_load_data(data)
            except ValueError as e:
                LOGGER.warn(e)
                continue

        # parse data
        now = pytz.utc.localize(datetime.utcnow())
        for raw_dp in raw_data:
            # set up storage
            parsed_dp = {}

            # add values
            parsed_dp['timestamp'] = self.utcify(raw_dp['BeginDate'])
            parsed_dp['load_MW'] = raw_dp['LoadMw']
            parsed_dp['ba_name'] = self.NAME
            parsed_dp['market'] = self.options['market']
            parsed_dp['freq'] = self.options['frequency']

            # add to full storage
            if self.options['forecast'] and parsed_dp['timestamp'] < now:
                # don't include past forecast data
                pass
            else:
                parsed_data.append(parsed_dp)

        return parsed_data

    def handle_options(self, **kwargs):
        # default options
        super(ISONEClient, self).handle_options(**kwargs)

        # handle market
        if not self.options.get('market'):
            if self.options['data'] == 'gen':
                # generation on n/a market
                self.options['market'] = self.MARKET_CHOICES.na
            else:
                # load on real-time 5-min or hourly forecast
                if self.options['forecast']:
                    self.options['market'] = self.MARKET_CHOICES.dam
                else:
                    self.options['market'] = self.MARKET_CHOICES.fivemin

        # handle frequency
        if not self.options.get('frequency'):
            if self.options['data'] == 'gen':
                # generation on n/a frequency
                self.options['frequency'] = self.FREQUENCY_CHOICES.na
            else:
                # load on real-time 5-min or hourly forecast
                if self.options['market'] == self.MARKET_CHOICES.dam:
                    self.options['frequency'] = self.FREQUENCY_CHOICES.dam
                else:
                    self.options['frequency'] = self.FREQUENCY_CHOICES.fivemin

        if self.options['data'] == 'lmp':
            if self.options['market'] == self.MARKET_CHOICES.fivemin:
                if self.options['forecast']:
                    raise ValueError('ISONE does not produce forecast five minute lmps')

    def request_endpoints(self, location_id=None):
        """Returns a list of endpoints to query, based on handled options"""
        # base endpoint
        ext = ''
        if self.options['data'] == 'gen':
            base_endpoint = 'genfuelmix'
        elif self.options['data'] == 'lmp' and location_id is not None:
            ext = '/location/%s' % location_id
            if self.options['market'] == self.MARKET_CHOICES.fivemin:
                base_endpoint = 'fiveminutelmp'
            elif self.options['market'] == self.MARKET_CHOICES.dam:
                base_endpoint = 'hourlylmp/da/final'
            elif self.options['market'] == self.MARKET_CHOICES.hourly:
                base_endpoint = 'hourlylmp/rt/prelim'
        elif self.options['data'] == 'load':
            if self.options['market'] == self.MARKET_CHOICES.dam:
                base_endpoint = 'hourlyloadforecast'
            else:
                base_endpoint = 'fiveminutesystemload'
        else:
            raise ValueError('Data type not recognized %s' % self.options['data'])

        # set up storage
        request_endpoints = []

        # handle dates
        if self.options['latest']:
            request_endpoints.append('/%s/current%s.json' % (base_endpoint, ext))

        elif self.options['start_at'] and self.options['end_at']:
            for date in self.dates():
                date_str = date.strftime('%Y%m%d')
                request_endpoints.append('/%s/day/%s%s.json' % (base_endpoint, date_str, ext))

        else:
            msg = 'Either latest or forecast must be True, or start_at and end_at must both be provided.'
            raise ValueError(msg)

        # return
        return request_endpoints

    def fetch_data(self, endpoint, auth):
        url = self.base_url + endpoint
        response = self.request(url, auth=auth)
        if response:
            return response.json()
        else:
            return {}

    def parse_json_load_data(self, data):
        """
        Pull approriate keys from json data set.
        Raise ValueError if parser fails.
        """
        try:
            if self.options.get('latest'):
                return data['FiveMinSystemLoad']
            elif self.options['market'] == self.MARKET_CHOICES.dam:
                return data['HourlyLoadForecasts']['HourlyLoadForecast']
            else:
                return data['FiveMinSystemLoads']['FiveMinSystemLoad']
        except (KeyError, TypeError):
            raise ValueError('Could not parse ISONE load data %s' % data)

    def parse_json_lmp_data(self, data):
        """
        Pull approriate keys from json data set.
        Raise ValueError if parser fails.
        """
        try:
            if self.options['market'] == self.MARKET_CHOICES.fivemin:
                if self.options.get('latest'):
                    return data['FiveMinLmp']
                else:
                    return data['FiveMinLmps']['FiveMinLmp']
            else:
                return data['HourlyLmps']['HourlyLmp']
        except (KeyError, TypeError):
            raise ValueError('Could not parse ISONE lmp data %s' % data)

    def _parse_lmp(self, json):
        df = pd.DataFrame(json)
        # Get datetimes
        df.index = df['BeginDate']
        df.index = pd.to_datetime(df.index, utc=True)
        df['timestamp'] = df.index

        df.rename(columns={'LmpTotal': 'lmp'}, inplace=True)
        df['ba_name'] = self.NAME
        df['market'] = self.options['market']
        df['freq'] = self.options['frequency']
        df['node_id'] = self.options['node_id']
        df['lmp_type'] = 'energy'

        # drop unwanted columns
        df.drop([u'BeginDate', u'CongestionComponent', u'EnergyComponent',
                 u'Location', u'LossComponent', ],
                axis=1, inplace=True)

        return df

    def get_lmp(self, node_id='INTERNALHUB', latest=True, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='lmp', latest=latest,
                            start_at=start_at, end_at=end_at, node_id=node_id, **kwargs)
        # get location id
        try:
            locationid = self.locations[node_id.upper()]
        except KeyError:
            raise ValueError('No LMP data available for location %s' % node_id)

        # set up storage
        raw_data = []
        # collect raw data
        for endpoint in self.request_endpoints(locationid):
            # carry out request
            data = self.fetch_data(endpoint, self.auth)

            # pull out data
            try:
                raw_data += self.parse_json_lmp_data(data)
            except ValueError as e:
                LOGGER.warn(e)
                continue

        # parse and slice
        df = self._parse_lmp(raw_data)
        df = self.slice_times(df)

        # return
        return df.to_dict(orient='record')

from pyiso.base import BaseClient
from os import environ
from datetime import datetime, timedelta
from dateutil import parser
import pytz
import logging


logger = logging.getLogger(__name__)


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
                logger.warn(e)
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

    def request_endpoints(self):
        """Returns a list of endpoints to query, based on handled options"""
        # base endpoint
        if self.options['data'] == 'gen':
            base_endpoint = 'genfuelmix'
        elif self.options['market'] == self.MARKET_CHOICES.dam:
            base_endpoint = 'hourlyloadforecast'
        else:
            base_endpoint = 'fiveminutesystemload'

        # set up storage
        request_endpoints = []

        # handle dates
        if self.options['latest']:
            request_endpoints.append('/%s/current.json' % base_endpoint)

        elif self.options['start_at'] and self.options['end_at']:
            for date in self.dates():
                date_str = date.strftime('%Y%m%d')
                request_endpoints.append('/%s/day/%s.json' % (base_endpoint, date_str))

        else:
            msg = 'Either latest or forecast must be True, or start_at and end_at must both be provided.'
            raise ValueError(msg)

        # return
        return request_endpoints

    def fetch_data(self, endpoint, auth):
        url = self.base_url + endpoint
        response = self.request(url, auth=auth)
        return response.json()

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

    def get_lmp(self, zone_name, latest=True, start_at=False, end_at=False, **kwargs):

        # TODO, handle kwargs including latest, etc
        loc_dict = [a for a in self.locations if a['LocationName'] == '.Z.%s' % zone_name][0]
        locationid = loc_dict['LocationID']
        if latest:
            endpoint = '/fiveminutelmp/current/location/%s.json' % locationid
            price = self.fetch_data(endpoint, self.auth)['FiveMinLmp'][0]['LmpTotal']
            return price
        else:
            day = start_at.strftime('%Y%m%d')
            if not end_at:
                end_at = start_at + timedelta(days=1)

            endpoint = '/fiveminutelmp/day/%s/location/%s.json' % (day, locationid)
            response = self.fetch_data(endpoint, self.auth)

            lmp_dict = {}
            for item in response['FiveMinLmps']['FiveMinLmp']:
                # '%Y-%m-%dT%H:%M:%S.%f%z'
                current_time = parser.parse(item['BeginDate']).astimezone(pytz.utc)
                if start_at <= current_time <= end_at:
                    lmp_dict[current_time] = item['LmpTotal']
            return lmp_dict

    locations = [
        {u'LocationName': u'.H.INTERNAL_HUB', u'LocationType': u'HUB', u'AreaType':
            u'INTERNAL', u'LocationID': 4000},
        {u'LocationName': u'.Z.MAINE', u'LocationType': u'LOAD ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 4001},
        {u'LocationName': u'.Z.NEWHAMPSHIRE', u'LocationType': u'LOAD ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 4002},
        {u'LocationName': u'.Z.VERMONT', u'LocationType': u'LOAD ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 4003},
        {u'LocationName': u'.Z.CONNECTICUT', u'LocationType': u'LOAD ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 4004},
        {u'LocationName': u'.Z.RHODEISLAND', u'LocationType': u'LOAD ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 4005},
        {u'LocationName': u'.Z.SEMASS', u'LocationType': u'LOAD ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 4006},
        {u'LocationName': u'.Z.WCMASS', u'LocationType': u'LOAD ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 4007},
        {u'LocationName': u'.Z.NEMASSBOST', u'LocationType': u'LOAD ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 4008},
        {u'LocationName': u'.I.SALBRYNB345 1', u'LocationType': u'EXT. NODE', u'AreaType':
            u'EXTERNAL', u'LocationID': 4010},
        {u'LocationName': u'.I.ROSETON 345 1', u'LocationType': u'EXT. NODE', u'AreaType':
            u'EXTERNAL', u'LocationID': 4011},
        {u'LocationName': u'.I.HQ_P1_P2345 5', u'LocationType': u'EXT. NODE', u'AreaType':
            u'EXTERNAL', u'LocationID': 4012},
        {u'LocationName': u'.I.HQHIGATE120 2', u'LocationType': u'EXT. NODE', u'AreaType':
            u'EXTERNAL', u'LocationID': 4013},
        {u'LocationName': u'.I.SHOREHAM138 99', u'LocationType': u'EXT. NODE', u'AreaType':
            u'EXTERNAL', u'LocationID': 4014},
        {u'LocationName': u'.I.NRTHPORT138 5', u'LocationType': u'EXT. NODE', u'AreaType':
            u'EXTERNAL', u'LocationID': 4017},
        {u'LocationName': u'ROS', u'LocationType': u'SYSTEM', u'AreaType':
            u'INTERNAL', u'LocationID': 7000},
        {u'LocationName': u'SWCT', u'LocationType': u'RESERVE ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 7001},
        {u'LocationName': u'CT', u'LocationType': u'RESERVE ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 7002},
        {u'LocationName': u'NEMABSTN', u'LocationType': u'RESERVE ZONE', u'AreaType':
            u'INTERNAL', u'LocationID': 7003}]

import copy
from bs4 import BeautifulSoup
from pyiso.base import BaseClient


class PJMClient(BaseClient):
    NAME = 'PJM'
    TZ_NAME = 'America/New_York'
    base_url = 'http://edatamobile.pjm.com/eDataWireless/SessionManager'

    def time_from_soup(self, soup):
        """
        Returns a UTC timestamp if one is found in the soup,
        or None if an error was encountered.
        """
        ts_elt = soup.find(class_='ts')
        if not ts_elt:
            self.logger.error('PJM: Timestamp not found in soup:\n%s' % soup)
            return None
        return self.utcify(ts_elt.string)

    def val_from_soup(self, soup, key):
        """
        Returns a float value if one is found in the soup for the provided key,
        or None if an error was encountered.
        """
        for elt in soup.find_all('td'):
            try:
                if elt.find('a').string == key:
                    # numbers may have commas in the thousands
                    val_str = elt.next_sibling.string.replace(',', '')
                    return float(val_str)
            except AttributeError: # no 'a' child
                continue
            
        # no value found
        self.logger.error('PJM: Value for %s not found in soup:\n%s' % (key, soup))
        return None

    def fetch_edata(self, data_type, key):
        # get request
        response = self.request(self.base_url, params={'a': data_type})
        if not response:
            return None, None

        # soup it up
        soup = BeautifulSoup(response.content)
        
        # get time and value
        ts = self.time_from_soup(soup)
        val = self.val_from_soup(soup, key)

        # return
        return ts, val
                
    def get_generation(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='gen', **kwargs)

        # get data
        load_ts, load_val = self.fetch_edata('instLoad', 'PJM RTO Total')
        imports_ts, imports_val = self.fetch_edata('tieFlow', 'PJM RTO')
        wind_ts, wind_gen = self.fetch_edata('wind', 'RTO Wind Power')
        
        # compute nonwind gen
        try:
            total_gen = load_val - imports_val
            nonwind_gen = total_gen - wind_gen
        except TypeError: # value was None
            self.logger.error('PJM: No timestamps found for options %s' % str(self.options))
            return []

        # choose best time to use
        if load_ts:
            ts = load_ts
        elif imports_ts:
            ts = imports_ts
        elif wind_ts:
            ts = wind_ts
        else:
            self.logger.error('PJM: No timestamps found for options %s' % str(self.options))
            return []

        # set up storage
        parsed_data = []
        base_dp = {'timestamp': ts,
                   'freq': self.FREQUENCY_CHOICES.fivemin, 'market': self.MARKET_CHOICES.fivemin,
                   'gen_MW': 0, 'ba_name': self.NAME}

        # collect data
        for gen_MW, fuel_name in [(wind_gen, 'wind'), (nonwind_gen, 'nonwind')]:
            parsed_dp = copy.deepcopy(base_dp)
            parsed_dp['fuel_name'] = fuel_name
            parsed_dp['gen_MW'] = gen_MW
            parsed_data.append(parsed_dp)

        # return
        return parsed_data

    def get_load(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='load', **kwargs)

        # get data
        load_ts, load_val = self.fetch_edata('instLoad', 'PJM RTO Total')

        if load_ts and load_val:
            return [{'timestamp': load_ts,
                        'freq': self.FREQUENCY_CHOICES.fivemin,
                        'market': self.MARKET_CHOICES.fivemin,
                        'load_MW': load_val,
                        'ba_name': self.NAME,
                    }]
        else:
            return []
import copy
import re
from pyiso.base import BaseClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
from datetime import datetime


class SPPClient(BaseClient):
    NAME = 'SPP'
    TZ_NAME = 'America/Chicago'
    base_url = 'https://marketplace.spp.org/web/guest/'
        
    def get_fuels(self, year=2014):
        if year == 2014:
            return {
                'COAL': 'coal',
                'FUEL_OIL': 'oil',
                'GAS': 'natgas',
                'HYDRO': 'hydro',
                'NUCLEAR': 'nuclear',
                'OTHER': 'other',
                'PUMP_HYDRO': 'smhydro',
                'SOLAR': 'solar',
                'WASTE': 'refuse',
                'WIND': 'wind',
            }
            
        else:
            return {
                'COAL': 'coal',
                'HYDRO': 'hydro',
                'GAS': 'natgas',
                'NUCLEAR': 'nuclear',
                'DFO': 'oil',
                'WIND': 'wind',
            }
                
    def auth_keys(self):
        # use genmix page as example url
        url = self.base_url + 'generation-mix'

        # get page with link
        response = self.request(url)
        if not response:
            return ''

        # get tags from js
        idstr = 'PublicDisplays_WAR_PublicDisplaysportlet_INSTANCE_'
        m_auth = re.search('p_p_auth=(?P<val>[a-zA-Z0-9]{8})', response.content)
        m_id = re.search('p_p_id=%s(?P<val>[a-zA-Z0-9]{12})' % idstr, response.content)

        # return matches
        return m_auth.group('val'), idstr+m_id.group('val'), response.headers

    def fetch_csv(self, auth, idstr, headers):
        # assemble url params
        if self.options['data'] == 'gen':
            tail = 'generation-mix'
            params = {'p_p_auth': auth,
                        'p_p_id': idstr,
                        'p_p_lifecycle': 2,
                        'p_p_state': 'normal',
                        'p_p_mode': 'view',
                        'p_p_resource_id': 'APP/1/GenMix.csv',
                        'p_p_cacheability': 'cacheLevelPage',
                        }

        url = self.base_url +tail #+'?'+'&'.join('%s=%s' % (k,v) for k,v in params.iteritems())

        fp = webdriver.FirefoxProfile()

        fp.set_preference("browser.download.folderList",2)
        fp.set_preference("browser.download.manager.showWhenStarting",False)
        fp.set_preference("browser.download.dir", os.getcwd())
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/msexcel")


        driver = webdriver.Firefox()
        driver.get(url)
        driver.refresh()
        try:
            div = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "v-link"))
            )
            link = div.find_element_by_css_selector('a')
            link.click()
          #  print link.get_attribute('href')
            time.sleep(20)

        finally:
            driver.quit()

        # request
        # response = self.request(self.base_url+tail,
        #                         params=params,
        #                         headers={'etag': headers['etag']})
        # if not response:
        #     return ''

        # print response.cookies
        # print response.headers
        # print len(response.content)

    def _preprocess(self, row):
            vals = row.split(',')
            vals[0] = self.utcify(vals[0])
            return vals

    def get_generation(self, latest=False, market='RTHR',
                       start_at=None, end_at=None, yesterday=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, market=market, **kwargs)

        # set up requests
        if self.options['market'] == 'RTHR':
            file_freq = 'Hourly'
        elif self.options['market'] == 'RT5M':
            file_freq = '5Minute'
        else:
            raise ValueError('market must be RTHR or RT5M.')
        request_urls = []
        if self.options['latest']:
            request_urls.append('%d_%s_GenMix.csv' % (datetime.today().year, file_freq))            
        elif self.options['sliceable']:
            this_year = self.options['start_at'].year
            while this_year <= self.options['end_at'].year:
                request_urls.append('%d_%s_GenMix.csv' % (this_year, file_freq))
                this_year += 1
        
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
                return []
            
            # preliminary parsing
            rows = response.content.split('\n')
            header = rows[0].split(',')
            if latest:
                raw_data.append(dict(zip(header, self._preprocess(rows[-2]))))
            else:
                for row in rows[1:]:
                    vals = self._preprocess(row)
                    if vals[0] >= start_at and vals[0] <= end_at:
                        raw_data.append(dict(zip(header, vals)))
                                    
        # parse data
        for raw_dp in raw_data:
            # get timestamp and expected fuels
            ts = raw_dp['']
            fuels = self.get_fuels(ts.year)
            
            for raw_fuel_name, parsed_fuel_name in fuels.iteritems():
                # set up storage
                parsed_dp = {}   
    
                # add values
                parsed_dp['timestamp'] = ts
                try:
                    parsed_dp['gen_MW'] = float(raw_dp[raw_fuel_name])
                except KeyError:
                    self.logger.error('No data for %s found in %s; skipping this time.' % (raw_fuel_name, raw_dp))
                    break
                except ValueError:
                    self.logger.error('Found %s instead of float for %s; skipping time %s.' % (raw_dp[raw_fuel_name], raw_fuel_name, ts))
                    break                    
                parsed_dp['fuel_name'] = parsed_fuel_name
                parsed_dp['ba_name'] = self.ba_name
                if market == 'RTHR':
                    parsed_dp['freq'] = self.FREQUENCY_CHOICES.hourly
                    parsed_dp['market'] = self.MARKET_CHOICES.hourly
                elif market == 'RT5M':
                    parsed_dp['freq'] = self.FREQUENCY_CHOICES.fivemin
                    parsed_dp['market'] = self.MARKET_CHOICES.fivemin
                
                # add to full storage
                parsed_data.append(parsed_dp)
            
        return parsed_data

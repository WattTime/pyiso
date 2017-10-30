import time
from pyiso.base import BaseClient
from pyiso import LOGGER
import requests
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime, timedelta
import pytz
from os import environ
from lxml import objectify
import re


class EUClient(BaseClient):
    """
    EU Client

    EU Now uses a RESTful interface, https://transparency.entsoe.eu/api
    This uses a Security Token rather than a username/password combination.
    You need to register on the Transparency site to get a token.  Instructions
    are in the REST manual:
    https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
    Readable names for resources now use an alphanumeric code, e.g. A71 for the
    generation forecast series.  Those in use at the moment are:

    A65 = Total Load
    A69 = Generation Forecast (Wind/Solar)
    A71 = Generation Forecast By Type
    A73 = Generation Actual
    A74 = Generation Actual (Wind/Solar)
    A75 = Generation Actual by Type

    Within these are codes for the type of report.  E.g.

    A01 = Day ahead hourly
    A16 = Realised
    """
    NAME = 'EU'
    TZ_NAME = 'UTC'
    base_url = 'https://transparency.entsoe.eu/api'

    CONTROL_AREAS = {
        'AL': {'country': 'Albania', 'Code': 'CTA|AL',
            'ENTSOe_ID': '10YAL-KESH-----5',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'AT': {'country': 'Austria', 'Code': 'CTA|AT',
            'ENTSOe_ID': '10YAT-APG------L',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'BY': {'country': 'Belarus', 'Code': 'CTA|BY',
            'ENSTOe_ID': '10Y1001A1001S51S',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'BE': {'country': 'Belgium', 'Code': 'CTA|BE',
            'ENTSOe_ID': '10YBE----------2',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'BA': {'country': 'Bosnia and Herz. ', 'Code': 'CTA|BA',
            'ENTSOe_ID': '10YBA-JPCC-----D',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'BG': {'country': 'Bulgaria', 'Code': 'CTA|BG',
            'ENTSOe_ID': '10YCA-BULGARIA-R',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'HR': {'country': 'Croatia', 'Code': 'CTA|HR',
            'ENTSOe_ID': '10YHR-HEP------M',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'CY': {'country': 'Cyprus', 'Code': 'CTA|CY',
            'ENTSOe_ID': '10YCY-1001A0003J',
            'gen_freq': '30m', 'gen_market': 'RT5M'},
        'CZ': {'country': 'Czech Republic', 'Code': 'CTA|CZ',
            'ENTSOe_ID': '10YCZ-CEPS-----N',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'PL-CZ': {'country': 'Czech Republic', 'Code': 'CTA|PL-CZ',
            'ENTSOe_ID': '10YDOM-1001A082L',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'DK': {'country': 'Denmark', 'Code': 'CTA|DK',
            'ENTSOe_ID': '10Y1001A1001A796',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'EE': {'country': 'Estonia', 'Code': 'CTA|EE',
            'ENTSOe_ID': '10Y1001A1001A39I',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'MK': {'country': 'FYR Macedonia', 'Code': 'CTA|MK',
            'ENTSOe_ID': '10YMK-MEPSO----8',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'FI': {'country': 'Finland', 'Code': 'CTA|FI',
            'ENTSOe_ID': '10YFI-1--------U',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'FR': {'country': 'France', 'Code': 'CTA|FR',
            'ENTSOe_ID': '10YFR-RTE------C',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'DE(50HzT)': {'country': 'Germany', 'Code': 'CTA|DE(50HzT)',
            'ENTSOe_ID': '10YDE-VE-------2',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'DE(Amprion)': {'country': 'Germany', 'Code': 'CTA|DE(Amprion)',
            'ENTSOe_ID': '10YDE-RWENET---I',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'DE(TenneT GER)': {'country': 'Germany', 'Code': 'CTA|DE(TenneT GER)',
            'ENTSOe_ID': '10YDE-EON------1',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'DE(TransnetBW)': {'country': 'Germany', 'Code': 'CTA|DE(TransnetBW)',
            'ENTSOe_ID': '10YDE-ENBW-----N',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'GR': {'country': 'Greece', 'Code': 'CTA|GR',
            'ENTSOe_ID': '10YGR-HTSO-----Y',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'HU': {'country': 'Hungary', 'Code': 'CTA|HU',
            'ENTSOe_ID': '10YHU-MAVIR----U',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'IE': {'country': 'Ireland', 'Code': 'CTA|IE',
            'ENTSOe_ID': '10YIE-1001A00010',
            'gen_freq': '30m', 'gen_market': 'RT5M'},
        'IT': {'country': 'Italy', 'Code': 'CTA|IT',
            'ENTSOe_ID': '10YIT-GRTN-----B',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'LV': {'country': 'Latvia', 'Code': 'CTA|LV',
            'ENTSOe_ID': '10YLV-1001A00074',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'LT': {'country': 'Lithuania', 'Code': 'CTA|LT',
            'ENTSOe_ID': '10YLT-1001A0008Q',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'LU': {'country': 'Luxembourg', 'Code': 'CTA|LU',
            'ENTSOe_ID': '10YLU-CEGEDEL-NQ',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'MT': {'country': 'Malta', 'Code': 'CTA|MT',
            'ENTSOe_ID': '10Y1001A1001A93C',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'MD': {'country': 'Moldavia', 'Code': 'CTA|MD',
            'ENTSOe_ID': '10Y1001A1001A990',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'ME': {'country': 'Montenegro', 'Code': 'CTA|ME',
            'ENTSOe_ID': '10YCS-CG-TSO---S',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'NL': {'country': 'Netherlands', 'Code': 'CTA|NL',
            'ENTSOe_ID': '10YNL----------L',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'NO': {'country': 'Norway', 'Code': 'CTA|NO',
            'ENTSOe_ID': '10YNO-0--------C',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'PL': {'country': 'Poland', 'Code': 'CTA|PL',
            'ENTSOe_ID': '10YPL-AREA-----S',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'PT': {'country': 'Portugal', 'Code': 'CTA|PT',
            'ENTSOe_ID': '10YPT-REN------W',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'RO': {'country': 'Romania', 'Code': 'CTA|RO',
            'ENTSOe_ID': '10YRO-TEL------P',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'RU': {'country': 'Russia', 'Code': 'CTA|RU',
            'ENTSOe_ID': '10YRO-TEL------P',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'RU-KGD': {'country': 'Russia', 'Code': 'CTA|RU-KGD',
            'ENTSOe_ID': '10Y1001A1001A50U',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'RS': {'country': 'Serbia', 'Code': 'CTA|RS',
            'ENTSOe_ID': '10YCS-SERBIATSOV',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'SK': {'country': 'Slovakia', 'Code': 'CTA|SK',
            'ENTSOe_ID': '10YSK-SEPS-----K',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'SI': {'country': 'Slovenia', 'Code': 'CTA|SI',
            'ENTSOe_ID': '10YSI-ELES-----O',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'ES': {'country': 'Spain', 'Code': 'CTA|ES',
            'ENTSOe_ID': '10YES-REE------0',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'SE': {'country': 'Sweden', 'Code': 'CTA|SE',
            'ENTSOe_ID': '10YSE-1--------K',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'CH': {'country': 'Switzerland', 'Code': 'CTA|CH',
            'ENTSOe_ID': '10YCH-SWISSGRIDZ',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'TR': {'country': 'Turkey', 'Code': 'CTA|TR',
            'ENTSOe_ID': '10YTR-TEIAS----W',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'UA': {'country': 'Ukraine', 'Code': 'CTA|UA',
            'ENTSOe_ID': '10Y1001A1001A869',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'UA-WEPS': {'country': 'Ukraine', 'Code': 'CTA|UA-WEPS',
            'ENTSOe_ID': '10YUA-WEPS-----0',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'UA-UNK': {'country': 'Ukraine', 'Code': 'CTA|UA-UNK',
           'ENTSOe_ID': '10Y1001C--000182',
           'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'NIE': {'country': 'United Kingdom', 'Code': 'CTA|NIE',
            'ENTSOe_ID': '10Y1001A1001A016',
            'gen_freq': '30m', 'gen_market': 'RT5M'},
        'National Grid': {'country': 'United Kingdom', 'Code': 'CTA|National Grid',
            'ENTSOe_ID': '10YGB----------A',
            'gen_freq': '30m', 'gen_market': 'RT5M'},
        }

    fuels = {
        'B01': 'biomass',    # Biomass
        'B02': 'coal',       # Brown coal/Lignite
        'B03': 'fossil',     # Coal derived gas
        'B04': 'natgas',     # Natural gas
        'B05': 'coal',       # Hard coal/Anthracite
        'B06': 'oil',        # Oil
        'B07': 'oil',        # Shale Oil
        'B08': 'fossil',     # Peat
        'B09': 'geo',        # Geothermal
        'B10': 'hydro',      # Hydro - Pumped Storage
        'B11': 'hydro',      # Hydro Run-of-river and poundage
        'B12': 'hydro',      # Hydro Water Reservoir
        'B13': 'renewable',  # Marine
        'B14': 'nuclear',    # Nuclear
        'B15': 'renewable',  # Other renewable
        'B16': 'solar',      # Solar
        'B17': 'refuse',     # Waste
        'B18': 'wind',       # Wind Offshore
        'B19': 'wind',       # Wind Onshore
        'B20': 'other'       # Other
    }


    def get_load(self, control_area=None, latest=False, start_at=None, end_at=None,
                 forecast=False, **kwargs):
        self.handle_options(data='load', start_at=start_at, end_at=end_at, forecast=forecast,
                            latest=latest, control_area=control_area, **kwargs)

        response = self.fetch_entsoe()
        return self.parse_response(response)

    def get_generation(self, control_area=None, latest=False, yesterday=False, start_at=False, 
                       end_at=False, forecast=False, **kwargs):
        self.handle_options(data='gen', start_at=start_at, end_at=end_at, yesterday=yesterday, 
                            latest=latest, control_area=control_area, forecast=False, **kwargs)

        response = self.fetch_entsoe()
        return self.parse_response(response)

    def handle_options(self, **kwargs):
        # regular handle options
        super(EUClient, self).handle_options(**kwargs)

        # if latest is True
        if self.options.get('latest', None):
            self.options.update(start_at=datetime.now(pytz.utc) - timedelta(days=1),
                                end_at=datetime.now(pytz.utc))

        # workaround for base.handle_options setting forecast to false if end_at too far in past
        if 'forecast' in kwargs and kwargs['forecast']:
            self.options['forecast'] = True

    def fetch_entsoe(self):
        payload = {
            'securityToken': environ['ENTSOe_SECURITY_TOKEN']
        }

        format_str = "%Y%m%d%H00"
        date_from = self.options['start_at'].strftime(format_str)
        date_to = self.options['end_at'].strftime(format_str)

        TSO_ID = self.get_tso_id()

        if self.options['data'] == 'load':
            domainType = 'outBiddingZone_Domain'
            documentType = 'A65'
        elif self.options['data'] == 'gen':
            domainType = 'in_Domain'
            if self.options['forecast']:
                documentType = 'A71'
            else:
                documentType = 'A75'

        if (self.options['forecast']):
            processType = 'A01'
        else:
            processType = 'A16'

        payload.update({
          domainType: TSO_ID,
          'documentType': documentType,
          'processType': processType,
          'periodStart': date_from,
          'periodEnd': date_to
        })

        r = self.request(self.base_url, params=payload)
        # For some reason lxml gets pernikity about the XML with a header.
        return r.text.encode('ascii')

    def parse_response(self, response):
        """
        Take the XML repsonse, pull out the required components
        and return a list of dicts containing the data requested
        """
        data = []
        xmldoc = objectify.fromstring(response)
        for ts in xmldoc.TimeSeries:
            for period in ts.Period:
                initialOffset = self.utcify(period.timeInterval.start.text)
                resolution = self.parse_resolution(period.resolution.text)
                if self.options['latest']:
                  points = [ period.Point[-1] ]
                else:
                  points = period.Point
                for point in points:
                    if int(point.quantity.text) == 0:
                      continue
                    timestamp = initialOffset + resolution * point.position
                    datapoint = {
                        'ba_name': self.options['control_area'],
                        'market': 'RTHR',
                        'timestamp': timestamp,
                    }
                    if (self.options['forecast']):
                        datapoint['market'] = 'DAM'
                    if self.options['data'] == 'gen':
                      datapoint['market'] = self.CONTROL_AREAS[self.options['control_area']]['gen_market']
                      datapoint['freq'] = self.CONTROL_AREAS[self.options['control_area']]['gen_freq']
                      datapoint['gen_MW'] = int(point.quantity.text)
                      datapoint['fuel_name'] = self.fuels[ts.MktPSRType.psrType.text]
                    elif self.options['data'] == 'load':
                      datapoint['load_MW'] = int(point.quantity.text)
                    data.append(datapoint)
        return data


    def parse_resolution(self, resolution):
        """
        Resolutions are given as ISO8601 durations.
        While the number of these seen is only a handful
        this method tries to handle all possible values
        """
        pattern = """^P
                  ((?P<year>[0-9.]+)Y)?
                  ((?P<month>[0-9.]+)M)?
                  ((?P<day>[0-9.]+)D)?
                  (T
                  ((?P<hour>[0-9.]+)H)?
                  ((?P<minute>[0-9.]+)M)?
                  ((?P<second>[0-9.]+)S)?
                  )?$"""
        matched = re.match(pattern, resolution, re.X).groupdict(0)
        days = float(matched['year']) * 365 + float(matched['month']) * 30 + float(matched['day']);

        return timedelta(days=days, hours=float(matched['hour']),
               minutes=float(matched['minute']), seconds=float(matched['second']))

    def get_tso_id(self):
        # TSO ID from control area code
        try:
            return self.CONTROL_AREAS[self.options['control_area']]['ENTSOe_ID']
        except KeyError:
            msg = 'Control area code not found for %s. Options are %s' % (self.options['control_area'],
                                                                          sorted(self.CONTROL_AREAS.keys()))
            raise ValueError(msg)



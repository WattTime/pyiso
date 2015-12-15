from pyiso.base import BaseClient
from pyiso import LOGGER
import requests
import pandas as pd
import numpy as np
from io import StringIO
from time import sleep
from datetime import datetime, timedelta
import pytz
from os import environ
import xmltodict

example_properties = [{
        u'Name': 'DataItem', 
        u'Value': 'CB_BALANCING_VOLUMES_OF_EXCHANGED'
    },
    {
        'Name': 'TimeInterval',
        'Value': '2015-03-08 00:00:00/2015-03-09 00:00:00'
    },
    {
        'Name': 'AREA|MBA',
        'Value': '10YFR-RTE------C'
    },
    {
        'Name': 'AREA|MBA',
        'Value': '10YES-REE------0'
    }
]

MESSAGE_BODY = """
<soap:Envelope xmlns:msg="http://iec.ch/TC57/2011/schema/message" xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
    <soap:Header/>
        <soap:Body>
            <msg:RequestMessage>
                <msg:Header>
                    <msg:Verb>{verb}</msg:Verb>
                    <msg:Noun>{noun}</msg:Noun>
                    <msg:Context>{context}</msg:Context>
                    {properties}
                </msg:Header>
            </msg:RequestMessage>
        </soap:Body>
</soap:Envelope>
"""
MESSAGE_PROPERTIES = """
                    <msg:Property>
                        <msg:Name>{Name}</msg:Name>
                        <msg:Value>{Value}</msg:Value>
                    </msg:Property>
"""


class EUClient(BaseClient):
    NAME = 'EU'
    TZ_NAME = 'UTC'
    base_url = 'https://transparency-ws.entsoe.eu/'
    endpoint = 'data-receiver-ws/endpoints/DataService'
    export_endpoint = 'load-domain/r2/totalLoadR2/export'

    CONTROL_AREAS = {
        'AL': {'country': 'Albania', 'Code': 'CTA|AL',
            'ENTSOe_ID': 'CTY|10YAL-KESH-----5!CTA|10YAL-KESH-----5'},
        'AT': {'country': 'Austria', 'Code': 'CTA|AT',
            'ENTSOe_ID': 'CTY|10YAT-APG------L!CTA|10YAT-APG------L'},
        'BE': {'country': 'Belgium', 'Code': 'CTA|BE',
            'ENTSOe_ID': 'CTY|10YBE----------2!CTA|10YBE----------2'},
        'BA': {'country': 'Bosnia and Herz. ', 'Code': 'CTA|BA',
            'ENTSOe_ID': 'CTY|10YBA-JPCC-----D!CTA|10YBA-JPCC-----D'},
        'BG': {'country': 'Bulgaria', 'Code': 'CTA|BG',
            'ENTSOe_ID': 'CTY|10YCA-BULGARIA-R!CTA|10YCA-BULGARIA-R'},
        'HR': {'country': 'Croatia', 'Code': 'CTA|HR',
            'ENTSOe_ID': 'CTY|10YHR-HEP------M!CTA|10YHR-HEP------M'},
        'CY': {'country': 'Cyprus', 'Code': 'CTA|CY',
            'ENTSOe_ID': 'CTY|10YCY-1001A0003J!CTA|10YCY-1001A0003J'},
        'CZ': {'country': 'Czech Republic', 'Code': 'CTA|CZ',
            'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!CTA|10YCZ-CEPS-----N'},
        'PL-CZ': {'country': 'Czech Republic', 'Code': 'CTA|PL-CZ',
            'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!CTA|10YDOM-1001A082L'},
        'DK': {'country': 'Denmark', 'Code': 'CTA|DK',
            'ENTSOe_ID': 'CTY|10Y1001A1001A65H!CTA|10Y1001A1001A796'},
        'EE': {'country': 'Estonia', 'Code': 'CTA|EE',
            'ENTSOe_ID': 'CTY|10Y1001A1001A39I!CTA|10Y1001A1001A39I'},
        'MK': {'country': 'FYR Macedonia', 'Code': 'CTA|MK',
            'ENTSOe_ID': 'CTY|10YMK-MEPSO----8!CTA|10YMK-MEPSO----8'},
        'FI': {'country': 'Finland', 'Code': 'CTA|FI',
            'ENTSOe_ID': 'CTY|10YFI-1--------U!CTA|10YFI-1--------U'},
        'FR': {'country': 'France', 'Code': 'CTA|FR',
            'ENTSOe_ID': 'CTY|10YFR-RTE------C!CTA|10YFR-RTE------C'},
        'DE(50HzT)': {'country': 'Germany', 'Code': 'CTA|DE(50HzT)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-VE-------2'},
        'DE(Amprion)': {'country': 'Germany', 'Code': 'CTA|DE(Amprion)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-RWENET---I'},
        'DE(TenneT GER)': {'country': 'Germany', 'Code': 'CTA|DE(TenneT GER)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-EON------1&'},
        'DE(TransnetBW)': {'country': 'Germany', 'Code': 'CTA|DE(TransnetBW)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-ENBW-----N'},
        'GR': {'country': 'Greece', 'Code': 'CTA|GR',
            'ENTSOe_ID': 'CTY|10YGR-HTSO-----Y!CTA|10YGR-HTSO-----Y'},
        'HU': {'country': 'Hungary', 'Code': 'CTA|HU',
            'ENTSOe_ID': 'CTY|10YHU-MAVIR----U!CTA|10YHU-MAVIR----U'},
        'IE': {'country': 'Ireland', 'Code': 'CTA|IE',
            'ENTSOe_ID': 'CTY|10YIE-1001A00010!CTA|10YIE-1001A00010'},
        'IT': {'country': 'Italy', 'Code': 'CTA|IT',
            'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!CTA|10YIT-GRTN-----B'},
        'LV': {'country': 'Latvia', 'Code': 'CTA|LV',
            'ENTSOe_ID': 'CTY|10YLV-1001A00074!CTA|10YLV-1001A00074'},
        'LT': {'country': 'Lithuania', 'Code': 'CTA|LT',
            'ENTSOe_ID': 'CTY|10YLT-1001A0008Q!CTA|10YLT-1001A0008Q'},
        'LU': {'country': 'Luxembourg', 'Code': 'CTA|LU',
            'ENTSOe_ID': 'CTY|10YLU-CEGEDEL-NQ!CTA|10YLU-CEGEDEL-NQ'},
        'MT': {'country': 'Malta', 'Code': 'CTA|MT',
            'ENTSOe_ID': 'CTY|MT!CTA|Not+delivered+MT'},
        'MD': {'country': 'Moldavia', 'Code': 'CTA|MD',
            'ENTSOe_ID': 'CTY|MD!CTA|Not+delivered+MD'},
        'ME': {'country': 'Montenegro', 'Code': 'CTA|ME',
            'ENTSOe_ID': 'CTY|10YCS-CG-TSO---S!CTA|10YCS-CG-TSO---S'},
        'NL': {'country': 'Netherlands', 'Code': 'CTA|NL',
            'ENTSOe_ID': 'CTY|10YNL----------L!CTA|10YNL----------L'},
        'NO': {'country': 'Norway', 'Code': 'CTA|NO',
            'ENTSOe_ID': 'CTY|10YNO-0--------C!CTA|10YNO-0--------C'},
        'PL': {'country': 'Poland', 'Code': 'CTA|PL',
            'ENTSOe_ID': 'CTY|10YPL-AREA-----S!CTA|10YPL-AREA-----S'},
        'PT': {'country': 'Portugal', 'Code': 'CTA|PT',
            'ENTSOe_ID': 'CTY|10YPT-REN------W!CTA|10YPT-REN------W'},
        'RO': {'country': 'Romania', 'Code': 'CTA|RO',
            'ENTSOe_ID': 'CTY|10YRO-TEL------P!CTA|10YRO-TEL------P'},
        'RU': {'country': 'Russia', 'Code': 'CTA|RU',
            'ENTSOe_ID': 'CTY|10YRO-TEL------P!CTA|10YRO-TEL------P'},
        'RU-KGD': {'country': 'Russia', 'Code': 'CTA|RU-KGD',
            'ENTSOe_ID': 'CTY|RU!CTA|10Y1001A1001A50U'},
        'RS': {'country': 'Serbia', 'Code': 'CTA|RS',
            'ENTSOe_ID': 'CTY|10YCS-SERBIATSOV!CTA|10YCS-SERBIATSOV'},
        'SK': {'country': 'Slovakia', 'Code': 'CTA|SK',
            'ENTSOe_ID': 'CTY|10YSK-SEPS-----K!CTA|10YSK-SEPS-----K'},
        'SI': {'country': 'Slovenia', 'Code': 'CTA|SI',
            'ENTSOe_ID': 'CTY|10YSI-ELES-----O!CTA|10YSI-ELES-----O'},
        'ES': {'country': 'Spain', 'Code': 'CTA|ES',
            'ENTSOe_ID': 'CTY|10YES-REE------0!CTA|10YES-REE------0'},
        'SE': {'country': 'Sweden', 'Code': 'CTA|SE',
            'ENTSOe_ID': 'CTY|10YSE-1--------K!CTA|10YSE-1--------K'},
        'CH': {'country': 'Switzerland', 'Code': 'CTA|CH',
            'ENTSOe_ID': 'CTY|10YCH-SWISSGRIDZ!CTA|10YCH-SWISSGRIDZ'},
        'TR': {'country': 'Turkey', 'Code': 'CTA|TR',
            'ENTSOe_ID': 'CTY|TR!CTA|10YTR-TEIAS----W'},
        'UA': {'country': 'Ukraine', 'Code': 'CTA|UA',
            'ENTSOe_ID': 'CTY|UA!CTA|10Y1001A1001A869'},
        'UA-WEPS': {'country': 'Ukraine', 'Code': 'CTA|UA-WEPS',
            'ENTSOe_ID': 'CTY|UA!CTA|10YUA-WEPS-----0'},
        'NIE': {'country': 'United Kingdom', 'Code': 'CTA|NIE',
            'ENTSOe_ID': 'CTY|GB!CTA|10Y1001A1001A016'},
        'National Grid': {'country': 'United Kingdom', 'Code': 'CTA|National Grid',
            'ENTSOe_ID': 'CTY|GB!CTA|10YGB----------A'},
        }

    def construct_payload(self, prop_list,
                     verb='get', noun='EnergyAccountReport', context='Testing'):
        prop_xml = ''
        for prop in prop_list:
            prop_xml = prop_xml + MESSAGE_PROPERTIES.format(**prop)
        msg = MESSAGE_BODY.format(verb=verb, noun=noun, context=context,
                                  properties=prop_xml)
        return msg

    def get_load(self, control_area=None, latest=False, start_at=None, end_at=None,
                 forecast=False, **kwargs):
        self.handle_options(data='load', start_at=start_at, end_at=end_at, forecast=forecast,
                            latest=latest, control_area=control_area, **kwargs)

        pieces = []
        for date in self.dates():
            payload = self.construct_payload(date)
            url = self.base_url + self.export_endpoint
            response = self.fetch_entsoe(url, payload)
            day_df = self.parse_load_response(response)
            pieces.append(day_df)

        df = pd.concat(pieces)
        sliced = self.slice_times(df)
        return self.serialize_faster(sliced)

    def temp_example(self):
        payload = self.construct_payload(example_properties)
        return self.fetch_entsoe(payload)

    def temp_get_load(self, control_area=None):
        area_A, area_B = self.CONTROL_AREAS[control_area]['ENTSOe_ID'].split('!')
        prop_list = [{
                u'Name': 'DataItem',
                u'Value': 'ACTUAL_TOTAL_LOAD'},
            {
                'Name': 'TimeInterval',
                'Value': '2015-03-08 00:00:00/2015-03-09 00:00:00'},
             {
                'Name': 'AREA|CTA',
                'Value': area_B,
            }]

        payload = self.construct_payload(prop_list)
        response = self.fetch_entsoe(payload)
        return self.parse_load_response(response)

    def handle_options(self, **kwargs):
        # regular handle options
        super(EUClient, self).handle_options(**kwargs)

        # if latest is True
        if self.options.get('latest', None):
            self.options.update(start_at=datetime.now(pytz.utc) - timedelta(days=1),
                                end_at=datetime.now(pytz.utc))

        # workaround for base.handle_options setting forecast to false if end_at too far in past
        if kwargs['forecast']:
            self.options['forecast'] = True

    def auth(self):
        if not getattr(self, 'session', None):
            self.session = requests.Session()
            self.session.headers.update({'content-type': 'application/soap+xml'})
            self.session.cert = ('dataconsumer_cert.pem', 'dataconsumer_keyclear.pem')

    def fetch_entsoe(self, payload, count=0):
        if not getattr(self, 'session', None):
             self.auth()

        url = self.base_url + self.endpoint
        r = self.request(url, data=payload, mode='post')

        # TODO error checking
        if len(r.text) == 0:
            if count > 3:  # try 3 times to get response
                LOGGER.warn('Request failed, no response found after %i attempts' % count)
                return False
            # throttled
            sleep(5)
            return self.fetch_entsoe(url, payload, count + 1)
        return r.text


    def parse_load_response(self, response):
        full_dict = xmltodict.parse(response)
        data_dict = full_dict['env:Envelope']['env:Body']['msg:ResponseMessage']['msg:Payload']\
                ['GL_MarketDocument:GL_MarketDocument']['TimeSeries']['Period']['Point']
        return data_dict

#     def construct_payload(self, date):
#         # format date
#         format_str = '%d.%m.%Y'
#         date_str = date.strftime(format_str) + ' 00:00|UTC|DAY'
# 
#         # TSO ID from control area code
#         try:
#             TSO_ID = self.CONTROL_AREAS[self.options['control_area']]['ENTSOe_ID']
#         except KeyError:
#             msg = 'Control area code not found for %s. Options are %s' % (self.options['control_area'],
#                                                                           sorted(self.CONTROL_AREAS.keys()))
#             raise ValueError(msg)
# 
#         payload = {
#             'name': '',
#             'defaultValue': 'false',
#             'viewType': 'TABLE',
#             'areaType': 'CTA',
#             'atch': 'false',
#             'dateTime.dateTime': date_str,
#             'biddingZone.values': TSO_ID,
#             'dateTime.timezone': 'UTC',
#             'dateTime.timezone_input': 'UTC',
#             'exportType': 'CSV',
#             'dataItem': 'ALL',
#             'timeRange': 'DEFAULT',
#         }
#         return payload
# 
#     def parse_load_response(self, response):
#         df = pd.read_csv(StringIO(response))
# 
#         # get START_TIME_UTC as tz-aware datetime
#         df['START_TIME_UTC'], df['END_TIME_UTC'] = zip(
#             *df['Time (UTC)'].apply(lambda x: x.split('-')))
# 
#         # Why do these methods only work on Index and not Series?
#         df.set_index(df.START_TIME_UTC, inplace=True)
#         df.index = pd.to_datetime(df.index, utc=True, format='%d.%m.%Y %H:%M ')
#         df.index.set_names('timestamp', inplace=True)
# 
#         # find column name and choose which to return and which to drop
#         (forecast_load_col, ) = [c for c in df.columns if 'Day-ahead Total Load Forecast [MW]' in c]
#         (actual_load_col, ) = [c for c in df.columns if 'Actual Total Load [MW]' in c]
#         if self.options['forecast']:
#             load_col = forecast_load_col
#             drop_load_col = actual_load_col
#         else:
#             load_col = actual_load_col
#             drop_load_col = forecast_load_col
# 
#         # rename columns for list of dicts
#         rename_d = {load_col: 'load_MW'}
#         df.rename(columns=rename_d, inplace=True)
#         drop_col = ['Time (UTC)', 'END_TIME_UTC', 'START_TIME_UTC', drop_load_col]
#         df.drop(drop_col, axis=1, inplace=True)
# 
#         # drop nan rows
#         df.replace('-', np.nan, inplace=True)
#         df.dropna(subset=['load_MW'], inplace=True)
# 
#         # Add columns
#         df['ba_name'] = self.options['control_area']
#         df['freq'] = '1hr'
#         df['market'] = 'RTHR'  # not necessarily appropriate terminology
# 
#         return df
# 

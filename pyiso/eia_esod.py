from pyiso.base import BaseClient
import json
from os import environ
from dateutil.parser import parse as dateutil_parse
from datetime import datetime, timedelta
import pytz


class EIACLIENT(BaseClient):
    """
    Interface to EIA API.

    The EIA API provides this information for the US lower 48 and beyond:
     -Hourly load (actual and forecast),
     -Generation
     -Imports/exports

    Full listing of BAs with time zones here:
    https://www.eia.gov/beta/realtime_grid/docs/UserGuideAndKnownIssues.pdf

    """

    NAME = 'EIA'

    base_url = 'http://api.eia.gov/'

    fuels = {
        'Other': 'other',
    }

    def __init__(self, *args, **kwargs):
        super(EIACLIENT, self).__init__(*args, **kwargs)
        try:
            self.auth = environ['EIA_KEY']
        except KeyError:
            msg = 'You must define EIA_KEY environment variable to use the \
                   EIA client.'
            raise RuntimeError(msg)

        self.category_url = self.base_url + "category/?api_key=%s&category_id="\
            % self.auth
        self.series_url = self.base_url + "series/?api_key=%s&series_id=EBA."\
            % self.auth

    def get_generation(self, latest=False, yesterday=False,
                       start_at=False, end_at=False, **kwargs):
        """
        Scrape and parse generation fuel mix data.

        :param bool latest: If True, only get the generation mix at the one most recent available time point.
           Available for all regions.
        :param bool yesterday: If True, get the generation mix for every time point yesterday.
           Not available for all regions.
        :param datetime start_at: If the datetime is naive, it is assumed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be greater than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :param datetime end_at: If the datetime is naive, it is assumed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be less than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
           Timestamps are in UTC.
        :rtype: list

        """

        self.handle_options(data='gen', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)

        result = json.loads(self.request(self.url).text)
        result_formatted = self.format_result(result)

        return result_formatted

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, forecast=False, **kwargs):
        """
        Scrape and parse load data.

        :param bool latest: If True, only get the load at the one most recent available time point.
           Available for all regions.
        :param bool yesterday: If True, get the load for every time point yesterday.
           Not available for all regions.
        :param datetime start_at: If the datetime is naive, it is assumed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be greater than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :param datetime end_at: If the datetime is naive, it is assumed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be less than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list

        """

        self.handle_options(data='load', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at,
                            forecast=forecast, **kwargs)
        result = json.loads(self.request(self.url).text)
        result_formatted = self.format_result(result)
        return result_formatted

    def get_trade(self, latest=False, yesterday=False, start_at=False,
                  end_at=False, **kwargs):

        """
        Scrape and parse import/export data.
        Value is net export (export - import), can be positive or negative.

        -:param bool latest: If True, only get the trade at the one most recent available time point.
           Available for all regions.
        :param bool yesterday: If True, get the trade for every time point yesterday.
           Not available for all regions.
        :param datetime start_at: If the datetime is naive, it is assumed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be greater than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :param datetime end_at: If the datetime is naive, it is assumed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be less than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, net_exp_MW]``.
           Timestamps are in UTC.
        :rtype: list

        """

        self.handle_options(data='trade', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)
        result = json.loads(self.request(self.url).text)
        result_formatted = self.format_result(result)

        return result_formatted

    def handle_options(self, **kwargs):
        # Need to clean up this method
        super(EIACLIENT, self).handle_options(**kwargs)

        self.options = kwargs

        if "bal_auth" not in self.options:
            if self.data == "gen":
                self.url = self.category_url + "2122629"
            elif self.data == "load":
                if self.options["forecast"]:
                    self.url = self.category_url + "2122627"
                else:
                    self.url = self.category_url + "2122628"
            elif self.data == "trade":
                self.url = self.category_url + "2122632"
        else:
            if self.options["data"] == "gen":
                self.url = self.series_url + "%s-ALL.NG.H" % self.options["bal_auth"]
            elif self.options["data"] == "load":
                if self.options['forecast']:
                        self.url = self.series_url + "%s-ALL.DF.H" % self.options["bal_auth"]
                else:
                    self.url = self.series_url + "%s-ALL.D.H" % self.options["bal_auth"]
            elif self.options["data"] == "trade":
                self.url = self.series_url + "%s-ALL.TI.H" % self.options["bal_auth"]


        # reconcile this w/ format results- may need to cull format results.

        # ensure market and freq are set
        # if 'market' not in self.options:
        #     if self.options['forecast']:
        #         self.options['market'] = self.MARKET_CHOICES.dam
        #     else:
        #         self.options['market'] = self.MARKET_CHOICES.dam
        # if 'freq' not in self.options:
        #     if self.options['forecast']:
        #         self.options['freq'] = self.FREQUENCY_CHOICES.hourly
        #     else:
        #         self.options['freq'] = self.FREQUENCY_CHOICES.fivemin
        #
        if 'latest' not in self.options:
            self.options['latest'] = False
        if 'forecast' not in self.options:
            self.options['forecast'] = False

        # if self.options.get('start_at') or self.options.get('end_at') or not self.options.get('latest'):
        #         raise ValueError('PJM 5-minute lmp only available for latest, not for date ranges')
        # self.options['latest'] = True

        """
        Process and store keyword argument options.
        """
        # i think this is already covered - get this in the correct order
        # self.options = kwargs

        # check start_at and end_at args
        if self.options.get('start_at', None) and self.options.get('end_at', None):
            assert self.options['start_at'] < self.options['end_at']
            self.options['start_at'] = self.utcify(self.options['start_at'])
            self.options['end_at'] = self.utcify(self.options['end_at'])
            self.options['sliceable'] = True
            self.options['latest'] = False

            # force forecast to be True if end_at is in the future
            if self.options['end_at'] > pytz.utc.localize(datetime.utcnow()):
                self.options['forecast'] = True
            else:
                self.options['forecast'] = False

        # set start_at and end_at for yesterday in local time
        elif self.options.get('yesterday', None):
            local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))
            self.options['end_at'] = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            self.options['start_at'] = self.options['end_at'] - timedelta(days=1)
            self.options['sliceable'] = True
            self.options['latest'] = False
            self.options['forecast'] = False

        # set start_at and end_at for today+tomorrow in local time
        elif self.options.get('forecast', None):
            local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))
            self.options['start_at'] = local_now.replace(microsecond=0)
            self.options['end_at'] = self.options['start_at'] + timedelta(days=2)
            self.options['sliceable'] = True
            self.options['latest'] = False
            self.options['forecast'] = True

        else:
            self.options['sliceable'] = False
            self.options['forecast'] = False

    def format_result(self, data):
        # self.handle_timezone(self.options["bal_auth"])
        # Determine if we need to make this conversion

        """Output EIA API results in pyiso format"""
        if self.options["forecast"]:
            market = "DAHR"
        else:
            market = "RTHR"

        if self.options["data"] == 'trade':
            data_type = "net_exp_MW"
        elif self.options["data"] == "gen":
            data_type = "gen_MW"
        elif self.options["data"] == "load":
            data_type = "load_MW"

        data_formatted = []

        if self.options['latest']:
            last_datapoint = data["series"][0]["data"][0]
            data_formatted.append(
                                    {
                                        "ba_name": self.options["bal_auth"],
                                        "timestamp": last_datapoint[0],
                                        # "freq": need to add this
                                        data_type: last_datapoint[1],
                                        "market": market
                                    }
                        )
            if self.options["data"] == "gen":
                for i in data_formatted:
                    i["fuel_name"] = "other"
        elif self.options["yesterday"]:
            yesterday = self.local_now() - timedelta(days=1)

            for i in data["series"]:
                for j in i["data"]:
                    timestamp = dateutil_parse(j[0])
                    if timestamp.year == yesterday.year and \
                       timestamp.month == yesterday.month and \
                       timestamp.day == yesterday.day:
                        data_formatted.append(
                                            {
                                                "ba_name": self.options["bal_auth"],
                                                "timestamp": j[0],
                                                # "freq": need to add this
                                                data_type: j[1],
                                                "market": market
                                            }
                                        )
            if self.options["data"] == "gen":
                for i in data_formatted:
                    i["fuel_name"] = "other"
        else:
            try:
                for i in data["series"]:
                    for j in i["data"]:
                        data_formatted.append(
                                            {
                                                "ba_name": self.options["bal_auth"],
                                                "timestamp": j[0],
                                                # "freq": need to add this
                                                data_type: j[1],
                                                "market": market
                                            }
                                        )
                if self.options["data"] == "gen":
                    for i in data_formatted:
                        i["fuel_name"] = "other"
            except:
                print("problematic area: ", data["request"])
                print(self.options)
        return data_formatted

    def handle_timezone(self, ba):
        timezones = {
            "AEC": 'Central',
            "AECI": 'Central',
            "AESO": '', #alberta
            "AVA": 'Pacific',
            "AZPS": 'Arizona',
            "BANC": 'Pacific',
            "BCTC": '', #britain
            "BPAT": 'Pacific',
            "CFE": '', #mexico
            "CHPD": 'Pacific',
            "CISO": 'Pacific',
            "CPLE": 'Eastern',
            "CPLW": 'Eastern',
            "DEAA": 'Arizona',
            "DOPD": 'Pacific',
            "DUK": 'Eastern',
            "EEI": 'Central',
            "EPE": 'Arizona',
            "ERCO": 'Central',
            "FMPP": 'Eastern',
            "FPC": 'Eastern',
            "FPL": 'Eastern',
            "GCPD": 'Pacific',
            "GRID": 'Pacific',
            "GRIF": 'Arizona',
            "GRMA": 'Arizona',
            "GVL": 'Eastern',
            "GWA": 'Mountain',
            "HGMA": 'Arizona',
            "HQT": '', #Quebec
            "HST": 'Eastern',
            "IESO": '', #ontario
            "IID": 'Pacific',
            "IPCO": 'Pacific',
            "ISNE": 'Eastern',
            "JEA": 'Eastern',
            "LDWP": 'Pacific',
            "LGEE": 'Central Standard', #huh?
            "MHEB": '', #manitoba
            "MISO": 'Eastern Standard',
            "NBSO": '', #New Brunswick
            "NEVP": 'Pacific',
            "NSB": 'Eastern',
            "NWMT": 'Mountain',
            "NYIS": 'Eastern',
            "OVEC": 'Eastern',
            "PACE": 'Mountain',
            "PACW": 'Pacific',
            "PGE": 'Pacific',
            "PJM": 'Eastern',
            "PNM": 'Arizona',
            "PSCO": 'Mountain',
            "PSEI": 'Pacific',
            "SC": 'Eastern',
            "SCEG": 'Eastern',
            "SCL": 'Pacific',
            "SEC": 'Eastern',
            "SEPA": 'Eastern',
            "SOCO": 'Central',
            "SPA": 'Central',
            "SPC": '',#saskatchewan
            "SRP": 'Arizona',
            "SWPP": 'Central',
            "TAL": 'Eastern',
            "TEC": 'Eastern',
            "TEPC": 'Arizona',
            "TIDC": 'Pacific',
            "TPWR": 'Pacific',
            "TVA": 'Central',
            "WACM": 'Arizona',
            "WALC": 'Arizona',
            "WAUW": 'Mountain',
            "WWA": 'Mountain',
            "YAD": 'Eastern'
            }
        return timezones[ba]

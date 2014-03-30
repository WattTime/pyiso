import logging
from collections import namedtuple


# named tuple for time period interval labels
IntervalChoices = namedtuple('IntervalChoices', ['hourly', 'fivemin', 'tenmin', 'na'])

# list of fuel choices
FUEL_CHOICES = ['biogas', 'biomass', 'coal', 'geo', 'hydro',
                'natgas', 'nonwind', 'nuclear', 'oil', 'other',
                'refuse', 'renewable', 'smhydro', 'solar', 'solarpv',
                'solarth', 'thermal', 'wind']


class BaseClient:
    """
    Base class for scraper/parser clients.
    """
    # logger
    logger = logging.getLogger(__name__)

    # choices for market and frequency interval labels
    MARKET_CHOICES = IntervalChoices(hourly='RTHR', fivemin='RT5M', tenmin='RT5M', na='RT5M')
    FREQUENCY_CHOICES = IntervalChoices(hourly='1hr', fivemin='5m', tenmin='10m', na='n/a')
        
    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        """
        Scrape and parse generation fuel mix data.

        :param bool latest: If True, only get the generation mix at the one most recent available time point.
           Available for all regions.
        :param bool yesterday: If True, get the generation mix for every time point yesterday.
           Not available for all regions.
        :param datetime start_at: A timezone-aware datetime. The timestamp of all returned data points will be greater than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :param datetime end_at: A timezone-aware datetime. The timestamp of all returned data points will be less than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
           Timestamps are in UTC.
        :rtype: list 

        """
        raise NotImplementedError('Derived classes must implement the get_generation method.')

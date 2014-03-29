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
        
    def get_generation(self, **kwargs):
        """Scrape and parse generation fuel mix data."""
        raise NotImplementedError('Derived classes must implement the get_generation method.')

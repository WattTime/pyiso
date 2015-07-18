from pyiso.base import BaseClient


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

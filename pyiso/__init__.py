import imp
import logging
import os.path
import sys
from os import environ

__version__ = '0.3.19'


# ERROR = 40, WARNING = 30, INFO = 20, DEBUG = 10
LOG_LEVEL = int(environ.get('LOG_LEVEL', logging.INFO))


# logger: create here to only add the handler once!
LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
LOGGER.addHandler(handler)
LOGGER.setLevel(LOG_LEVEL)

BALANCING_AUTHORITIES = {
    'AESO': {'class': 'AESOClient', 'module': 'aeso'},
    'AZPS': {'class': 'SVERIClient', 'module': 'sveri'},
    'BPA': {'class': 'BPAClient', 'module': 'bpa'},
    'CAISO': {'class': 'CAISOClient', 'module': 'caiso'},
    'DEAA': {'class': 'SVERIClient', 'module': 'sveri'},
    'EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
    'ELE': {'class': 'SVERIClient', 'module': 'sveri'},
    'ERCOT': {'class': 'ERCOTClient', 'module': 'ercot'},
    'EU': {'class': 'EUClient', 'module': 'eu'},
    'GRIF': {'class': 'SVERIClient', 'module': 'sveri'},
    'HGMA': {'class': 'SVERIClient', 'module': 'sveri'},
    'IID': {'class': 'SVERIClient', 'module': 'sveri'},
    'ISONE': {'class': 'ISONEClient', 'module': 'isone'},
    'MISO': {'class': 'MISOClient', 'module': 'miso'},
    'NBP': {'class': 'NBPowerClient', 'module': 'nbpower'},
    'NEVP': {'class': 'NVEnergyClient', 'module': 'nvenergy'},
    'NYISO': {'class': 'NYISOClient', 'module': 'nyiso'},
    'PJM': {'class': 'PJMClient', 'module': 'pjm'},
    'PNM': {'class': 'SVERIClient', 'module': 'sveri'},
    'SASK': {'class': 'SaskPowerClient', 'module': 'sask'},
    'SPP': {'class': 'SPPClient', 'module': 'spp'},
    'SPPC': {'class': 'NVEnergyClient', 'module': 'nvenergy'},
    'SRP': {'class': 'SVERIClient', 'module': 'sveri'},
    'TEPC': {'class': 'SVERIClient', 'module': 'sveri'},
    'WALC': {'class': 'SVERIClient', 'module': 'sveri'},
}


def client_factory(client_name, **kwargs):
    """Return a client for an external data set"""
    # set up
    dir_name = os.path.dirname(os.path.abspath(__file__))
    error_msg = 'No client found for name %s' % client_name
    client_key = client_name.upper()

    # find client
    try:
        client_vals = BALANCING_AUTHORITIES[client_key]
        module_name = client_vals['module']

        class_name = client_vals['class']
    except KeyError:
        raise ValueError(error_msg)

    # find module
    try:
        fp, pathname, description = imp.find_module(module_name, [dir_name])
    except ImportError:
        raise ValueError(error_msg)

    # load
    try:
        mod = imp.load_module(module_name, fp, pathname, description)
    finally:
        # Since we may exit via an exception, close fp explicitly.
        if fp:
            fp.close()

    # instantiate class
    try:
        client_inst = getattr(mod, class_name)(**kwargs)
    except AttributeError:
        raise ValueError(error_msg)

    # set name
    client_inst.NAME = client_name

    return client_inst

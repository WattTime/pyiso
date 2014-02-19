from isone import ISONEClient
from miso import MISOClient
from spp import SPPClient

def client_factory(client_name, **kwargs):
    """Return a client for an external data set"""
    if client_name == 'ISNE':
        return ISONEClient(**kwargs)
    if client_name == 'MISO':
        return MISOClient(**kwargs)
    if client_name == 'SPS':
        return SPPClient(**kwargs)
    else:
        raise ValueError('No client found for name %s' % client_name)
        
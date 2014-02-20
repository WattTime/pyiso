from isone import ISONEClient
from miso import MISOClient
from spp import SPPClient
from bpa import BPAClient


def client_factory(client_name, **kwargs):
    """Return a client for an external data set"""
    if client_name == 'ISONE':
        return ISONEClient(**kwargs)
    if client_name == 'MISO':
        return MISOClient(**kwargs)
    if client_name == 'SPP':
        return SPPClient(**kwargs)
    if client_name == 'BPA':
        return BPAClient(**kwargs)
    else:
        raise ValueError('No client found for name %s' % client_name)
        
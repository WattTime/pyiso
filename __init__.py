from isone import ISONEClient

def client_factory(client_name, **kwargs):
    """Return a client for an external data set"""
    if client_name == 'ISNE':
        return ISONEClient(**kwargs)
    else:
        raise ValueError('No client found for name %s' % client_name)
        
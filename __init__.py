import imp
import os.path

def client_factory(client_name, **kwargs):
    """Return a client for an external data set"""
    # set up
    module_name = client_name.lower()
    class_name = '%sClient' % client_name.upper()
    dir_name = os.path.dirname(os.path.abspath(__file__))
    
    # find module
    fp, pathname, description = imp.find_module(module_name, [dir_name])

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
        raise ValueError('No client found for name %s' % client_name)
        
    return client_inst
from __future__ import absolute_import
from celery import shared_task
from pyiso import client_factory
import logging
from datetime import datetime


# set up logger
logger = logging.getLogger(__name__)

@shared_task
def get_generation(ba_name, **kwargs):
    # get data
    c = client_factory(ba_name)
    data = c.get_generation(**kwargs)
    
    # log
    if len(data) == 0:
        msg = 'No data in %s at %s with args %s' % (ba_name, datetime.utcnow().isoformat(),
                                                    kwargs)
        logger.warn(msg)
    
    # return
    return data

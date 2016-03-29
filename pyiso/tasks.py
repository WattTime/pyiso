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
        msg = '%s: No generation data at %s with args %s' % (ba_name, datetime.utcnow().isoformat(),
                                                    kwargs)
        logger.warn(msg)

    # return
    return data

@shared_task
def get_load(ba_name, **kwargs):
    # get data
    c = client_factory(ba_name)
    data = c.get_load(**kwargs)

    # log
    if len(data) == 0:
        msg = '%s: No load data at %s with args %s' % (ba_name, datetime.utcnow().isoformat(),
                                                    kwargs)
        logger.warn(msg)

    # return
    return data


@shared_task
def get_trade(ba_name, **kwargs):
    # get data
    c = client_factory(ba_name)
    data = c.get_trade(**kwargs)

    # log
    if len(data) == 0:
        msg = '%s: No trade data at %s with args %s' % (ba_name, datetime.utcnow().isoformat(),
                                                    kwargs)
        logger.warn(msg)

    # return
    return data

@shared_task
def get_lmp(ba_name, node_list, **kwargs):
    # get data
    c = client_factory(ba_name)
    data = c.get_lmp(node_list, **kwargs)

    # log
    if len(data) == 0:
        msg = '%s: No lmp data at %s with args %s' % (ba_name, datetime.utcnow().isoformat(),
                                                    kwargs)
        logger.warn(msg)

    # return
    return data



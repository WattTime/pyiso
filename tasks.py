from __future__ import absolute_import
from celery import shared_task
from apps.clients import client_factory

@shared_task
def get_generation(ba_name, **kwargs):
    # get data
    c = client_factory(ba_name)
    data = c.get_generation(**kwargs)
    return data

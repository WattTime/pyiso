import logging


class BaseClient:
    logger = logging.getLogger(__name__)
        
    def get_generation(self, **kwargs):
        """Scrape and parse generation fuel mix data."""
        raise NotImplementedError('Derived classes must implement the get_generation method.')
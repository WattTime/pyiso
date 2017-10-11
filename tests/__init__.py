import os


def read_fixture(ba_name, filename):
    """
    :param str ba_name: The balancing authority name.
    :param str filename: The fixture file you wish to load.
    :return: The file's content.
    :rtype: str
    """
    fixtures_base_path = os.path.join(os.path.dirname(__file__), './fixtures', ba_name.lower())
    return open(os.path.join(fixtures_base_path, filename), 'r').read()

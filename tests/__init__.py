import os


def read_fixture(ba_name, filename):
    """
    :param str ba_name: The balancing authority name.
    :param str filename: The fixture file you wish to load.
    :return: The file's content.
    :rtype: str
    """
    fixtures_base_path = os.path.join(os.path.dirname(__file__), './fixtures', ba_name.lower())
    fixture_file = open(os.path.join(fixtures_base_path, filename), 'r')
    data = fixture_file.read()
    if hasattr(data, 'decode'):
        data = getattr(data, 'decode')(encoding='utf-8', errors='replace')
    fixture_file.close()
    return data

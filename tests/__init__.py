import os


def fixture_path(ba_name, filename):
    """
    :param str ba_name: The balancing authority module name.
    :param str filename: The fixture file you wish to find the path of.
    :return: The full path to the test file within the fixtures directory, regardless of working directory.
    :rtype: str
    """
    fixtures_base_path = os.path.join(os.path.dirname(__file__), './fixtures', ba_name.lower())
    return os.path.join(fixtures_base_path, filename)


def read_fixture(ba_name, filename, as_bytes=False):
    """
    :param str ba_name: The balancing authority module name.
    :param str filename: The fixture file you wish to load.
    :param bool as_bytes: Indicates whether file should open with 'read bytes' mode.
    :return: The file's content.
    """
    file_path = fixture_path(ba_name=ba_name, filename=filename)
    mode = 'rb' if as_bytes else 'r'
    fixture_file = open(file_path, mode=mode)
    data = fixture_file.read()
    if hasattr(data, 'decode'):
        data = getattr(data, 'decode')(encoding='utf-8', errors='replace')
    fixture_file.close()
    return data

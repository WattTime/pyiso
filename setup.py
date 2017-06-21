from setuptools import setup
import codecs
import os
import re

# to release:
# python setup.py register sdist bdist_egg upload


here = os.path.abspath(os.path.dirname(__file__))


# Read the version number from a source file.
# Why read it, and not import?
# see https://groups.google.com/d/topic/pypa-dev/0PkjVpcxTzQ/discussion
# https://github.com/pypa/sampleproject/blob/master/setup.py
def find_version(*file_paths):
    # Open in Latin-1 so that we avoid encoding errors.
    # Use codecs.open for Python 2 compatibility
    with codecs.open(os.path.join(here, *file_paths), 'r', 'latin1') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


# Get the long description from the relevant file
with codecs.open('README.md', encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='pyiso',
    packages=['pyiso'],
    version=find_version('pyiso', '__init__.py'),
    description='Python client libraries for ISO and other power grid data sources.',
    long_description=long_description,
    author='Anna Schneider',
    author_email='anna@watttime.org',
    url='https://github.com/WattTime/pyiso',
    license='Apache',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    test_suite='nose.collector',
    install_requires=[
        'beautifulsoup4==4.5.0',
        'pandas>=0.18',
        'python-dateutil',
        'pytz',
        'requests',
        'celery>=3.1',
        'xlrd',
        'lxml==3.6.1',
        'html5lib',
        'mock',
    ],
)

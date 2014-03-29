from setuptools import setup
import codecs


# Get the long description from the relevant file
with codecs.open('README.md', encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='grid-clients',
    packages=['grid_clients'],
    version='0.1',
    description='Client libraries for power grid data sources.',
    long_description=long_description,
    author='Anna Schneider',
    author_email='anna@watttime.org',
    url='https://github.com/WattTime/grid-clients',
    license='Apache',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
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
    test_suite = 'nose.collector',
    install_requires=[
        'beautifulsoup4',
        'pandas>=0.12',
        'python-dateutil',
        'pytz',
        'requests',
        'xlrd',
    ],
)
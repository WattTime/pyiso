Configuration
=============


Accounts
--------

ISONE requires a username and password to collect data.
You can register for an ISONE account here (http://www.iso-ne.com/participate/applications-status-changes/access-software-systems#data-feeds)


Then, set your usernames and passwords as environment variables:

    export ISONE_USERNAME=myusername1
    export ISONE_PASSWORD=mysecret1

The EU (ENTSOe) REST API requires a security token.  You must first sign up for an account and then get your security token from here (https://transparency.entsoe.eu/).  To use the token set as an environment variable as follows:

    export ENTSOe_SECURITY_TOKEN=token

The EIA API requires an API key. You can apply for a key here (https://www.eia.gov/opendata/register.cfm). To use the key, set an environment variable as follows:

    export EIA_KEY=my-eia-api-key

All other ISOs allow unauthenticated users to collect data, so no other credentials are needed.


Logging and debug
-----------------

By default, logging occurs at the INFO level. If you want to change this, you can set the `LOG_LEVEL` environment variable to the `integer associated with the desired log level <https://docs.python.org/2/library/logging.html#logging-levels>`_. For instance, ERROR is 40 and DEBUG is 10.

You can also turn on DEBUG level logging by setting the `DEBUG` environment variable to a truthy value. This setting will additionally enable caching during testing, which will significantly speed up the test suite.

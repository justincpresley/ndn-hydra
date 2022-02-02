Configuration
=============

Repo Prefix Strategy
--------------------

Before running Hydra on any node, we must register the repo prefix as multicast.
You can do that by doing the following..

.. code-block:: bash

    $ nfdc strategy set /<repo-prefix>/group /localhost/nfd/strategy/multicast

More info on different strategies found `here <https://named-data.net/doc/NFD/current/manpages/nfdc-strategy.html>`_.

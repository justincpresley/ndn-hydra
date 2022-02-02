General Use
===========

Python Importing
----------------

There are two packages that are joined together into the pip library ``ndn-hydra``.
    * **- ndn_hydra.client**
    * **- ndn_hydra.repo**

A general pip import would look like this:

.. code-block:: python

    from ndn_hydra.client import *
    from ndn_hydra.repo import *

or simply:

.. code-block:: python

    from ndn_hydra import *


Command-Line
------------

The pip package automatically installs two command-line aliases for your convenience.
    * **- ndn-hydra-client**
    * **- ndn-hydra-repo**

Each alias links back to it's corresponding package. Running each alias by itself will tell what arguments are missing.
(note: registering ``/<repo-prefix>/group`` as multicast is still needed!)

An example of running one alias is seen below.

.. code-block:: bash

    ndn-hydra-repo -rp "/hydra" -n "node_05"


Identity / Keys
---------------

A NDN Hydra node will require a identity (along with a key) that is the same as the ``node_name``.
For now however, the default localhost/default key that NFD produces automatically is required in order
to have a successful run. Deleting ``~/.ndn`` or the default keys will result in the inability to run
a hydra node.

Databases
=========

Storage
-------

A Hydra node keeps 3 databases in order to function properly. All databases are stored under the directory ``~/.ndn/repo<repo_prefix>/<session_id>``.

The databases:
    * **The SVS Database**
    * **The Global View Database**
    * **The Files Database**


The SVS Database
----------------

This database holds all data that is published using the SVS protocol.
The data is group-message tlvs that are used when other Hydra nodes want to see the published information from this node.


The Global View Database
------------------------

This database holds all information regarding ``hydra``. This includes ALL information on any files within the system
and any sids within the system. This database is used when receiving a query and used when deciding which sids has what files
or who will get what files.


The Files Database
------------------

This database holds all files that this node becomes responsible for within ``hydra``. This database is used to serve files
to any clients it comes into contact with.
Examples
========

For the basic example, we will be having all nodes fall under one NFD.
This means we can eliminate routing for a quick run.

**1. Edit NFD config to see things happen**

.. code-block:: bash

    cs_max_packets 0

**2. Start / Restart NFD**

**3. Configure Repo Prefix Strategy**

.. code-block:: bash

    $ nfdc strategy set /hydra/group /localhost/nfd/strategy/multicast

**4. Run the following on 4 or more terminals.**

.. code-block:: bash

    $ cd ndn-hydra
    $ python3 ./examples/repo.py -rp /hydra -n <node-name>

- *node_name* : A unique, per node, name. Remains constant through restarts. (example: /node1)

**5. On a seperate terminal, run all client interactions**

Client needs to also be in the root directory :literal:`cd ndn-hydra`.
Running :literal:`python3 ./examples/client.py` will help you see all choices you have.

* Insertion

.. code-block:: bash

    $ python3 ./examples/client.py insert -r /hydra -f /home/a.txt -p ./examples/files/10mb.txt

* Query

.. code-block:: bash

    $ python3 ./examples/client.py query -r /hydra -q /files

* Fetch

.. code-block:: bash

    $ python3 ./examples/client.py fetch -r /hydra -f /home/a.txt -p ./examples/output/10mb.txt

* Deletion

.. code-block:: bash

    $ python3 ./examples/client.py delete -r /hydra -f /home/a.txt

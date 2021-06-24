# ndn-distributed-repo

## Dependencies

```
pip install python-ndn ndn-svs ndn-python-repo
```

## Run

1. add the root folder of this repo to PYTHONPATH environment variable
2. register the group prefix `/<hydra-prefix>/group` as multi-cast 
```bash
nfdc strategy set /<hydra-prefix>/group /localhost/nfd/strategy/multicast/%FD%03
```

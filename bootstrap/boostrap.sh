#!/bin/bash

# install customized ndncert
# mkdir dep && cd mkdir dep
# git clone https://github.com/tianyuan129/ndncert.git
# cd ndncert && git checkout v0.3
# ./waf configure
# sudo ./waf install && cd ..

help()
{
   echo "Usage: $0 -c ssl_cert -p ssl_prv -d name_component"
   echo -c "\t SSL Certificate"
   echo -p "\t Private key for the supplied SSL Certificate"
   exit 1 # Exit script after printing help
}

python3 bootstrap/json-writter.py
sudo cp bootstrap/ndncert-client.conf /usr/local/etc/ndncert/client.conf

while getopts c:p: flag
do
    case "${flag}" in
        c) ssl_cert=${OPTARG};;
        p) ssl_prv=${OPTARG};;
        ?) help;;
    esac
done

if [ -c "$ssl_cert" ] || [ -p "$ssl_prv" ]
then
   help
   exit 1
fi

python3 bootstrap/auto.py -c $ssl_cert -p $ssl_prv
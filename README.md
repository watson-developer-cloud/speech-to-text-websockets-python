

## Synopsis

This project consists of a python client that interacts with the IBM Watson Speech To Text service through its WebSockets interface. The client streams audio to the STT service and receives recognition hypotheses in real time. It can run N simultaneous recognition sessions

## installation

There are some dependencies that need to be installed for this script to work. In order to interact with the STT service via WebSockets it is necessary to install the 'twisted' and 'autobahn' libraries. An updated version of these libraries can be installed by typing:

`
$ pip install twisted
`

`
$ pip install autobahn
`

In order to you token based authentication it is necessary to install the requests library

`
$ pip install requests
`

If you need to upgrade your existing versions of twisted or autobhan you can type

`
$ pip install twisted --upgrade
`

`
$ pip install autobahn --upgrade
`

Sometimes you may need to install some additional dependencies, check the following commands:

`
$ pip install pyOpenSSL
`

`
$ apt-get install build-essential python-dev
`

Finally, the version 0.10.3 of Autobahn comes with a bug/typo that you need to fix by changing 'taxio' to 'txaio' in /usr/local/lib/python2.7/dist-packages/autobahn/websocket/protocol.py

## Examples                                                                                                                                        
                                                                                                                                                    
The example below will run the default 10 WAV files through the WebSockets interface of the Speech To Text (STT) service and will dump the recognition hypotheses to a file under the "./output" directory.                           
                                                                                                                                                    
`                                                                                                                                                   
$ python ./sttClient.py -credentials <username>:<password> -model en-US_BroadbandModel
`                                                                                                                                                   
                                                                                                                                                    
The example below performs the same task much faster by opening 10 simultaneous recognition sessions (WebSocket connections) against the STT service.
                                                                                                                                                    
`                                                                                                                                                   
$ python ./sttClient.py -credentials <username>:<password> -model en-US_BroadbandModel -threads 10
`                                                                                                                                                   
 
## Options

To see the list of available options type:

`
$ python sttClient.py -h
`

## Motivation

This script has been created by Daniel Bolanos in order to facilitate and promote the utilization of the IBM Watson Speech To Text service.



                                                              


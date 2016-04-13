

## Synopsis

This project consists of a python client that interacts with the IBM Watson Speech To Text service through its WebSockets interface. The client streams audio to the STT service and receives recognition hypotheses in real time. It can run N simultaneous recognition sessions

## Installation

There are some dependencies that need to be installed for this script to work. It is advisable to install the required packages in a separate virtual environment. Certain packages have been observed to conflict with the package requirements for this script; in particular the package nose conflicts with these required packages. In order to interact with the STT service via WebSockets, it is necessary to install [pip](https://pip.readthedocs.org/en/1.1/installing.html), then write the following commands:

`
pip install -r requirements.txt
`

You also may need to write this command

`
$ apt-get install build-essential python-dev
`

If you are creating an environment using anaconda, proceed with the above pip command to install the packages--do not use conda to install the requirements as conda will install nose as a dependency.

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



                                                              


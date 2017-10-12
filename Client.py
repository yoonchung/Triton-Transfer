#!/usr/bin/env python

import sys

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from shared.ttypes import *
from metadataServer.ttypes import *
from blockServer.ttypes import *

# Add classes / functions as required here

if __name__ == "__main__":

    if len(sys.argv) < 5:
        print "Invocation : <executable> <config_file> <base_dir> <command> <filename>"
        exit(-1)


    print "Starting client"

    '''
    Server information can be parsed from the config file

    connections can be created as follows

    Eg:
'''
    # Make socket
    transport = TSocket.TSocket('serverip', serverport)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = HelloService.Client(protocol)

    # Connect!
    try:
        transport.open()
    except Exception as e:
        print "Error while opening socket to server\n", e
        exit(1)

    # Create custom data structure object
    m = message()

    # Fill in data
    m.data = "Hello From Client!!!"

    # Call via RPC
    try:
        dataFromServer = client.HelloServiceMethod(m)
    except Exception as e:
        print "Caught an exception while calling RPC"
        # Add handling code
        exit(1)


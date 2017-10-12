#!/usr/bin/env python

import sys

sys.path.append('gen-py')

from blockServer import BlockServerService
from blockServer.ttypes import *
from shared.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class BlockServerHandler():

    def __init__(self, configpath):
        # Initialize using config file, intitalize state etc
        pass

    def storeBlock(self, hashBlock):
        # Store hash block, called by client during upload
        pass

    def getBlock(self, hash):
        # Retrieve block using hash, called by client during download
        pass

    def deleteBlock(self, hash):
        # Delete the particular hash : block pair
        pass

    def readServerPort(self):
        # In this function read the configuration file and get the port number for the server
        pass

    # Add your functions here

# Add additional classes and functions here if needed


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print "Invocation <executable> <config_file>"
        exit(-1)

    config_path = sys.argv[1]

    print "Initializing block server"
    handler = BlockServerHandler(config_path)
    # Retrieve the port number from the config file so that you could strt the server
    port = handler.readServerPort()
    # Define parameters for thrift server
    processor = BlockServerService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    # Create a server object
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print "Starting server on port : ", port

    try:
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print "\nExecption / Keyboard interrupt occured: ", e
        exit(0)

#!/bin/bash

# This script is meant to start your metadata server

# Check the parameters provided
if [ "$#" -ne 2 ]
then
	echo "Wrong arguments, usage :"
	echo "./runMetaServer.sh <config_file> <id>"
	exit
fi

# First parameter is the path for the config_file while second one is the ID
# ID field says whether its metadata server 1, 2 or n
CONFIGFILE_PATH=$1
ID=$2

# Now call your metadata server with the above arguments
# Eg. If you are using python and your metaServer is MetaServer.py
# Call
#
# python MetaServer.py $CONFIGFILE_PATH $ID
#
# or if CPP
#
# ./metaServer $CONFIGFILE_PATH $ID
java -cp /usr/share/java/slf4j/slf4j-api.jar:/usr/share/java/slf4j/slf4j-simple.jar:/usr/share/java/libthrift.jar:MetaServer.jar metadataServer.MetadataServerServer $CONFIGFILE_PATH $ID
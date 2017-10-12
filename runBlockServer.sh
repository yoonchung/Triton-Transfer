#!/bin/bash

# This script is meant to start your metadata server

# Check the parameters provided
if [ "$#" -ne 1 ]
then
	echo "Wrong arguments, usage :"
	echo "./runBlockServer.sh <config_file>"
	exit
fi


CONFIGFILE_PATH=$1

# Now call your metadata server with the above arguments
# Eg. If you are using python and your blockServer is BlockServer.py
# Call
#
# python BlockServer.py $CONFIGFILE_PATH
#
# or if CPP
#
# ./blockServer $CONFIGFILE_PATH
java -cp /usr/share/java/slf4j/slf4j-api.jar:/usr/share/java/slf4j/slf4j-simple.jar:/usr/share/java/libthrift.jar:BlockServer.jar blockServer.BlockServerServer $CONFIGFILE_PATH

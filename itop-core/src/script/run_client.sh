#!/bin/bash

CLASSPATH=../lib/*
CLASSPATH=$CLASSPATH:../conf

DEBUG=" -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=8990,suspend=y"

if [ "$1" == "-debug" ]; then
        PARAMS=$PARAMS$DEBUG
fi

java $PARAMS -cp $CLASSPATH edu.clarkson.cs.itop.core.RunClient $@
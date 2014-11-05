#!/bin/sh


CLASSPATH=../lib/*
CLASSPATH=$CLASSPATH:../config

java -cp $CLASSPATH edu.clarkson.cs.itop.core.RunWorker

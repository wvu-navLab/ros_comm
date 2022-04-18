#!/bin/bash

source user-variables.sh

for pid in `cat $SCRIPT_PATH/test-pids.txt`
do kill $pid
done
rm $SCRIPT_PATH/test-pids.txt

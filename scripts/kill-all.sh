#!/bin/bash

source $SCRIPT_PATH/user-variables.sh

for pid in `cat $SCRIPT_PATH/test-pids.txt`
do kill $pid
done
rm $SCRIPT_PATH/test-pids.txt

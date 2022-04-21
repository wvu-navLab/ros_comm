#!/bin/bash

if [ $# -lt 2 ]
then
echo "Usage: $0 <package name> <launch file name>"
exit 1
fi

PACKAGE_NAME=$1
LAUNCH_NAME=$2

source user-variables.sh
source $ROS_BASE/devel/setup.bash
export ROS_MASTER_URI=http://$LOCAL_IP:11311; export ROS_HOSTNAME=$LOCAL_IP

source $SCRIPT_PATH/amishare-ros-variables.sh
export FIFO_PATH=/tmp/com_kinnami_amishare_BoxAFSNotify_AmiShareFS-$USER
export REG_FILE=$AMISHARE_PREFIX/registration
touch $REG_FILE

roscore -r $REG_FILE > $OUTPUT_PATH/roscoreout.txt 2>&1 &
echo $! > $SCRIPT_PATH/test-pids.txt
sleep 1

python3 $AMINOTIFY $FIFO_PATH $SOCKET_PATH $REG_FILE $AMISHARE_PREFIX > $OUTPUT_PATH/aminotifyout.txt 2>&1 &
echo $! >> $SCRIPT_PATH/test-pids.txt
sleep 1

roslaunch $PACKAGE_NAME ${LAUNCH_NAME}.launch > $OUTPUT_PATH/${LAUNCH_NAME}out.txt 2>&1 &
echo $! >> $SCRIPT_PATH/test-pids.txt



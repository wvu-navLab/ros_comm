#!/bin/bash

source $SCRIPT_PATH/user-variables.sh
source $ROS_BASE/devel/setup.bash

#export CATKIN_PATH=~/catkin_ws
#cd $CATKIN_PATH 
#source devel/setup.bash

if [ $# -lt 1 ]
then echo "Usage: $0 <package_name> <node_name> [<node_name> ...]"
exit 1
fi

PACKAGE_NAME=$1
ARGS_LIST=$*

export ROS_MASTER_URI=http://$LOCAL_IP:11311; export ROS_HOSTNAME=$LOCAL_IP

source $SCRIPT_PATH/amishare-ros-variables.sh
export FIFO_PATH=/tmp/com_kinnami_amishare_BoxAFSNotify_AmiShareFS-$USER
export REG_FILE=$AMISHARE_PREFIX/registration
touch $REG_FILE

roscore -r $REG_FILE > $OUTPUT_PATH/roscoreout.txt 2>&1 &
echo $! > $SCRIPT_PATH/test-pids.txt
sleep 1 # roscore needs a moment to start up

python3 $AMINOTIFY $FIFO_PATH $SOCKET_PATH $REG_FILE $AMISHARE_PREFIX > $OUTPUT_PATH/aminotifyout.txt 2>&1 &
echo $! >> $SCRIPT_PATH/test-pids.txt
sleep 1

for name in $ARGS_LIST
do if [ $name != $PACKAGE_NAME ]
then
stdbuf --output=L rosrun $PACKAGE_NAME $name > $OUTPUT_PATH/${name}out.txt 2>&1 &
export TEST_PID=$! 
sleep 1 
ps -e -o ppid,pid | awk -v pid=$TEST_PID '$1==pid{print $2}' >> $SCRIPT_PATH/test-pids.txt
fi
done


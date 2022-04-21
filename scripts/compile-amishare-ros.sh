#!/bin/bash

source user-variables.sh

echo "export SOCKET_PATH=/tmp/com_kinnami_amishare_aminotify-$USER" > $SCRIPT_PATH/amishare-ros-variables.sh
echo "export AMISHARE_PREFIX=/media/AmiShareFS-$USER" >> $SCRIPT_PATH/amishare-ros-variables.sh
source $SCRIPT_PATH/amishare-ros-variables.sh

cd $ROS_BASE/src/ros_comm/roscpp
sed "s|CMAKE_AMISHARE_ROS_PATH|\"${AMISHARE_PREFIX}\"|" CMakeLists.txt.temp > CMakeLists.txt
sed -i "s|CMAKE_FIFO_PATH|\"${SOCKET_PATH}\"|" CMakeLists.txt
sed -i 's/CMAKE_AMISHARE_ROS/1/' CMakeLists.txt

catkin build roscpp

#sed "s/CMAKE_AMISHARE_ROS_PATH//" CMakeLists.txt.temp > CMakeLists.txt
#sed -i "s/CMAKE_FIFO_PATH//" CMakeLists.txt
#sed -i 's/CMAKE_AMISHARE_ROS/0/' CMakeLists.txt

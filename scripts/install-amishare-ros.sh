#!/bin/bash

source user-variables.sh

mkdir $FORK_BASE
cd $FORK_BASE
git clone git@github.com:wvu-navLab/ros_comm.git
cp -r $FORK_BASE/ros_comm/clients/roscpp $ROS_BASE/src/ros_comm/
cp -r $FORK_BASE/ros_comm/tools/rosmaster $ROS_BASE/src/ros_comm/
cp -r $FORK_BASE/ros_comm/tools/roslaunch $ROS_BASE/src/ros_comm/
cp -r $FORK_BASE/ros_comm/tools/rosgraph $ROS_BASE/src/ros_comm/

cd $ROS_BASE
catkin build

git clone git@github.com:Kinnami/AmiNotify


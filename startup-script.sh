#!/bin/bash
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Increase fd limit
ulimit -n 32768
echo hadoop soft nofile 32768 >> /etc/security/limits.conf
echo hadoop hard nofile 32768 >> /etc/security/limits.conf

# Mount ephemeral disk
declare -r HADOOP_ROOT=/hadoop
declare -r DISK_DEVICE=/dev/disk/by-id/google-ephemeral-disk-0

mkdir $HADOOP_ROOT
/usr/share/google/safe_format_and_mount $DISK_DEVICE $HADOOP_ROOT

# Set up user and group
groupadd --gid 5555 hadoop
useradd --uid 1111 --gid hadoop --shell /bin/bash -m hadoop

# Prepare directories
mkdir $HADOOP_ROOT/hdfs
mkdir $HADOOP_ROOT/hdfs/name
mkdir $HADOOP_ROOT/hdfs/data
mkdir $HADOOP_ROOT/checkpoint
mkdir $HADOOP_ROOT/mapred
mkdir $HADOOP_ROOT/mapred/history

chown -R hadoop:hadoop $HADOOP_ROOT
chmod -R 755 $HADOOP_ROOT

mkdir /run/hadoop
chown hadoop:hadoop /run/hadoop
chmod g+w /run/hadoop

declare -r HADOOP_LOG_DIR=/var/log/hadoop
mkdir $HADOOP_LOG_DIR
chgrp hadoop $HADOOP_LOG_DIR
chmod g+w $HADOOP_LOG_DIR

# Error check in CreateTrackerDirIfNeeded() in gslib/util.py in gsutil 3.34
# (line 114) raises exception when called from Hadoop streaming MapReduce,
# saying permission error to create /homes.
perl -pi -e '$.>110 and $.<120 and s/raise$/pass/'  \
    /usr/local/share/google/gsutil/gslib/util.py

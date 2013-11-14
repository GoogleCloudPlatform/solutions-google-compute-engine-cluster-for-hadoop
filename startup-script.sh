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

function die() {
  # Message to STDERR goes to start-up script log in the instance.
  echo
  echo "########## ERROR ##########"
  echo "$@"
  echo "###########################"

  exit 1
}

declare -r METADATA_ROOT=http://metadata/computeMetadata/v1beta1

function metadata_url() {
  echo $METADATA_ROOT/instance/attributes/$1
}

function get_metadata_value() {
  name=$1
  echo $(curl --silent -f $(metadata_url $name))
}

NUM_WORKERS=$(get_metadata_value 'num-workers')
HADOOP_MASTER=$(get_metadata_value 'hadoop-master')
WORKER_NAME_TEMPLATE=$(get_metadata_value 'hadoop-worker-template')
TMP_CLOUD_STORAGE=$(get_metadata_value 'tmp-cloud-storage')
CUSTOM_COMMAND=$(get_metadata_value 'custom-command')

THIS_HOST=$(curl --silent -f  \
    $METADATA_ROOT/instance/network-interfaces/0/access-configs/0/external-ip)
if [[ ! "$THIS_HOST" ]] ; then
  THIS_HOST=$(hostname)
fi

# Set up routing on master on cluster with no external IP address on workers.
if (( ! $(get_metadata_value 'worker-external-ip') )) &&  \
    [[ "$(hostname)" == "$HADOOP_MASTER" ]] ; then
  echo "Setting up Hadoop master as Internet gateway for workers."
  # Turn on IP forwarding on kernel.
  perl -pi -e 's/#net.ipv4.ip_forward=1/net.ipv4.ip_forward=1/' /etc/sysctl.conf
  /sbin/sysctl -p
  # Set up NAT (IP masquerade) rule.
  iptables -t nat -A POSTROUTING -s 10.0.0.0/8 -j MASQUERADE
fi

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

declare -r TMP_DIR=/tmp/hadoop_package
declare -r HADOOP_DIR=hadoop-*
declare -r GENERATED_FILES_DIR=generated_files
declare -r DEB_PACKAGE_DIR=deb_packages

declare -r HADOOP_HOME=/home/hadoop
declare -r SCRIPT_DIR=hadoop_scripts

mkdir -p $TMP_DIR

# Set up SSH keys for hadoop user.
SSH_KEY_DIR=$HADOOP_HOME/.ssh
mkdir -p $SSH_KEY_DIR
curl -o $SSH_KEY_DIR/id_rsa $(metadata_url hadoop-private-key)
curl -o $SSH_KEY_DIR/authorized_keys $(metadata_url hadoop-public-key)

# Allow SSH between Hadoop cluster instances without user intervention.
SSH_CLIENT_CONFIG=$SSH_KEY_DIR/config
echo "Host *" >> $SSH_CLIENT_CONFIG
echo "  StrictHostKeyChecking no" >> $SSH_CLIENT_CONFIG

chown hadoop:hadoop -R $SSH_KEY_DIR
chmod 600 $SSH_KEY_DIR/id_rsa
chmod 700 $SSH_KEY_DIR
chmod 600 $SSH_CLIENT_CONFIG

# if [[ $HADOOP_MASTER == "$(hostname)" ]] ; then
  # Download packages from Cloud Storage.
  gsutil -m cp -R $TMP_CLOUD_STORAGE/$HADOOP_DIR.tar.gz  \
      $TMP_CLOUD_STORAGE/$DEB_PACKAGE_DIR  \
      $TMP_DIR ||  \
      die "Failed to download Hadoop and required packages from "  \
          "$TMP_CLOUD_STORAGE/"

# Set up Java Runtime Environment.
dpkg -i --force-depends $TMP_DIR/$DEB_PACKAGE_DIR/*.deb

SCRIPT_AS_HADOOP=$TMP_DIR/setup_as_hadoop.sh
cat > $SCRIPT_AS_HADOOP <<NEKO
# Exits if one of the commands fails.
set -o errexit

HADOOP_CONFIG_DIR=\$HOME/hadoop/conf

# Extract Hadoop package.
tar zxf $TMP_DIR/$HADOOP_DIR.tar.gz -C \$HOME
ln -s \$HOME/$HADOOP_DIR \$HOME/hadoop

# Create masters file.
echo $HADOOP_MASTER > \$HADOOP_CONFIG_DIR/masters

# Create slaves file.
rm -f \$HADOOP_CONFIG_DIR/slaves
for ((i = 0; i < $NUM_WORKERS; i++)) ; do
  printf "$WORKER_NAME_TEMPLATE\n" \$i >> \$HADOOP_CONFIG_DIR/slaves
done

# Overwrite Hadoop configuration files.
perl -pi -e "s/###HADOOP_MASTER###/$HADOOP_MASTER/g"  \
    \$HADOOP_CONFIG_DIR/core-site.xml  \
    \$HADOOP_CONFIG_DIR/mapred-site.xml

perl -pi -e "s/###EXTERNAL_IP_ADDRESS###/$THIS_HOST/g"  \
    \$HADOOP_CONFIG_DIR/hdfs-site.xml  \
    \$HADOOP_CONFIG_DIR/mapred-site.xml

# Set PATH for hadoop user
echo "export PATH=\$HOME/hadoop/bin:\$HOME/hadoop/sbin:\\\$PATH" >>  \
    \$HOME/.profile
echo "export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64" >> \$HOME/.profile

NEKO

sudo -u hadoop bash $SCRIPT_AS_HADOOP ||  \
    die "Failed to run set-up command as hadoop user"

# Error check in CreateTrackerDirIfNeeded() in gslib/util.py in gsutil 3.34
# (line 114) raises exception when called from Hadoop streaming MapReduce,
# saying permission error to create /homes.
perl -pi -e '$.>110 and $.<120 and s/raise$/pass/'  \
    /usr/local/share/google/gsutil/gslib/util.py

# Run custom commands.
eval "$CUSTOM_COMMAND" || die "Custom command error: $CUSTOM_COMMAND"

function run_as_hadoop() {
  failure_message=$1 ; shift

  sudo -u hadoop -i eval "$@" || die $failure_message
}

# Starts daemons if necessary.
function maybe_start_node() {
  condition=$1 ; shift
  failure_message=$1 ; shift

  if (( $(get_metadata_value $condition) )) ; then
    run_as_hadoop "$failure_message" $@
  fi
}

# Starts NameNode and Secondary NameNode.  Format HDFS if necessary.
function start_namenode() {
  echo "Prepare and start NameNode(s)"

  run_as_hadoop "Failed to format HDFS" "echo 'Y' | hadoop namenode -format"

  # Start NameNode
  run_as_hadoop "Failed to start NameNode" hadoop-daemon.sh start namenode
  # Start Secondary NameNode
  run_as_hadoop "Failed to start Secondary NameNode" hadoop-daemon.sh start  \
      secondarynamenode
}

if (( $(get_metadata_value NameNode) )) ; then
  start_namenode
fi

maybe_start_node DataNode "Failed to start DataNode"  \
    hadoop-daemon.sh start datanode

maybe_start_node JobTracker "Failed to start JobTracker"  \
    hadoop-daemon.sh start jobtracker

maybe_start_node TaskTracker "Failed to start TaskTracker"  \
    hadoop-daemon.sh start tasktracker

echo
echo "Start-up script for Hadoop finished."
echo

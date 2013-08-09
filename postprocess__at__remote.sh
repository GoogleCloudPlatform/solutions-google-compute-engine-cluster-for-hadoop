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

declare -r TMP_CLOUD_STORAGE=$1 ; shift
declare -r MASTER_NAME=$1 ; shift

declare -r TMP_DIR=/tmp/hadoop_package
declare -r HADOOP_DIR=hadoop-1.0.*
declare -r GENERATED_FILES_DIR=generated_files
declare -r HADOOP_HOME=/home/hadoop

declare -r DEB_PACKAGE_DIR=deb_packages
declare -r LOCAL_PACKAGE_DIR=$TMP_DIR/$DEB_PACKAGE_DIR

function die() {
  echo 1>&2
  echo "########## ERROR ##########" 1>&2
  echo $@ 1>&2
  echo "###########################" 1>&2
  echo 1>&2
  exit 1
}

mkdir -p $TMP_DIR

# Download packages from Cloud Storage
gsutil -m cp -R $TMP_CLOUD_STORAGE/$HADOOP_DIR.tar.gz  \
                $TMP_CLOUD_STORAGE/$GENERATED_FILES_DIR.tar.gz  \
                $TMP_CLOUD_STORAGE/$DEB_PACKAGE_DIR  \
                $TMP_DIR/ ||  \
    die "Failed to download Hadoop and generated files packages from "  \
        "$TMP_CLOUD_STORAGE/"

tar zxf $TMP_DIR/$GENERATED_FILES_DIR.tar.gz -C $TMP_DIR ||  \
    die "Failed to extract generated files"
chmod o+rx $TMP_DIR/$GENERATED_FILES_DIR
chmod o+r $TMP_DIR/$GENERATED_FILES_DIR/*
chmod o+x $TMP_DIR/$GENERATED_FILES_DIR/ssh-key
chmod o+r $TMP_DIR/$GENERATED_FILES_DIR/ssh-key/*

# Set up Java Runtime Environment
sudo dpkg -i --force-depends $LOCAL_PACKAGE_DIR/*.deb

# Set up hosts
sudo bash -c "cat $TMP_DIR/$GENERATED_FILES_DIR/hosts >> /etc/hosts"
sudo perl -pi -e 's/127\.0\.0\.1/127.0.0.1 localhost.localdomain/' /etc/hosts

SCRIPT_AS_HADOOP=$TMP_DIR/setup_as_hadoop.sh
cat > $SCRIPT_AS_HADOOP <<NEKO

# Set up SSH keys
mkdir -p $HADOOP_HOME/.ssh
cp -f $TMP_DIR/$GENERATED_FILES_DIR/ssh-key/* $HADOOP_HOME/.ssh
mv -f $HADOOP_HOME/.ssh/id_rsa.pub $HADOOP_HOME/.ssh/authorized_keys
chmod 600 $HADOOP_HOME/.ssh/id_rsa
chmod 700 $HADOOP_HOME/.ssh

# Allow SSH between Hadoop cluster instances without user intervention.
echo "Host *" >> \$HOME/.ssh/config
echo "  StrictHostKeyChecking no" >> \$HOME/.ssh/config
chmod 600 \$HOME/.ssh/config

tar zxf $TMP_DIR/$HADOOP_DIR.tar.gz -C $HADOOP_HOME
ln -s $HADOOP_HOME/$HADOOP_DIR $HADOOP_HOME/hadoop

# Overwrite masters, slaves files
cp -f $TMP_DIR/$GENERATED_FILES_DIR/masters $HADOOP_HOME/hadoop/conf/masters
cp -f $TMP_DIR/$GENERATED_FILES_DIR/slaves $HADOOP_HOME/hadoop/conf/slaves

# Overwrite Hadoop site.xml files.
perl -pi -e "s/###HADOOP_MASTER###/$MASTER_NAME/g"  \
    $HADOOP_HOME/hadoop/conf/core-site.xml  \
    $HADOOP_HOME/hadoop/conf/mapred-site.xml

# Get external IP address of the instance from Compute Engine metadata.
EXTERNAL_IP_ADDRESS=$(curl http://metadata.google.internal/0.1/meta-data/network 2>/dev/null | python -c "import json,sys ; print json.loads(sys.stdin.read())['networkInterface'][0]['accessConfiguration'][0]['externalIp']")

perl -pi -e "s/###EXTERNAL_IP_ADDRESS###/\$EXTERNAL_IP_ADDRESS/g"  \
    $HADOOP_HOME/hadoop/conf/mapred-site.xml  \
    $HADOOP_HOME/hadoop/conf/hdfs-site.xml

# Set PATH for hadoop user
echo "export PATH=$HADOOP_HOME/hadoop/bin:\$PATH" >> $HADOOP_HOME/.profile
echo "export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64" >> $HADOOP_HOME/.profile

NEKO

sudo sudo -u hadoop bash $SCRIPT_AS_HADOOP ||  \
    die "Failed to run set-up command as hadoop user"

rm -rf $TMP_DIR/$GENERATED_FILES_DIR/ssh-key

# Run custom commands.
eval "$@" || die "Custom command error: $@"

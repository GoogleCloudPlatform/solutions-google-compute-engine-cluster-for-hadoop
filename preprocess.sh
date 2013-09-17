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

declare -r LOCAL_TMP_DIR=$1 ; shift
declare -r PROJECT=$1 ; shift
declare -r TMP_CLOUD_STORAGE=$1 ; shift

declare -r HADOOP=hadoop-1.2.1
declare -r GENERATED_FILES_DIR=generated_files
declare -r KEY_DIR=$LOCAL_TMP_DIR/$GENERATED_FILES_DIR/ssh-key
declare -r DEB_PACKAGE_DIR=$LOCAL_TMP_DIR/deb_packages

function die() {
  echo 1>&2
  echo "########## ERROR ##########" 1>&2
  echo $@ 1>&2
  echo "###########################" 1>&2
  echo 1>&2
  exit 1
}

# Generage key pair
if [ -d $KEY_DIR ] ; then
  rm -rf $KEY_DIR
fi
mkdir -p $KEY_DIR
ssh-keygen -t rsa -P '' -f $KEY_DIR/id_rsa || die "Failed to create SSH key."

# Upload Hadoop package and JRE package to Google Cloud Storage
gsutil -m cp -R $LOCAL_TMP_DIR/$HADOOP.tar.gz $DEB_PACKAGE_DIR $TMP_CLOUD_STORAGE/ ||  \
    die "Failed to copy Hadoop and Java packages to "  \
        "Cloud Storage $TMP_CLOUD_STORAGE/"

# Set up firewalls to allow Hadoop monitor ports
function set_firewall() {
  local -r name=$1 ; shift
  local -r description=$1 ; shift
  local -r port=$1 ; shift

  if gcutil --project=$PROJECT getfirewall $name > /dev/null 2>&1 ; then
    :
  else
    gcutil --project=$PROJECT addfirewall $name  \
        --description="$description" --allowed="tcp:$port" ||  \
        die "Failed to create firewall rule $name for port $port "  \
            "\($description\)"
  fi
}

set_firewall hdfs-namenode "HDFS NameNode Web console" 50070
set_firewall hdfs-datanode "HDFS DataNode Web console" 50075
set_firewall hadoop-mr-jobtracker "Hadoop MapReduce JobTracker Web console"  \
    50030
set_firewall hadoop-mr-tasktracker "Hadoop MapReduce TaskTracker Web console"  \
    50060

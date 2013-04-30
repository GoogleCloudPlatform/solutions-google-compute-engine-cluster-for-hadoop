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

HADOOP_BIN=/home/hadoop/hadoop/bin

function output_message() {
  local filename=$1 ; shift

  echo -e "[$filename]\t$@"
}

# Avoid splitting filename containing whitespace.
IFS=$'\n'$'\t'

while read hdfs gcs ; do
  output_message $hdfs "Copy to: $gcs"
  command="$HADOOP_BIN/hadoop dfs -cat $hdfs | gsutil cp - $gcs"
  output_message $hdfs "COMMAND: $command"
  eval $command | while read message ; do
    output_message $hdfs $message
  done
  output_message $hdfs "Copy finished"
done

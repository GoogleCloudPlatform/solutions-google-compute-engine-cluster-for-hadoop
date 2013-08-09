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

declare -r PROJECT=$1 ; shift
declare -r ZONE=$1 ; shift
declare -r HOST=$1 ; shift
declare -r SCRIPT=$1 ; shift

function die() {
  echo 1>&2
  echo "########## ERROR ##########" 1>&2
  echo $@ 1>&2
  echo "###########################" 1>&2
  echo 1>&2
  exit 1
}

if [[ "$1" != "--" ]] ; then
  AS_USER=$1
fi
shift

gcutil --project=$PROJECT push --zone=$ZONE $HOST $(dirname $0)/$SCRIPT /tmp ||  \
    die "Failed to push script $SCRIPT to $HOST"

if [ -n "$AS_USER" ] ; then
  gcutil --project=$PROJECT ssh --zone=$ZONE $HOST sudo bash -c  \
      "\"chmod a+rx /tmp/$SCRIPT && ulimit -n 32768  \
      && sudo -u $AS_USER /tmp/$SCRIPT $@\""
else
  gcutil --project=$PROJECT ssh --zone=$ZONE $HOST /tmp/$SCRIPT $@
fi

exit $?

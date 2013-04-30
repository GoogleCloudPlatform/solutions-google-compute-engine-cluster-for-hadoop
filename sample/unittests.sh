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

function expect_equals() {
  expected=$1
  actual=$2
  if [[ $expected == $actual ]] ; then
    echo "OK"
  else
    echo "FAIL"
    echo "EXPECTED: " $expected
    echo "ACTUAL  : " $actual
  fi
}

SAMPLE_DIR=$(dirname $0)
UNITTEST_DATADIR=$SAMPLE_DIR/unittestdata

EXPECTED=$(cat $UNITTEST_DATADIR/mapper-expected.txt)
ACTUAL=$(cat $UNITTEST_DATADIR/mapper-input.txt | $SAMPLE_DIR/shortest-to-longest-mapper.pl)

expect_equals "$EXPECTED" "$ACTUAL"


EXPECTED=$(cat $UNITTEST_DATADIR/reducer-expected.txt)
ACTUAL=$(cat $UNITTEST_DATADIR/reducer-input.txt | $SAMPLE_DIR/shortest-to-longest-reducer.pl)

expect_equals "$EXPECTED" "$ACTUAL"

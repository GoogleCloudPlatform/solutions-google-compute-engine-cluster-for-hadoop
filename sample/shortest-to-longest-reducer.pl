#!/usr/bin/perl
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

# Reducer sample.
# The word is already sorted in the desirable order.
# The reducer counts the occurrence of each word and outputs the word
# and its occurrence.

sub output {
  my $k = @_[0];
  # Reducer input key consists of <word length>:<word>
  ($len, $word) = split /:/, $k;
  # Reducer output.
  print "$word\t$count\n";
}

$count = 1;
while (<>) {
  $key = (split /\t/)[0];
  if ($key eq $prev) {
    # The same key as the previous line indicates the repetition of the same
    # word.  Increment the counter.
    $count++;
  } else {
    # The different key indicates the next word.  Output the count of the
    # previous word.
    # Note in Hadoop streaming MapReduce, it's guaranteed that the entries
    # of the same key is handled by the same reducer.
    &output($prev) if defined $prev;
    $prev = $key;
    $count = 1;
  }
}
# Output the last word count.
&output($prev);

#!/usr/bin/perl -n
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

# Mapper sample.
# The mapper takes arbitrary text as input.
# With the corresponding reducer, the MapReduce task counts occurrence
# of the word in the original text.
# The output is sorted by the length of the word, and then in alphabetical
# order if the length of the word is the same.

# perl -n in shebang splits the input into lines.

# Convert input line in $_ into lower case.
$_ = lc($_);

# Pick up a word (consecutive alphabetic characters) and construct mapper output
# as "<Length of word>:<word><tab>1<new line>".
# Example:
#   1:a<tab>1<new line>
#   4:this<tab>1<new line>
# Including length as top of mapper output key makes the result shuffled
# from the shortest word to the longest word.
printf "%03d:$&\t1\n", length($&) while /[a-z]+/g;

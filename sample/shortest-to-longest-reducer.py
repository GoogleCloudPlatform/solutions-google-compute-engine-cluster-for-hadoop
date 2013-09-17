#!/usr/bin/env python
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

"""Reducer sample.

The word is already sorted in the desirable order.
The reducer counts the occurrence of each word and outputs the word
and its occurrence.
"""

import sys


class Word(object):
  """Class to keep current word's occurrence."""

  def __init__(self, word):
    self.word = word
    self.count = 0

  def Print(self):
    print '%s\t%d' % (self.word, self.count)

  def Increment(self, count=1):
    self.count += count


class ShortestToLongestReducer(object):
  """Class to accumulate counts from reducer input lines."""

  def __init__(self):
    self.current_word = None

  def PrintCurrentWord(self):
    """Outputs word count of the currently processing word."""
    if self.current_word:
      self.current_word.Print()

  def ProcessLine(self, line):
    """Process an input line.

    Args:
      line: Input line.
    """
    # Split input to key and value.
    key = line.split('\t', 1)[0]

    # Split key to word-length and word.
    word = key.split(':', 1)[1]

    if not self.current_word:
      self.current_word = Word(word)
    elif self.current_word.word != word:
      self.current_word.Print()
      self.current_word = Word(word)

    self.current_word.Increment()


def main(input_lines):
  reducer = ShortestToLongestReducer()

  for line in input_lines:
    reducer.ProcessLine(line)

  reducer.PrintCurrentWord()


if __name__ == '__main__':
  main(sys.stdin)

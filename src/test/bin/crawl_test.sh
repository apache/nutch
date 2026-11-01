#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

function set_up() {
  local root
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
  # Path is computed from this file; ShellCheck cannot follow it.
  # shellcheck disable=SC1091
  source "${root}/src/bin/crawl"
}

# Rows are: wait argument, expected seconds, description shown in the report.
# Lowercase suffixes follow the usage text. Uppercase letters are dropped by
# the suffix filter, so the remaining digits are counted as seconds.
function provider_wait_suffixes() {
  bashunit::data_set "1h" "3600" "lowercase h is one hour, so --wait 1h is 3600 seconds"
  bashunit::data_set "2h" "7200" "lowercase h is one hour, so --wait 2h is 7200 seconds"
  bashunit::data_set "10h" "36000" "lowercase h is one hour, so --wait 10h is 36000 seconds"
  bashunit::data_set "1m" "60" "lowercase m is one minute, so --wait 1m is 60 seconds"
  bashunit::data_set "1d" "86400" "lowercase d is one day, so --wait 1d is 86400 seconds"
  bashunit::data_set "1s" "1" "lowercase s is seconds, so --wait 1s is 1 second"
  bashunit::data_set "15" "15" "no suffix means seconds, so --wait 15 is 15 seconds"
  bashunit::data_set "1H" "1" "uppercase H is dropped by the suffix filter, so --wait 1H is 1 second"
  bashunit::data_set "1M" "1" "uppercase M is dropped by the suffix filter, so --wait 1M is 1 second"
  bashunit::data_set "1D" "1" "uppercase D is dropped by the suffix filter, so --wait 1D is 1 second"
}

# @data_provider provider_wait_suffixes
function test_wait_suffix_::1::_is_::2::_seconds() {
  bashunit::set_test_title "$3"
  assert_same "$2" "$(__to_seconds "$1")"
}

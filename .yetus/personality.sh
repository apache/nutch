# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Ensure JAVA_HOME is set for pre-patch and other phases when running in
# the Yetus Docker container (avoids "JAVA_HOME is not defined" in pre-patch).
if [ -z "${JAVA_HOME}" ] && [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
  export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
fi

# Pass JAVA_HOME into the re-exec Docker container so pre-patch and other
# phases see it (YETUS-913; otherwise the inner container may not get it).
# @audience private
# @stability stable
function docker_do_env_adds
{
  declare k
  DOCKER_EXTRAARGS+=("--env=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64")
  for k in "${DOCKER_EXTRAENVS[@]}"; do
    [[ -z "${k}" ]] && continue
    if [[ "JAVA_HOME" != "${k}" ]]; then
      DOCKER_EXTRAARGS+=("--env=${k}=${!k}")
    fi
  done
}

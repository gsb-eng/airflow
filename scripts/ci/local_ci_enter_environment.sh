#!/usr/bin/env bash
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

#
# Enters bash shell in the Docker container for full CI docker image in order to run tests in the container.
#

set -euo pipefail
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RUN_TESTS="false"
export MOUNT_LOCAL_SOURCES="true"
export PYTHON_VERSION=${PYTHON_VERSION:="3.6"}
export VERBOSE=${VERBOSE:="false"}

# shellcheck source=./ci_run_airflow_testing.sh
exec "${MY_DIR}/ci_run_airflow_testing.sh"

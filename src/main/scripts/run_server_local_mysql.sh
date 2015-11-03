#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CHANGE THIS TO THE ROOT OF THE DIRECTORY CONTAINING EXTRACTED PINLATER JARS.
export PINLATER_HOME_DIR=/var

export LOG_PROPERTIES=log4j.local.properties
export SERVER_CONFIG=pinlater.local.properties
export BACKEND_CONFIG=mysql.local.json

MY_DIR=`dirname $0`
source $MY_DIR/run_server_common.sh

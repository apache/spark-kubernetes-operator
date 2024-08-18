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

args=("$@")

if [ "$1" = "help" ]; then
  printf "Usage: $(basename "$0") (operator)\n"
  printf "    Or $(basename "$0") help\n\n"
  exit 0
elif [ "$1" = "operator" ]; then
  echo "Starting Operator..."

  exec java -cp "./$OPERATOR_JAR" "$LOG_CONFIG" "$OPERATOR_JAVA_OPTS" org.apache.spark.k8s.operator.SparkOperator
fi

args=("${args[@]}")

# Running command in pass-through mode
exec "${args[@]}"

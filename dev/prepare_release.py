#!/usr/bin/env python3

#
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
#

# Utility for preparing a release by removing the `-SNAPSHOT` and `-dev`
# suffixes from version strings (e.g. `1.0.1-SNAPSHOT` -> `1.0.1`,
# `1.9.0-dev` -> `1.9.0`).
#   usage: ./dev/prepare_release.py

import re
import sys
from pathlib import Path

FILES = [
    "AGENTS.md",
    "build-tools/helm/spark-kubernetes-operator/Chart.yaml",
    "build-tools/helm/spark-kubernetes-operator/values.yaml",
    "build.gradle",
    "docs/operations.md",
    "tests/e2e/watched-namespaces-file/spark-operator-dynamic-config-1.yaml",
    "tests/e2e/watched-namespaces/spark-operator-dynamic-config-1.yaml",
    "tests/e2e/watched-namespaces/spark-operator-dynamic-config-2.yaml",
]

VERSION_SUFFIX = re.compile(r"(\d+\.\d+\.\d+)-(?:SNAPSHOT|dev)")


def main():
    root = Path(__file__).resolve().parent.parent
    changed = 0
    for name in FILES:
        path = root / name
        content = path.read_text()
        updated, count = VERSION_SUFFIX.subn(r"\1", content)
        if count > 0:
            path.write_text(updated)
            changed += 1
            print(f"Updated {name} ({count} occurrences)")
        else:
            print(f"No changes in {name}")
    if changed == 0:
        print("Nothing to update.")
        sys.exit(1)


if __name__ == "__main__":
    main()

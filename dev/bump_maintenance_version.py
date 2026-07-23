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

# Utility for bumping the next development version by incrementing the
# maintenance version and attaching the development suffix. The current
# operator version is read from `build.gradle` (suffix `-SNAPSHOT`) and
# the Helm chart version from `Chart.yaml` (suffix `-dev`), so it works
# whether or not `./dev/prepare_release.py` has already removed the
# suffixes (e.g. `1.1.0-SNAPSHOT` or `1.1.0` -> `1.1.1-SNAPSHOT`,
# `1.9.0-dev` or `1.9.0` -> `1.9.1-dev`).
#   usage: ./dev/bump_maintenance_version.py

import re
import sys
from pathlib import Path

from prepare_release import FILES

VERSION = r"\d+\.\d+\.\d+"


def bump(version):
    major, minor, patch = version.split(".")
    return f"{major}.{minor}.{int(patch) + 1}"


def read_version(path, pattern):
    match = re.search(pattern, path.read_text(), re.MULTILINE)
    if match is None:
        print(f"Cannot find the current version in {path.name}.")
        sys.exit(1)
    return match[1]


def main():
    root = Path(__file__).resolve().parent.parent
    operator = read_version(root / "build.gradle", rf'^\s*version = "({VERSION})(?:-SNAPSHOT)?"')
    chart = read_version(
        root / "build-tools/helm/spark-kubernetes-operator/Chart.yaml",
        rf"^version: ({VERSION})(?:-dev)?$",
    )
    if operator == chart:
        print(f"Operator and chart versions are both {operator}; cannot bump unambiguously.")
        sys.exit(1)
    rules = [
        (re.compile(rf"(?<![\d.]){re.escape(operator)}(?:-SNAPSHOT)?(?!\d)"), f"{bump(operator)}-SNAPSHOT"),
        (re.compile(rf"(?<![\d.]){re.escape(chart)}(?:-dev)?(?!\d)"), f"{bump(chart)}-dev"),
    ]
    changed = 0
    for name in FILES:
        path = root / name
        content = path.read_text()
        count = 0
        for pattern, replacement in rules:
            content, n = pattern.subn(replacement, content)
            count += n
        if count > 0:
            path.write_text(content)
            changed += 1
            print(f"Updated {name} ({count} occurrences)")
        else:
            print(f"No changes in {name}")
    if changed == 0:
        print("Nothing to update.")
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
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

"""Pipe-marker reads must not strand coalesced output in a buffer.

Spawns a producer that prints ordinary lines plus the marker in one burst,
then sleeps past the deadline. Exits 0 iff await_marker finds the marker.
A select()+buffered-readline loop fails this case: readline consumes the
burst into its buffer while select() sees an empty pipe.
"""

import subprocess
import sys
import time

try:
    from neteng.ai.libsoftmetaroce.test.netns.interruption_test import await_marker
except ImportError:
    # Resolves only for a direct run (script dir on sys.path), invisible to
    # the build-system source database the type checker searches.
    from interruption_test import await_marker  # pyrefly: ignore [missing-import]

PRODUCER = (
    "print('ordinary line one', flush=True);"
    "print('ordinary line two', flush=True);"
    "print('server serving after 0.0s', flush=True);"
    "import time; time.sleep(30)"
)


def main() -> int:
    proc = subprocess.Popen(
        [sys.executable, "-c", PRODUCER],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    out = proc.stdout
    assert out is not None
    try:
        found = await_marker(out, b"server serving after", time.monotonic() + 10)
    finally:
        proc.kill()
        proc.wait()
    print("AWAIT_MARKER_PASS" if found else "AWAIT_MARKER_FAIL", flush=True)
    return 0 if found else 1


if __name__ == "__main__":
    sys.exit(main())

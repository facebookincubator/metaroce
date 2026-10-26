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

"""Endpoint B lifecycle: sequential launches, one-at-a-time, teardown.

Runs under the mode launcher via CTest. Exit 0 iff a held first launch
blocks a concurrent second one where the topology serializes B (rootless),
terminating the holder reports its signal, a later launch exits cleanly, a
B-side TCP probe reports a closed port, (rootless) a SIGTERM-ignoring
straggler is dead after teardown, and (privileged) terminate-then-kill
through the handle drains a straggler's group. Port 1 never listens in a
fresh namespace, so that probe is exact.
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    from neteng.ai.libsoftmetaroce.test.netns.netns_topology import (
        topology_for,
        TopologyError,
    )
except ImportError:
    # Resolves only for a direct run (script dir on sys.path), invisible to
    # the build-system source database the type checker searches.
    from netns_topology import (  # pyrefly: ignore [missing-import]
        topology_for,
        TopologyError,
    )

HOLDER = "import time;time.sleep(30)"
IGNORER = (
    "import os,signal,time;"
    "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
    "print(f'armed {os.getpid()}',flush=True);"
    "time.sleep(60)"
)


def _armed_pid(log: Path):
    """Pid from the straggler's arming line, or None until it prints."""
    try:
        text = log.read_text()
    except FileNotFoundError:
        return None
    for line in text.splitlines():
        if line.startswith("armed "):
            try:
                return int(line.split()[1])
            except (IndexError, ValueError):
                return None
    return None


def _straggler_alive(pid: int) -> bool:
    """True when pid runs our straggler: pid plus cmdline, not pid alone."""
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return False
    return b"SIG_IGN" in cmdline


def _launch_armed_straggler(topo):
    """Launch a TERM-ignoring straggler; handle, scratch, and self-reported
    pid once armed.

    Returns None when the straggler never prints its arming line within
    the deadline. The arming print proves the SIGTERM handler is
    installed; without it the test would race interpreter startup. The
    reported pid identifies the grandchild itself: the handle's pid is
    the launcher in privileged mode.
    """
    scratch = Path(tempfile.mkdtemp(prefix="straggler_"))
    log = scratch / "straggler.log"
    proc = topo.popen("b", [sys.executable, "-c", IGNORER], str(log))
    armed_by = time.monotonic() + 30
    while True:
        pid = _armed_pid(log)
        if pid is not None:
            return proc, scratch, pid
        if time.monotonic() >= armed_by:
            return None
        time.sleep(0.05)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Exercise endpoint B launches and teardown."
    )
    parser.add_argument("--mode", required=True, choices=("rootless", "privileged"))
    args = parser.parse_args()
    topo = topology_for(args.mode)
    straggler_pid = None
    group_drained_ok = True
    scratch = None
    with topo:
        holder = topo.popen("b", [sys.executable, "-c", HOLDER], os.devnull)
        if args.mode == "rootless":
            try:
                topo.popen("b", [sys.executable, "-c", "pass"], os.devnull)
            except TopologyError:
                concurrent_ok = True
            else:
                concurrent_ok = False
        else:
            extra = topo.popen("b", [sys.executable, "-c", "pass"], os.devnull)
            concurrent_ok = extra.wait(timeout=30) == 0
        holder.terminate()
        holder_code = holder.wait(timeout=30)
        second = topo.popen("b", [sys.executable, "-c", "pass"], os.devnull)
        second_code = second.wait(timeout=30)
        closed_ok = topo.check_tcp("b", topo.client_addr, 1) is False
        launched = _launch_armed_straggler(topo)
        if launched is None:
            print("straggler never armed", flush=True)
            return 1
        straggler, scratch, straggler_pid = launched
        if args.mode != "rootless":
            # The handle's pid is the sudo launcher; stopping it must not
            # orphan the TERM-ignoring grandchild it started.
            try:
                straggler.terminate()
                try:
                    straggler.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    straggler.kill()
                    straggler.wait(timeout=10)
            except subprocess.TimeoutExpired:
                group_drained_ok = False
            else:
                group_drained_ok = not _straggler_alive(straggler_pid)
    # Rootless leaves the straggler unwaited on purpose: teardown must
    # reap it anyway. Privileged re-checks the kill above.
    if straggler_pid is None:
        straggler_ok = True
    else:
        straggler_ok = not _straggler_alive(straggler_pid)
    if scratch is not None:
        shutil.rmtree(scratch, ignore_errors=True)
    ok = all(
        [
            concurrent_ok,
            # Termination arrives as a signal death (-15) or sudo's 143;
            # either proves the group signal landed. Only 0 (natural exit)
            # means the stop never reached the process.
            holder_code in (-signal.SIGTERM, 128 + signal.SIGTERM),
            second_code == 0,
            closed_ok,
            straggler_ok,
            group_drained_ok,
        ]
    )
    print(f"concurrent guard: {'OK' if concurrent_ok else 'FAIL'}", flush=True)
    print(f"holder code: {holder_code} second code: {second_code}", flush=True)
    print(f"closed-port probe: {'OK' if closed_ok else 'FAIL'}", flush=True)
    print(f"straggler reaped: {'OK' if straggler_ok else 'FAIL'}", flush=True)
    print(f"group drained: {'OK' if group_drained_ok else 'FAIL'}", flush=True)
    print("REPEAT_LAUNCH_PASS" if ok else "REPEAT_LAUNCH_FAIL", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

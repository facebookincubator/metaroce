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

"""Interrupting a conversation stops both endpoints and writes a summary.

Runs send_recv_smoke.py as a subprocess, waits for readiness on its stdout,
then SIGTERMs the smoke process alone: group signaling would mask a missing
cleanup by killing the endpoints itself. Exit 0 iff the smoke exits 143,
summary.txt carries a FAIL verdict without recording shutdown survivors,
and no endpoint outlives the smoke.
"""

import argparse
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path


def _stray_endpoints(artifact_dir: Path) -> list[tuple[int, str, str, str]]:
    """Live endpoints as (pid, comm, pgrp, cmdline) details for diagnosis.

    Matches processes whose command line names a conversation artifact
    JSON file. The group id distinguishes an unsignaled tree from one
    that escaped its signaled group.
    """
    needle = f"json_path={artifact_dir}".encode()
    strays = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit() or int(pid) == os.getpid():
            continue
        try:
            args = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            comm = Path(f"/proc/{pid}/comm").read_text().strip()
            after_comm = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1]
            # stat fields after "(comm)": state ppid pgrp ...
            pgrp = after_comm.split()[2]
        except (OSError, IndexError):
            continue
        if any(needle in arg for arg in args):
            cmd = b" ".join(a for a in args if a).decode(errors="replace")
            strays.append((int(pid), comm, pgrp, cmd))
    return strays


def _summary_problems(summary: Path) -> list:
    """Problems with the post-interruption summary (empty means clean)."""
    if not summary.is_file():
        return ["summary.txt missing"]
    lines = summary.read_text().splitlines()
    problems = []
    if "FAIL" not in lines:
        problems.append("summary lacks FAIL verdict")
    if any("survived SIGKILL" in line for line in lines):
        problems.append("summary reports shutdown survivors")
    return problems


def await_marker(pipe, marker: bytes, deadline: float) -> bool:
    """True once marker appears in pipe output; raw bytes, no buffering.

    select() observes the file descriptor while buffered readers consume
    ahead of it, so a buffered readline loop can strand the marker in its
    buffer behind an empty pipe. Accumulating raw reads keeps the two in
    agreement.
    """
    buf = b""
    while time.monotonic() < deadline:
        ready, _, _ = select.select([pipe], [], [], max(deadline - time.monotonic(), 0))
        if not ready:
            return False
        chunk = os.read(pipe.fileno(), 65536)
        if not chunk:
            return marker in buf
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if marker in line:
                return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="SIGTERM a running conversation and check the aftermath."
    )
    parser.add_argument("--rdma-gen", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--mode", required=True, choices=("rootless", "privileged"))
    args = parser.parse_args()
    artifact_dir = Path(args.artifact_dir)
    smoke = Path(__file__).resolve().parent / "send_recv_smoke.py"
    proc = subprocess.Popen(
        [
            sys.executable,
            str(smoke),
            "--rdma-gen",
            args.rdma_gen,
            "--artifact-dir",
            str(artifact_dir),
            "--mode",
            args.mode,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    out = proc.stdout
    assert out is not None
    serving = await_marker(out, b"server serving after", time.monotonic() + 180)
    if not serving:
        proc.kill()
        proc.wait()
        print("smoke never served; cannot interrupt", flush=True)
        return 1
    proc.send_signal(signal.SIGTERM)
    try:
        code = proc.wait(timeout=120)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        print("smoke ignored SIGTERM", flush=True)
        return 1
    ok = True
    if code != 128 + signal.SIGTERM:
        print(f"smoke exit {code}, expected 143", flush=True)
        ok = False
    summary = artifact_dir / "summary.txt"
    for problem in _summary_problems(summary):
        print(problem, flush=True)
        ok = False
    strays = _stray_endpoints(artifact_dir)
    if strays:
        print(
            f"endpoints outlived the smoke: {[p for p, _, _, _ in strays]}",
            flush=True,
        )
        for pid, comm, pgrp, cmd in strays:
            print(
                f"stray pid={pid} comm={comm} pgrp={pgrp} cmd={cmd}",
                flush=True,
            )
        ok = False
    print("INTERRUPTION_PASS" if ok else "INTERRUPTION_FAIL", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

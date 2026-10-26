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

"""Network plumbing probe: two namespaces, veth, IPv6, bidirectional ping.

Modes are explicit; there is no fallback between them:
- rootless: run under `unshare -U -r -n -m`; the namespaces are private
  handles that vanish with the process.
- privileged: run directly; the namespaces are named, unique per run, and
  managed with non-interactive sudo.

Both modes build the same topology and run the same assertions. Stdlib only.
Exit 0 iff both directions ping and cleanup leaves nothing behind.
"""

import argparse
import os
import signal
import subprocess
import sys
import time

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


def ping(topo, ns: str, addr: str, tries: int = 6) -> bool:
    for _ in range(tries):
        proc = topo.popen(ns, [topo.ping, "-6", "-c", "1", "-W", "1", addr], os.devnull)
        try:
            code = proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            code = proc.wait()
        if code == 0:
            return True
        time.sleep(0.5)
    return False


def _on_sigterm(signum: int, _frame: object) -> None:
    print(f"received signal {signum}; cleaning up", file=sys.stderr, flush=True)
    raise SystemExit(128 + signum)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe two-namespace veth plumbing in one explicit mode."
    )
    parser.add_argument("--mode", required=True, choices=("rootless", "privileged"))
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, _on_sigterm)
    topo = topology_for(args.mode)
    if not os.path.isfile(topo.ip):
        print(f"ip not found (looked up {topo.ip})", file=sys.stderr)
        return 2
    if not os.path.isfile(topo.ping):
        print(f"ping not found (looked up {topo.ping})", file=sys.stderr)
        return 2
    try:
        with topo:
            to_b = ping(topo, "a", topo.client_addr)
            print(
                f"A->B ping {topo.client_addr}: {'OK' if to_b else 'FAIL'}", flush=True
            )
            to_a = ping(topo, "b", topo.server_addr)
            print(
                f"B->A ping {topo.server_addr}: {'OK' if to_a else 'FAIL'}", flush=True
            )
            ok = to_a and to_b
    except TopologyError as e:
        print(f"topology failed: {e}", flush=True)
        return 1
    for error in getattr(topo, "cleanup_errors", []):
        print(error, flush=True)
        ok = False
    print(f"ADDRESSES {topo.server_addr} {topo.client_addr}", flush=True)
    print("PROBE_PASS" if ok else "PROBE_FAIL", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

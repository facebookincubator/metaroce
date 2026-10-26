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
import secrets
import shutil
import signal
import subprocess
import sys
import time

CLONE_NEWNET = 0x40000000
PID_SUFFIX = os.getpid() & 0xFFFF
# Addresses are isolation-scoped to one run's namespaces, so a pid suffix
# cannot collide. Names live in shared registries, so they take a random
# token instead.
ADDR_A = f"fc00::{PID_SUFFIX:x}:1"
ADDR_B = f"fc00::{PID_SUFFIX:x}:101"
TOKEN = secrets.token_hex(4)
NS1 = f"lsmr1_{TOKEN}"
NS2 = f"lsmr2_{TOKEN}"
VETH1 = f"v1_{TOKEN}"
VETH2 = f"v2_{TOKEN}"
IP = shutil.which("ip") or "/usr/sbin/ip"
PING = shutil.which("ping") or "/usr/bin/ping"


def run(*args: str, prefix: tuple[str, ...] = ()) -> None:
    subprocess.run(
        [*prefix, *args], capture_output=True, text=True, check=True, timeout=30
    )


def ping(addr: str, prefix: tuple[str, ...] = (), tries: int = 6) -> bool:
    for _ in range(tries):
        r = subprocess.run(
            [*prefix, PING, "-6", "-c", "1", "-W", "1", addr],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if r.returncode == 0:
            return True
        time.sleep(0.5)
    return False


def child_main(w_up: int, r_go: int, w_done: int, r_parent_done: int) -> int:
    os.unshare(CLONE_NEWNET)
    os.write(w_up, b"1")  # netns B exists; parent may move the peer by our pid
    assert os.read(r_go, 1) == b"1"  # peer moved
    run(IP, "link", "set", "lo", "up")
    run(IP, "addr", "add", f"{ADDR_B}/64", "dev", "vethB")
    run(IP, "link", "set", "vethB", "up")
    ok = ping(ADDR_A)
    os.write(w_done, b"1" if ok else b"0")
    print(f"B->A ping {ADDR_A}: {'OK' if ok else 'FAIL'}", flush=True)
    # Exiting would destroy this namespace before the parent's ping runs,
    # so stay until the parent signals. EOF releases us on its failure path.
    os.read(r_parent_done, 1)
    return 0 if ok else 1


def parent_main(
    child_pid: int, r_up: int, w_go: int, r_done: int, w_parent_done: int
) -> bool:
    try:
        assert os.read(r_up, 1) == b"1"
        run(IP, "link", "set", "vethB", "netns", str(child_pid))
        run(IP, "addr", "add", f"{ADDR_A}/64", "dev", "vethA")
        run(IP, "link", "set", "vethA", "up")
        os.write(w_go, b"1")
        child_ok = os.read(r_done, 1) == b"1"
        parent_ok = ping(ADDR_B)
        print(f"A->B ping {ADDR_B}: {'OK' if parent_ok else 'FAIL'}", flush=True)
    finally:
        # Release the waiting child on every path; a close without a byte
        # reads as EOF. The write may fail if the child already exited.
        try:
            os.write(w_parent_done, b"1")
        except OSError:
            pass
        os.close(w_parent_done)
    _, status = os.waitpid(child_pid, 0)
    return child_ok and parent_ok and status == 0


def rootless_main() -> int:
    if os.getuid() != 0:
        print(
            "expected root-mapped userns (run via unshare -U -r)",
            file=sys.stderr,
        )
        return 2
    run(IP, "link", "set", "lo", "up")
    run(IP, "link", "add", "vethA", "type", "veth", "peer", "name", "vethB")
    r_up, w_up = os.pipe()
    r_go, w_go = os.pipe()
    r_done, w_done = os.pipe()
    r_pd, w_pd = os.pipe()
    pid = os.fork()
    if pid == 0:
        for fd in (r_up, w_go, r_done, w_pd):
            os.close(fd)
        try:
            code = child_main(w_up, r_go, w_done, r_pd)
        except Exception as e:  # probe reports, never hangs parent
            print(f"child error: {e}", flush=True)
            code = 1
        os._exit(code)
    for fd in (w_up, r_go, w_done, r_pd):
        os.close(fd)
    try:
        ok = parent_main(pid, r_up, w_go, r_done, w_pd)
    except Exception as e:  # probe reports failure, then cleans up
        print(f"parent error: {e}", flush=True)
        ok = False
    print(f"ADDRESSES {ADDR_A} {ADDR_B}", flush=True)
    print("PROBE_PASS" if ok else "PROBE_FAIL", flush=True)
    return 0 if ok else 1


def _on_sigterm(signum: int, _frame: object) -> None:
    # The default disposition would skip cleanup; raising runs it, then exits
    # with the conventional signal status.
    print(f"received signal {signum}; cleaning up", file=sys.stderr, flush=True)
    raise SystemExit(128 + signum)


def _cleanup_privileged(
    sudo: tuple[str, ...], namespaces: list[str], ends_on_host: list[str]
) -> list[str]:
    errors: list[str] = []
    # Host ends first: deleting a namespace destroys the ends moved into it,
    # which would take an unmoved peer with them and leave this delete with
    # nothing to remove. Deleting one end removes the pair, so a lone delete
    # covers both the intact pair and a partway-move remnant.
    if ends_on_host:
        r = subprocess.run(
            [*sudo, IP, "link", "del", ends_on_host[0]],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if r.returncode != 0:
            errors.append(
                f"cleanup failed: link del {ends_on_host[0]}: {r.stderr.strip()}"
            )
    for ns in namespaces:
        r = subprocess.run(
            [*sudo, IP, "netns", "del", ns],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if r.returncode != 0:
            errors.append(f"cleanup failed: netns del {ns}: {r.stderr.strip()}")
    return errors


def privileged_main() -> int:
    signal.signal(signal.SIGTERM, _on_sigterm)
    sudo = ("sudo", "-n")
    gate = subprocess.run([*sudo, "true"], capture_output=True, timeout=30)
    if gate.returncode != 0:
        print("privileged mode needs non-interactive sudo", file=sys.stderr)
        return 2
    created_ns: list[str] = []
    ends_on_host: list[str] = []
    ok = False
    try:
        run(IP, "netns", "add", NS1, prefix=sudo)
        created_ns.append(NS1)
        run(IP, "netns", "add", NS2, prefix=sudo)
        created_ns.append(NS2)
        run(
            IP,
            "link",
            "add",
            VETH1,
            "type",
            "veth",
            "peer",
            "name",
            VETH2,
            prefix=sudo,
        )
        ends_on_host = [VETH1, VETH2]
        run(IP, "link", "set", VETH1, "netns", NS1, prefix=sudo)
        ends_on_host.remove(VETH1)
        run(IP, "link", "set", VETH2, "netns", NS2, prefix=sudo)
        ends_on_host.remove(VETH2)
        for ns, veth, addr in ((NS1, VETH1, ADDR_A), (NS2, VETH2, ADDR_B)):
            run(IP, "-n", ns, "link", "set", "lo", "up", prefix=sudo)
            run(IP, "-n", ns, "addr", "add", f"{addr}/64", "dev", veth, prefix=sudo)
            run(IP, "-n", ns, "link", "set", veth, "up", prefix=sudo)
        first = ping(ADDR_B, prefix=(*sudo, IP, "netns", "exec", NS1))
        print(f"{NS1}->{NS2} ping {ADDR_B}: {'OK' if first else 'FAIL'}", flush=True)
        second = ping(ADDR_A, prefix=(*sudo, IP, "netns", "exec", NS2))
        print(f"{NS2}->{NS1} ping {ADDR_A}: {'OK' if second else 'FAIL'}", flush=True)
        ok = first and second
    except subprocess.CalledProcessError as e:
        print(f"setup failed: {' '.join(e.cmd)}: {e.stderr.strip()}", flush=True)
        ok = False
    finally:
        errors = _cleanup_privileged(sudo, created_ns, ends_on_host)
        for error in errors:
            print(error, flush=True)
        ok = ok and not errors
    print(f"ADDRESSES {ADDR_A} {ADDR_B}", flush=True)
    print("PROBE_PASS" if ok else "PROBE_FAIL", flush=True)
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe two-namespace veth plumbing in one explicit mode."
    )
    parser.add_argument("--mode", required=True, choices=("rootless", "privileged"))
    args = parser.parse_args()
    if not os.path.isfile(IP):
        print(f"ip not found (looked up {IP})", file=sys.stderr)
        return 2
    if not os.path.isfile(PING):
        print(f"ping not found (looked up {PING})", file=sys.stderr)
        return 2
    if args.mode == "privileged":
        return privileged_main()
    return rootless_main()


if __name__ == "__main__":
    sys.exit(main())

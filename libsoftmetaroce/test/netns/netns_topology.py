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

"""Two-namespace veth topology for the conversation tests, both modes.

Rootless (run under `unshare -U -r -n -m`): the current namespace is
endpoint A; endpoint B is a child namespace served by a persistent
launcher, running one process at a time. Launches are sequential: wait for
one B-side process before starting another.

Privileged: named namespaces unique per run, managed with non-interactive
sudo; either endpoint runs any number of processes.

Teardown runs from `__exit__`; callers install their own SIGTERM handling so
a timeout still reaches it. Stdlib only.
"""

import json
import os
import secrets
import select
import shutil
import signal
import socket
import subprocess
import sys
import time

CLONE_NEWNET = 0x40000000

# Exits 0 iff (host, port) accepts TCP; silent on refusal either way.
_PROBE = (
    "import socket, sys\n"
    "try:\n"
    "    s = socket.create_connection(\n"
    "        (sys.argv[1], int(sys.argv[2])), timeout=float(sys.argv[3]))\n"
    "except OSError:\n"
    "    sys.exit(1)\n"
    "s.close()\n"
)


class TopologyError(Exception):
    """The topology could not be created."""


def _resolve_tool(name: str, fallback: str) -> str:
    found = shutil.which(name)
    return found if found else fallback


class LaunchedProcess:
    """Popen-like handle for one B-side process; status arrives on a pipe."""

    def __init__(self, pid: int, result_fd: int, argv: list[str]) -> None:
        self.pid = pid
        self._result_fd = result_fd
        self.args = argv
        self.returncode: int | None = None

    def _take_status(self) -> int:
        line = _read_line(self._result_fd)
        if not line:
            return -1
        try:
            return int(line.decode())
        except ValueError:
            raise TopologyError("endpoint B launcher misbehaved") from None

    def poll(self) -> int | None:
        if self.returncode is not None:
            return self.returncode
        ready, _, _ = select.select([self._result_fd], [], [], 0)
        if not ready:
            return None
        self.returncode = self._take_status()
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is not None:
            return self.returncode
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                assert timeout is not None
                raise subprocess.TimeoutExpired(self.args, timeout)
            ready, _, _ = select.select([self._result_fd], [], [], remaining)
            if ready:
                self.returncode = self._take_status()
                return self.returncode

    def _signal(self, signum: int) -> None:
        try:
            os.kill(self.pid, signum)
        except ProcessLookupError:
            pass

    def terminate(self) -> None:
        self._signal(signal.SIGTERM)

    def kill(self) -> None:
        self._signal(signal.SIGKILL)


class _GroupProcess:
    """Popen-like handle whose signals and waits cover the process group.

    Privileged endpoints launch through `sudo ip netns exec`; the handle's
    pid is the sudo launcher, so signaling it alone orphans rdma_gen when
    the launcher exits first. The child starts in its own session, so the
    group holds exactly this tree, and waiting drains the group rather
    than just reaping the launcher. Members run as root, so group
    signals go through a sudo python killpg helper: the `kill`
    command's group syntax varies enough across systems to fail
    silently, while the syscall has no syntax to vary.
    """

    def __init__(self, proc: subprocess.Popen, sudo: tuple) -> None:
        self._proc = proc
        self._sudo = sudo
        self.pid = proc.pid
        self.args = proc.args
        self._last_kill: tuple | None = None

    @property
    def returncode(self) -> int | None:
        return self._proc.returncode

    def poll(self) -> int | None:
        return self._proc.poll()

    def wait(self, timeout: float | None = None) -> int:
        try:
            code = self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._report_kill()
            raise
        deadline = None if timeout is None else time.monotonic() + timeout
        while not _group_empty(self.pid):
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                assert timeout is not None
                self._report_kill()
                raise subprocess.TimeoutExpired(self.args, timeout)
            time.sleep(0.05)
        self._last_kill = None
        return code

    def _signal_group(self, signum: int) -> None:
        # kill fails when the group already drained; the wait decides, and
        # reports this attempt if the group is still occupied at timeout.
        helper = "import os,sys; os.killpg(int(sys.argv[1]), int(sys.argv[2]))"
        sent = subprocess.run(
            [*self._sudo, sys.executable, "-c", helper, str(self.pid), str(signum)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self._last_kill = (signum, sent.returncode, sent.stderr.strip())

    def _report_kill(self) -> None:
        if self._last_kill is None:
            return
        signum, code, err = self._last_kill
        print(
            f"group {self.pid} still occupied; last kill({signum}) rc={code}"
            + (f": {err}" if err else ""),
            file=sys.stderr,
            flush=True,
        )

    def terminate(self) -> None:
        self._signal_group(signal.SIGTERM)

    def kill(self) -> None:
        self._signal_group(signal.SIGKILL)


def _read_line(fd: int) -> bytes:
    chunks = []
    while True:
        byte = os.read(fd, 1)
        if byte in (b"\n", b""):
            return b"".join(chunks) if byte else b""
        chunks.append(byte)


def _reaped(pid: int) -> bool:
    """True once pid is reaped (or was never our child)."""
    try:
        wpid, _ = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return True
    return wpid != 0


def _group_empty(pgid: int) -> bool:
    """True when no signalable process remains in the group."""
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        # Members exist but belong to another user (privileged endpoints
        # run as root); the group is observably non-empty.
        return False
    return False


class RootlessTopology:
    """Endpoints A (current namespace) and B (one forked child namespace)."""

    def __init__(self) -> None:
        self.ip = _resolve_tool("ip", "/usr/sbin/ip")
        self.ping = _resolve_tool("ping", "/usr/bin/ping")
        suffix = os.getpid() & 0xFFFF
        self.server_addr = f"fc00::{suffix:x}:1"
        self.client_addr = f"fc00::{suffix:x}:101"
        self._child_pid: int | None = None
        self._cmd_w: int | None = None
        self._r_result: int | None = None
        self._b_active: LaunchedProcess | None = None

    def _run(self, *args: str) -> None:
        subprocess.run(
            [self.ip, *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )

    def __enter__(self) -> "RootlessTopology":
        if os.getuid() != 0:
            raise TopologyError("rootless topology needs `unshare -U -r`")
        self._run("link", "set", "lo", "up")
        self._run("link", "add", "vethA", "type", "veth", "peer", "name", "vethB")
        r_up, w_up = os.pipe()
        r_go, w_go = os.pipe()
        r_ready, w_ready = os.pipe()
        r_cmd, w_cmd = os.pipe()
        r_result, w_result = os.pipe()
        pid = os.fork()
        if pid == 0:
            self._child_main(
                r_go,
                w_up,
                r_cmd,
                w_ready,
                w_result,
                r_up,
                w_go,
                r_ready,
                w_cmd,
                r_result,
            )
        for fd in (w_up, r_go, w_ready, r_cmd, w_result):
            os.close(fd)
        self._child_pid = pid
        self._cmd_w = w_cmd
        self._r_result = r_result
        try:
            assert os.read(r_up, 1) == b"1"
            self._run("link", "set", "vethB", "netns", str(pid))
            self._run("addr", "add", f"{self.server_addr}/64", "dev", "vethA")
            self._run("link", "set", "vethA", "up")
            os.write(w_go, b"1")
            assert os.read(r_ready, 1) == b"1"
        except BaseException:
            self._abandon_child(pid, [r_up, w_go, r_ready, w_cmd, r_result])
            raise
        for fd in (r_up, w_go, r_ready):
            os.close(fd)
        return self

    def _reap_launcher(self, pid: int) -> None:
        """Empty the launcher group: SIGTERM, then SIGKILL past the deadline.

        The group covers the launcher and any B-side process it started, so
        grandchildren die even when their pid never reached the parent. The
        wait tracks the group, not just the launcher: returning when the
        launcher is reaped would strand a descendant that ignored SIGTERM.
        """
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 5
        killed = False
        while True:
            if _reaped(pid) and _group_empty(pid):
                return
            if time.monotonic() >= deadline:
                if killed:
                    print(
                        f"launcher group {pid} survived SIGKILL; leaving it",
                        file=sys.stderr,
                        flush=True,
                    )
                    return
                try:
                    os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                killed = True
                deadline = time.monotonic() + 5
            time.sleep(0.05)

    def _abandon_child(self, pid: int, fds: list[int]) -> None:
        self._reap_launcher(pid)
        for fd in fds:
            try:
                os.close(fd)
            except OSError:
                pass
        self._child_pid = None
        self._cmd_w = None
        self._r_result = None

    def _child_main(
        self,
        r_go: int,
        w_up: int,
        r_cmd: int,
        w_ready: int,
        w_result: int,
        r_up: int,
        w_go: int,
        r_ready: int,
        w_cmd: int,
        r_result: int,
    ) -> None:
        for fd in (r_up, w_go, r_ready, w_cmd, r_result):
            os.close(fd)
        try:
            # Own process group: teardown signals reach B-side processes
            # even if interruption struck before their pid was reported.
            os.setsid()
            os.unshare(CLONE_NEWNET)
            os.write(w_up, b"1")
            assert os.read(r_go, 1) == b"1"
            self._run("link", "set", "lo", "up")
            self._run("addr", "add", f"{self.client_addr}/64", "dev", "vethB")
            self._run("link", "set", "vethB", "up")
            os.write(w_ready, b"1")
            while True:
                line = _read_line(r_cmd)
                if not line:
                    os._exit(0)
                argv, stdout_path, stderr_path = json.loads(line)
                gpid = os.fork()
                if gpid != 0:
                    os.write(w_result, f"{gpid}\n".encode())
                    _, status = os.waitpid(gpid, 0)
                    code = os.waitstatus_to_exitcode(status)
                    os.write(w_result, f"{code}\n".encode())
                    continue
                # Grandchild; never returns to the loop below.
                # Only stdio survives into the exec'd process: the command
                # and result pipes close here, so the parent's EOF read on
                # a dead launcher cannot hang on an inheritor.
                for fd in (r_cmd, w_result, r_go, w_up, w_ready):
                    os.close(fd)
                out = os.open(stdout_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                os.dup2(out, 1)
                if stderr_path is None:
                    os.dup2(out, 2)
                else:
                    err = os.open(
                        stderr_path,
                        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                        0o644,
                    )
                    os.dup2(err, 2)
                    os.close(err)
                os.close(out)
                devnull = os.open(os.devnull, os.O_RDONLY)
                os.dup2(devnull, 0)
                os.close(devnull)
                os.execvp(argv[0], argv)
        except Exception as e:
            print(f"B-side child failed: {e}", flush=True)
        os._exit(127)

    def popen(
        self,
        ns: str,
        argv: list[str],
        stdout_path: str | None,
        stderr_path: str | None = None,
    ):
        """Start one process in endpoint A or B (B one at a time)."""
        if ns == "a":
            out = open(stdout_path, "wb") if stdout_path else None
            err_file = None if stderr_path is None else open(stderr_path, "wb")
            err = subprocess.STDOUT if err_file is None else err_file
            proc = subprocess.Popen(
                argv, stdin=subprocess.DEVNULL, stdout=out, stderr=err
            )
            if out is not None:
                out.close()
            if err_file is not None:
                err_file.close()
            return proc
        if ns != "b":
            raise TopologyError(f"unknown endpoint {ns!r}")
        if self._cmd_w is None or self._r_result is None:
            raise TopologyError("endpoint B is not available")
        if self._b_active is not None and self._b_active.poll() is None:
            raise TopologyError("endpoint B runs one process at a time")
        os.write(
            self._cmd_w,
            (
                json.dumps([argv, stdout_path or os.devnull, stderr_path]) + "\n"
            ).encode(),
        )
        ready, _, _ = select.select([self._r_result], [], [], 30)
        if not ready:
            raise TopologyError("endpoint B did not start the process")
        pid_line = _read_line(self._r_result)
        if not pid_line:
            raise TopologyError("endpoint B launcher exited")
        try:
            gpid = int(pid_line.decode())
        except ValueError:
            raise TopologyError("endpoint B launcher misbehaved") from None
        proc = LaunchedProcess(gpid, self._r_result, argv)
        self._b_active = proc
        return proc

    def check_tcp(self, ns: str, host: str, port: int, timeout: float = 2.0) -> bool:
        """True iff (host, port) accepts TCP from inside endpoint ns."""
        if ns == "a":
            try:
                s = socket.create_connection((host, port), timeout=timeout)
            except OSError:
                return False
            s.close()
            return True
        if ns != "b":
            raise TopologyError(f"unknown endpoint {ns!r}")
        proc = self.popen(
            "b",
            [sys.executable, "-c", _PROBE, host, str(port), str(timeout)],
            os.devnull,
        )
        try:
            return proc.wait(timeout=timeout + 5) == 0
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            return False

    def __exit__(self, *exc_info) -> None:
        self._b_active = None
        if self._cmd_w is not None:
            os.close(self._cmd_w)
            self._cmd_w = None
        if self._child_pid is not None:
            self._reap_launcher(self._child_pid)
            self._child_pid = None
        if self._r_result is not None:
            os.close(self._r_result)
            self._r_result = None


class PrivilegedTopology:
    """Named namespaces unique per run, managed with `sudo -n`.

    Processes outlive their namespace; callers stop what they start.
    """

    def __init__(self) -> None:
        self.ip = _resolve_tool("ip", "/usr/sbin/ip")
        self.ping = _resolve_tool("ping", "/usr/bin/ping")
        self.sudo = ("sudo", "-n")
        suffix = os.getpid() & 0xFFFF
        self.server_addr = f"fc00::{suffix:x}:1"
        self.client_addr = f"fc00::{suffix:x}:101"
        token = secrets.token_hex(4)
        self.ns1 = f"lsmr1_{token}"
        self.ns2 = f"lsmr2_{token}"
        self.veth1 = f"v1_{token}"
        self.veth2 = f"v2_{token}"
        self.cleanup_errors: list[str] = []
        self._created_ns: list[str] = []
        self._ends_on_host: list[str] = []

    def _run(self, *args: str) -> None:
        subprocess.run(
            [*self.sudo, self.ip, *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )

    def __enter__(self) -> "PrivilegedTopology":
        gate = subprocess.run([*self.sudo, "true"], capture_output=True, timeout=30)
        if gate.returncode != 0:
            raise TopologyError("privileged topology needs passwordless sudo")
        try:
            self._run("netns", "add", self.ns1)
            self._created_ns.append(self.ns1)
            self._run("netns", "add", self.ns2)
            self._created_ns.append(self.ns2)
            self._run(
                "link", "add", self.veth1, "type", "veth", "peer", "name", self.veth2
            )
            self._ends_on_host = [self.veth1, self.veth2]
            self._run("link", "set", self.veth1, "netns", self.ns1)
            self._ends_on_host.remove(self.veth1)
            self._run("link", "set", self.veth2, "netns", self.ns2)
            self._ends_on_host.remove(self.veth2)
            for ns, veth, addr in (
                (self.ns1, self.veth1, self.server_addr),
                (self.ns2, self.veth2, self.client_addr),
            ):
                self._run("-n", ns, "link", "set", "lo", "up")
                self._run("-n", ns, "addr", "add", f"{addr}/64", "dev", veth)
                self._run("-n", ns, "link", "set", veth, "up")
        except BaseException:
            self.cleanup_errors.extend(self._cleanup())
            raise
        return self

    def prefix(self, ns: str) -> list[str]:
        if ns == "a":
            return [*self.sudo, self.ip, "netns", "exec", self.ns1]
        if ns == "b":
            return [*self.sudo, self.ip, "netns", "exec", self.ns2]
        raise TopologyError(f"unknown endpoint {ns!r}")

    def popen(
        self,
        ns: str,
        argv: list[str],
        stdout_path: str | None,
        stderr_path: str | None = None,
    ):
        """Start one process; the handle signals its whole group."""
        out = open(stdout_path, "wb") if stdout_path else None
        err_file = None if stderr_path is None else open(stderr_path, "wb")
        err = subprocess.STDOUT if err_file is None else err_file
        proc = subprocess.Popen(
            self.prefix(ns) + argv,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=err,
            start_new_session=True,
        )
        if out is not None:
            out.close()
        if err_file is not None:
            err_file.close()
        return _GroupProcess(proc, self.sudo)

    def check_tcp(self, ns: str, host: str, port: int, timeout: float = 2.0) -> bool:
        """True iff (host, port) accepts TCP from inside endpoint ns."""
        r = subprocess.run(
            self.prefix(ns)
            + [sys.executable, "-c", _PROBE, host, str(port), str(timeout)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return r.returncode == 0

    def _cleanup(self) -> list[str]:
        errors: list[str] = []
        # Host ends first: deleting a namespace destroys the ends moved into
        # it, which would take an unmoved peer with them. Deleting one end
        # removes the pair, so a lone delete covers both the intact pair and
        # a partway-move remnant.
        if self._ends_on_host:
            r = subprocess.run(
                [*self.sudo, self.ip, "link", "del", self._ends_on_host[0]],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode != 0:
                errors.append(
                    f"cleanup failed: link del {self._ends_on_host[0]}: "
                    f"{r.stderr.strip()}"
                )
        for ns in self._created_ns:
            r = subprocess.run(
                [*self.sudo, self.ip, "netns", "del", ns],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode != 0:
                errors.append(f"cleanup failed: netns del {ns}: {r.stderr.strip()}")
        return errors

    def __exit__(self, *exc_info) -> None:
        # Stored, not raised: teardown must not mask the body's failure.
        self.cleanup_errors.extend(self._cleanup())


def topology_for(mode: str):
    if mode == "rootless":
        return RootlessTopology()
    if mode == "privileged":
        return PrivilegedTopology()
    raise TopologyError(f"unknown mode {mode!r}")

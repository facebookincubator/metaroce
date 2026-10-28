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

"""One conversation between two rdma_gen endpoints in isolated namespaces.

The server runs in endpoint A, the client in endpoint B. Server readiness is
a TCP connect to the control port from inside endpoint A, polled with a
deadline; the control server only serves after the responder is constructed,
so an accepted connect means the client can proceed too. (A log marker was
tried first, but the serving line never reaches captured stderr, even on a
clean exit, so logs cannot synchronize readiness.)
Client completion and a bounded server stop follow. Endpoint shutdown and
summary finalization run in finally blocks, so interruption still stops both
endpoints and writes the summary. Privileged endpoints run
as root inside their namespaces; only namespace setup, entry, and cleanup
use privilege. Stdlib only.
"""

import contextlib
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

try:
    import neteng.ai.libsoftmetaroce.test.netns.netns_topology as netns_topology
    import neteng.ai.libsoftmetaroce.test.netns.rdmagen_output as rdmagen_output
except ImportError:
    # Resolves only for a direct run (script dir on sys.path), invisible to
    # the build-system source database the type checker searches.
    import netns_topology  # pyrefly: ignore [missing-import]
    import rdmagen_output  # pyrefly: ignore [missing-import]

REQUIRED_FLAGS = (
    "verbs_mode",
    "json_path",
    "rc_servers",
    "rc_cm_port",
    "rc_connect_ports",
    "num_messages",
    "num_completions_expected",
    "message_size",
    "mtu",
    "op_type",
    "buffer_cap",
    "qp_cap",
)


@dataclass
class ConversationSpec:
    """One conversation: endpoint flags plus the expected outcome."""

    name: str
    server_args: list
    client_args: list
    expected_server_cqes: int
    expected_client_cqes: int
    control_port: int = 18515
    # Spelling used in the JSON output, not the --op_type flag value.
    expected_op: str = "SendReceive"
    expected_server_exit: int = 0
    expected_client_exit: int = 0
    ready_timeout_s: float = 60.0
    deadline_s: float = 300.0
    stop_grace_s: float = 30.0


@dataclass
class ConversationResult:
    ok: bool
    problems: list = field(default_factory=list)


def check_compatible(rdma_gen: str) -> tuple[str, list[str]]:
    """Validate the binary against the CLI contract runners need.

    Returns version information plus problems; empty problems means the
    binary is compatible. Version is best-effort and never fails alone.
    """
    problems = []
    if not os.path.isfile(rdma_gen) or not os.access(rdma_gen, os.X_OK):
        return "unknown", [f"rdma_gen is not an executable file: {rdma_gen}"]
    try:
        helped = subprocess.run(
            [rdma_gen, "--help"], capture_output=True, text=True, timeout=60
        )
    except (OSError, subprocess.SubprocessError) as e:
        return "unknown", [f"rdma_gen --help failed: {e}"]
    documented = helped.stdout + helped.stderr
    for flag in REQUIRED_FLAGS:
        if not re.search(
            rf"(?:^|\s)-{{1,2}}{flag}(?:[\s=]|$)", documented, re.MULTILINE
        ):
            problems.append(f"rdma_gen lacks required flag --{flag}")
    try:
        versioned = subprocess.run(
            [rdma_gen, "--version"], capture_output=True, text=True, timeout=60
        )
        version = (versioned.stdout + versioned.stderr).strip().splitlines()
        version_info = version[0] if version else "unknown"
    except (OSError, subprocess.SubprocessError):
        version_info = "unknown"
    return version_info, problems


def _on_sigterm(signum: int, _frame: object) -> None:
    print(f"received signal {signum}; cleaning up", file=sys.stderr, flush=True)
    raise SystemExit(128 + signum)


@contextlib.contextmanager
def _defer_sigterm():
    """Record SIGTERM inside launches instead of raising it immediately.

    A launch whose popen forked but whose handle is not published yet
    would orphan a running, untracked endpoint if SystemExit struck
    between the two. Recording defers the exit until the handle is
    published, so shutdown always covers launched trees. A mask would
    persist across fork into the endpoint itself; a recording handler
    resets to default on exec instead.
    """
    pending = []
    previous = signal.signal(signal.SIGTERM, lambda s, f: pending.append(s))
    try:
        yield pending
    finally:
        signal.signal(signal.SIGTERM, previous)
    # Unreached when the body raised: a failed launch takes precedence
    # over a concurrently recorded signal.
    if pending:
        raise SystemExit(128 + signal.SIGTERM)


def _tail(path: Path, lines: int = 20) -> str:
    try:
        return "\n".join(path.read_text().splitlines()[-lines:])
    except OSError:
        return "<log unavailable>"


def _wait_for_tcp(topo, host: str, port: int, proc, timeout: float) -> str:
    """Poll a TCP connect from the server's namespace; fail fast on exit."""
    deadline = time.monotonic() + timeout
    while True:
        if topo.check_tcp("a", host, port):
            return "ready"
        if proc.poll() is not None:
            return "exited"
        if time.monotonic() >= deadline:
            return "timeout"
        time.sleep(0.2)


def _stop(proc, grace: float, what: str, problems: list) -> None:
    """Terminate, escalate to kill, and record when neither works."""
    proc.terminate()
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            problems.append(f"{what} survived SIGKILL")


class _Abort(Exception):
    """A conversation phase failed; problems already says why."""


@dataclass
class _Paths:
    server_json: Path
    client_json: Path
    server_log: Path
    client_log: Path


@dataclass
class _Live:
    """Endpoint handles, published incrementally as they launch.

    A phase that returned both handles at once would orphan them if a
    signal landed between the launch and the return; publishing each
    handle immediately after its popen keeps the finally shutdown exact.
    """

    server: object = None
    client: object = None


def _launch_endpoints(spec, rdma_gen, topo, start_s, paths, summary, problems, live):
    """Start the server, await control readiness, and start the client."""
    addrs = {"server_addr": topo.server_addr, "client_addr": topo.client_addr}
    server_cmd = [
        rdma_gen,
        *[a.format(**addrs) for a in spec.server_args],
        f"--json_path={paths.server_json}",
    ]
    summary.append("server: " + " ".join(server_cmd))
    try:
        with _defer_sigterm():
            server = topo.popen("a", server_cmd, str(paths.server_log))
            live.server = server
    except OSError as e:
        problems.append(f"server did not start: {e}")
        raise _Abort from None
    ready = _wait_for_tcp(
        topo, topo.server_addr, spec.control_port, server, spec.ready_timeout_s
    )
    if ready != "ready":
        problems.append(
            f"server never served ({ready} probing "
            f"{topo.server_addr}:{spec.control_port}); log tail:\n"
            f"{_tail(paths.server_log)}"
        )
        raise _Abort from None
    print(f"server serving after {time.monotonic() - start_s:.1f}s", flush=True)
    client_cmd = [
        rdma_gen,
        *[a.format(**addrs) for a in spec.client_args],
        f"--json_path={paths.client_json}",
    ]
    summary.append("client: " + " ".join(client_cmd))
    try:
        with _defer_sigterm():
            client = topo.popen("b", client_cmd, str(paths.client_log))
            live.client = client
    except (OSError, netns_topology.TopologyError) as e:
        problems.append(f"client did not start: {e}")
        raise _Abort from None


def _await_and_verify(spec, start_s, server, client, paths, summary, problems):
    """Wait for client completion and server exit; check exits and output."""
    remaining_s = spec.deadline_s - (time.monotonic() - start_s)
    try:
        client.wait(timeout=max(remaining_s, 1))
    except subprocess.TimeoutExpired:
        problems.append(f"client did not finish within {spec.deadline_s:.0f}s")
        raise _Abort from None
    try:
        server.wait(timeout=spec.stop_grace_s)
    except subprocess.TimeoutExpired:
        problems.append(
            f"server did not stop within {spec.stop_grace_s:.0f}s of client completion"
        )
        raise _Abort from None
    summary.append(f"server exit: {server.returncode}")
    summary.append(f"client exit: {client.returncode}")
    if server.returncode != spec.expected_server_exit:
        problems.append(
            f"server exit {server.returncode}, expected "
            f"{spec.expected_server_exit}; log tail:\n{_tail(paths.server_log)}"
        )
    if client.returncode != spec.expected_client_exit:
        problems.append(
            f"client exit {client.returncode}, expected "
            f"{spec.expected_client_exit}; log tail:\n{_tail(paths.client_log)}"
        )
    for label, path, expected in (
        ("server", paths.server_json, spec.expected_server_cqes),
        ("client", paths.client_json, spec.expected_client_cqes),
    ):
        try:
            data = rdmagen_output.load_run_output(path)
        except rdmagen_output.OutputError as e:
            problems.append(f"{label}: {e}")
            continue
        problems.extend(
            f"{label}: {p}"
            for p in rdmagen_output.check_success(data, expected, spec.expected_op)
        )


def _stop_endpoints(server, client, problems) -> None:
    """Bounded stop for endpoints still alive; exited ones are untouched."""
    for what, proc in (("server", server), ("client", client)):
        if proc is not None and proc.poll() is None:
            _stop(proc, 10, what, problems)


def _finalize_summary(topo, artifact_dir, summary, problems, finished) -> None:
    """Append teardown errors and the verdict; write and echo the summary."""
    for error in getattr(topo, "cleanup_errors", []):
        problems.append(error)
    if not finished and not problems:
        problems.append("interrupted before completion")
    summary.append("PASS" if finished and not problems else "FAIL")
    summary.extend(problems)
    try:
        (artifact_dir / "summary.txt").write_text("\n".join(summary) + "\n")
    except OSError as e:
        print(f"could not write summary: {e}", file=sys.stderr, flush=True)
    for line in summary:
        print(line, flush=True)


def run_conversation(
    spec: ConversationSpec, rdma_gen: str, mode: str, artifact_dir: Path
) -> ConversationResult:
    """Run one client/server conversation; artifacts stay in artifact_dir."""
    signal.signal(signal.SIGTERM, _on_sigterm)
    version, problems = check_compatible(rdma_gen)
    print(f"binary: {rdma_gen}", flush=True)
    print(f"version: {version}", flush=True)
    if problems:
        return ConversationResult(False, problems)
    paths = _Paths(
        server_json=artifact_dir / "server.json",
        client_json=artifact_dir / "client.json",
        server_log=artifact_dir / "server.log",
        client_log=artifact_dir / "client.log",
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    for fresh in (
        paths.server_json,
        paths.client_json,
        paths.server_log,
        paths.client_log,
    ):
        fresh.unlink(missing_ok=True)
    summary = [f"binary: {rdma_gen}", f"version: {version}"]
    try:
        topo = netns_topology.topology_for(mode)
    except netns_topology.TopologyError as e:
        return ConversationResult(False, [f"topology failed: {e}"])
    finished = False
    try:
        with topo:
            print(
                f"server {topo.server_addr} client {topo.client_addr}",
                flush=True,
            )
            live = _Live()
            try:
                start_s = time.monotonic()
                _launch_endpoints(
                    spec,
                    rdma_gen,
                    topo,
                    start_s,
                    paths,
                    summary,
                    problems,
                    live,
                )
                _await_and_verify(
                    spec,
                    start_s,
                    live.server,
                    live.client,
                    paths,
                    summary,
                    problems,
                )
                finished = True
            except _Abort:
                pass
            finally:
                _stop_endpoints(live.server, live.client, problems)
    except netns_topology.TopologyError as e:
        problems.append(f"topology failed: {e}")
    finally:
        _finalize_summary(topo, artifact_dir, summary, problems, finished)
    return ConversationResult(finished and not problems, problems)

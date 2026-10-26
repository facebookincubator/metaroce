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

"""SEND/RECV smoke: one small conversation, completions checked both ends.

Runs a libsoftmetaroce client and server in isolated namespaces and
requires every expected completion, successful, on both sides. Stdlib only.
"""

import argparse
import sys
from pathlib import Path

try:
    from neteng.ai.libsoftmetaroce.test.netns.netns_runner import (
        ConversationSpec,
        run_conversation,
    )
except ImportError:
    # Resolves only for a direct run (script dir on sys.path), invisible to
    # the build-system source database the type checker searches.
    from netns_runner import (  # pyrefly: ignore [missing-import]
        ConversationSpec,
        run_conversation,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one SEND/RECV conversation and check completions."
    )
    parser.add_argument("--rdma-gen", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--mode", required=True, choices=("rootless", "privileged"))
    parser.add_argument("--message-size", type=int, default=64000)
    parser.add_argument("--num-messages", type=int, default=10)
    args = parser.parse_args()
    common = [
        "--op_type=send_recv",
        "--verbs_mode=libsoftmetaroce",
        "--ib_dev=",
        "--mtu=IBV_MTU_4096",
        f"--message_size={args.message_size}",
        "--libsoftmetaroce_ignore_modify_psn=false",
        f"--buffer_cap={args.num_messages}",
        "--rc_cm_port=18515",
    ]
    spec = ConversationSpec(
        name="send_recv_smoke",
        server_args=[
            *common,
            f"--num_completions_expected={args.num_messages}",
        ],
        client_args=[
            *common,
            f"--num_messages={args.num_messages}",
            "--rc_servers={server_addr}#1",
            "--rc_connect_ports=18515",
        ],
        expected_server_cqes=args.num_messages,
        expected_client_cqes=args.num_messages,
        control_port=18515,
        expected_op="SendReceive",
    )
    result = run_conversation(spec, args.rdma_gen, args.mode, Path(args.artifact_dir))
    print(f"SMOKE_{'PASS' if result.ok else 'FAIL'}", flush=True)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())

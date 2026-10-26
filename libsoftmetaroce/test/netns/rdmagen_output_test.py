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

"""Checks for rdmagen_output: malformed output fails, good output passes.

Runs under plain python3 via CTest; no namespaces needed. Exit 0 iff every
case behaves. The good fixture mirrors the writer's per-CQE contract.
"""

import json
import tempfile
from pathlib import Path

try:
    import neteng.ai.libsoftmetaroce.test.netns.rdmagen_output as o
except ImportError:
    # Resolves only for a direct run (script dir on sys.path), invisible to
    # the build-system source database the type checker searches.
    import rdmagen_output as o  # pyrefly: ignore [missing-import]


def good_cqe(index):
    return {
        "operation_type": "SendReceive",
        "wr_id": index,
        "seq_num": index,
        "byte_len": 64000,
        "qp_num": 2,
        "end_time_us": 1000 + index,
    }


def write_tmp(payload):
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as tmp:
        tmp.write(payload if isinstance(payload, str) else json.dumps(payload))
    return Path(tmp.name)


def expect_problems(name, data, count, op="SendReceive"):
    found = o.check_success(data, count, op)
    assert found, f"{name}: expected problems, got none"
    print(f"{name}: rejected ({len(found)} problems)", flush=True)


def main():
    with tempfile.NamedTemporaryFile(suffix=".json") as tmp:
        missing = Path(tmp.name)
    try:
        o.load_run_output(missing)
    except o.OutputError:
        print("missing file: rejected", flush=True)
    else:
        raise AssertionError("missing file: expected OutputError")
    for name, payload in (
        ("unparseable", "{not json"),
        ("empty run list", []),
        ("two runs", [{"cqes": []}, {"cqes": []}]),
        ("non-object", 42),
    ):
        try:
            o.load_run_output(write_tmp(payload))
        except o.OutputError:
            print(f"{name}: rejected", flush=True)
        else:
            raise AssertionError(f"{name}: expected OutputError")
    assert o.load_run_output(write_tmp([{"cqes": []}])) == {"cqes": []}
    print("single run list: accepted", flush=True)

    good = {"cqes": [good_cqe(i) for i in range(10)]}
    assert o.check_success(good, 10, "SendReceive") == []
    print("good output: accepted", flush=True)
    expect_problems("empty completions", {"cqes": [{}] * 10}, 10)
    expect_problems(
        "wrong operation",
        {"cqes": [{**good_cqe(0), "operation_type": "Write"}]},
        1,
    )
    expect_problems("string wr_id", {"cqes": [{**good_cqe(0), "wr_id": "0"}]}, 1)
    expect_problems(
        "boolean byte_len", {"cqes": [{**good_cqe(0), "byte_len": True}]}, 1
    )
    expect_problems(
        "present status",
        {"cqes": [{**good_cqe(0), "status": "remote access error"}]},
        1,
    )
    expect_problems("count mismatch", good, 9)
    expect_problems("missing list", {}, 10)
    expect_problems(
        "recorded errors",
        {**good, "results": {"error_cqes": [good_cqe(0)]}},
        10,
    )
    print("OUTPUT_CHECKS_PASS", flush=True)


if __name__ == "__main__":
    main()

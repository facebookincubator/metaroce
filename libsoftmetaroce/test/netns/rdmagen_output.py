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

"""rdma_gen JSON output checks shared by the namespace smoke tests.

Stdlib only. Unlike `rdmagen_expectation.py`, which reads the first of
possibly several appended runs, a list here must hold exactly one run;
anything else is ambiguous. Stricter throughout: a missing file, unparseable
output, an absent key, or a malformed completion fails.
"""

import json
from pathlib import Path


class OutputError(Exception):
    """rdma_gen output is missing or malformed."""


def load_run_output(path: Path) -> dict:
    """Load one run's JSON object, failing loudly on anything unexpected."""
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        raise OutputError(f"rdma_gen output missing: {path}") from None
    except (json.JSONDecodeError, OSError) as e:
        raise OutputError(f"rdma_gen output unreadable: {path}: {e}") from None
    if isinstance(raw, list):
        if len(raw) != 1:
            raise OutputError(
                f"rdma_gen output holds {len(raw)} runs, expected exactly one: {path}"
            )
        raw = raw[0]
    if not isinstance(raw, dict):
        raise OutputError(f"rdma_gen output is not an object: {path}")
    return raw


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_cqe(cqe: dict, index: int, expected_op: str) -> list[str]:
    """One completion is well-formed, of the expected op, and successful."""
    problems = []
    op = cqe.get("operation_type")
    if op != expected_op:
        problems.append(
            f"completion {index} has operation_type {op!r}, expected {expected_op!r}"
        )
    for field in ("wr_id", "seq_num", "byte_len", "qp_num", "end_time_us"):
        if not _is_int(cqe.get(field)):
            problems.append(
                f"completion {index} has non-integer {field}: {cqe.get(field)!r}"
            )
    # The writer omits status on success; any present status failed.
    status = cqe.get("status")
    if status is not None:
        problems.append(
            f"completion {index} failed: wr_id={cqe.get('wr_id')} status={status}"
        )
    return problems


def check_success(data: dict, expected_cqes: int, expected_op: str) -> list[str]:
    """Every completion present, well-formed, of the expected op, successful."""
    cqes = data.get("cqes")
    if not isinstance(cqes, list):
        return [f"missing completion list (expected {expected_cqes} entries)"]
    problems = []
    if len(cqes) != expected_cqes:
        problems.append(f"expected {expected_cqes} completions, found {len(cqes)}")
    for index, cqe in enumerate(cqes):
        if not isinstance(cqe, dict):
            problems.append(f"completion {index} is not an object: {cqe!r}")
            continue
        problems.extend(_check_cqe(cqe, index, expected_op))
    results = data.get("results", {})
    if not isinstance(results, dict):
        problems.append("results section is not an object")
    elif "error_cqes" in results:
        errors = results["error_cqes"]
        if not isinstance(errors, list):
            problems.append("error_cqes section is not a list")
        elif errors:
            problems.append(f"{len(errors)} error completions recorded")
    return problems


def check_error(data: dict) -> list[str]:
    """At least one error completion recorded (expected-error scenarios)."""
    results = data.get("results")
    if not isinstance(results, dict):
        return ["missing results section"]
    errors = results.get("error_cqes")
    if not isinstance(errors, list) or not errors:
        return ["expected error completions, found none"]
    return []

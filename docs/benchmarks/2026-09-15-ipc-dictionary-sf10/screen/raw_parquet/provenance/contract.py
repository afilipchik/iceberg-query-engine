"""Versioned evidence contract and fail-closed latency gates (stdlib only).

No inferred membership: the manifest declares every expected query/track/session.
An invalid or incomplete cell never contributes a partial aggregate score.
"""
import hashlib
import json
import math
import random
import statistics
from collections import defaultdict
from pathlib import Path
from .gpu_residency import resident_sample_issues

VERSION = 1
CPU_TRACKS = ("raw_parquet", "native", "iceberg", "lance")
STATUSES = frozenset(("completed", "query_error", "unsupported", "timeout", "oom",
                      "refused", "crash", "validation_error", "startup_error"))
TIMING = "embedded_parse_to_arrow_consumed"


class ContractError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ContractError(message)


def positive(value):
    return (type(value) in (int, float) and math.isfinite(value) and value > 0)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def unique_strings(values, label):
    require(isinstance(values, list) and values, f"{label}: nonempty list required")
    require(all(isinstance(x, str) and x for x in values), f"{label}: string IDs required")
    require(len(values) == len(set(values)), f"{label}: duplicate IDs")


def validate_manifest(m):
    require(isinstance(m, dict), "manifest must be an object")
    require(m.get("version") == VERSION, "unsupported manifest version")
    unique_strings(m.get("tracks"), "tracks")
    require(set(m["tracks"]) <= set(CPU_TRACKS) | {"ipc_cache", "gpu_assisted", "gpu_control", "decoded_ipc", "gpu"},
            "unknown track")
    policy = m.get("gpu_residency_policy", "mixed")
    require(policy in ("mixed", "required"), "unknown GPU residency policy")
    environment = m.get("environment", {})
    require(isinstance(environment, dict), "invalid benchmark environment")
    setup = environment.get("setup", {})
    require(isinstance(setup, dict), "invalid GPU residency setup")
    if policy == "required":
        require(m["tracks"] == ["gpu"] and m.get("cache_state") == "resident",
                "required residency needs GPU-only resident manifest")
        require(setup.get("gpu_residency_required") is True
                and setup.get("host_arrow_preloaded") is True,
                "required residency needs matching worker policy and preloaded inputs")
    else:
        require(not setup.get("gpu_residency_required", False), "worker requires residency but manifest does not")
    unique_strings(m.get("sessions"), "sessions")
    require(type(m.get("samples_per_query")) is int and m["samples_per_query"] > 0,
            "samples_per_query must be positive integer")
    require(m.get("timing_boundary") == TIMING, "unmatched or unknown timing boundary")
    require(m.get("cache_state") in ("warm_host", "fresh_process_warm_os", "verified_cold", "resident"),
            "explicit cache_state required")
    require(isinstance(m.get("profile"), str) and m["profile"], "profile required")
    require(isinstance(m.get("workloads"), list) and m["workloads"], "workloads required")
    require(all(isinstance(w, dict) for w in m["workloads"]), "workloads must be objects")
    unique_strings([w.get("id") for w in m["workloads"]], "workload IDs")
    for workload in m["workloads"]:
        require(isinstance(workload.get("queries"), list), "queries required")
        require(all(isinstance(q, dict) for q in workload["queries"]), "queries must be objects")
        unique_strings([q.get("id") for q in workload["queries"]], "query IDs")
        for q in workload["queries"]:
            require(isinstance(q.get("sql_sha256"), str) and len(q["sql_sha256"]) == 64
                    and all(c in "0123456789abcdef" for c in q["sql_sha256"]), "SQL sha256 required")
            require(q.get("result_policy") in ("bag", "ordered", "limit_ties", "unordered_limit"), "result_policy required")
            policy = q["result_policy"]
            require(type(q.get("offset_rows", 0)) is int and q.get("offset_rows", 0) >= 0, "nonnegative offset_rows required")
            if policy in ("limit_ties", "unordered_limit"):
                require(type(q.get("limit_rows")) is int and q["limit_rows"] >= 0, "nonnegative limit_rows required")
            else:
                require(q.get("offset_rows", 0) == 0, "OFFSET requires a limited policy")
            if policy == "unordered_limit":
                require(not q.get("order_by"), "unordered LIMIT cannot specify ordering")
            mapping = q.get("oracle_output_columns")
            if mapping is not None:
                require(isinstance(mapping, list) and mapping and all(type(i) is int and i >= 0 for i in mapping)
                        and len(set(mapping)) == len(mapping), "invalid oracle output mapping")
    for key in ("source_revision", "engine_binary_sha256", "duckdb_version", "dataset_manifest_sha256"):
        require(isinstance(m.get(key), str) and bool(m[key]), f"{key} required")
    return m


def cells(m):
    return {(w["id"], q["id"], track, session)
            for w in m["workloads"] for q in w["queries"]
            for track in m["tracks"] for session in m["sessions"]}


def row_key(r):
    return tuple(r.get(k) for k in ("workload", "query", "track", "session"))


def aggregates(pairs_by_query):
    medians = [(statistics.median(p[0] for p in pairs),
                statistics.median(p[1] for p in pairs)) for pairs in pairs_by_query]
    ratios = [e / d for e, d in medians]
    return {"geomean": math.exp(statistics.mean(map(math.log, ratios))),
            "suite_ratio": sum(e for e, _ in medians) / sum(d for _, d in medians),
            "engine_total_ms": sum(e for e, _ in medians),
            "duckdb_total_ms": sum(d for _, d in medians),
            "wins": sum(r < 1 for r in ratios), "queries": len(ratios),
            "worst_ratio": max(ratios)}


def intervals(query_sessions, repetitions=2000):
    """Paired hierarchical bootstrap: resample sessions, then pairs within them.

    Session draws are shared across queries to retain session-level host effects.
    Query membership is fixed; slow queries are never sampled out of the suite.
    """
    rng = random.Random(20260905)
    values = {"geomean": [], "suite_ratio": []}
    n_sessions = len(query_sessions[0])
    for _ in range(repetitions):
        sessions = [rng.randrange(n_sessions) for _ in range(n_sessions)]
        sampled = []
        for per_session in query_sessions:
            pairs = []
            for i in sessions:
                block = per_session[i]
                pairs.extend(block[rng.randrange(len(block))] for _ in block)
            sampled.append(pairs)
        result = aggregates(sampled)
        for k in values:
            values[k].append(result[k])
    return {k: [sorted(v)[int(.025 * (len(v) - 1))], sorted(v)[int(.975 * (len(v) - 1))]]
            for k, v in values.items()}


def leadership(a):
    return (a["geomean"] <= .90 and a["suite_ratio"] <= .90
            and a["wins"] > a["queries"] / 2 and a["worst_ratio"] <= 2)


def report(manifest, rows, repetitions=2000):
    m = validate_manifest(manifest)
    expected = cells(m)
    indexed = defaultdict(dict)
    issues = []
    policies = {(w["id"], q["id"]): q["result_policy"]
                for w in m["workloads"] for q in w["queries"]}
    query_contracts = {(w["id"], q["id"]): q for w in m["workloads"] for q in w["queries"]}
    sql_hashes = {(w["id"], q["id"]): q["sql_sha256"]
                  for w in m["workloads"] for q in w["queries"]}
    for number, r in enumerate(rows, 1):
        if not isinstance(r, dict) or not all(isinstance(r.get(k), str)
                for k in ("workload", "query", "track", "session")):
            issues.append(f"row {number}: invalid sample object or cell IDs")
            continue
        key = row_key(r)
        if key not in expected:
            issues.append(f"row {number}: unexpected cell {key}")
            continue
        iteration = r.get("iteration")
        if type(iteration) is not int or not 1 <= iteration <= m["samples_per_query"]:
            issues.append(f"row {number}: invalid iteration")
            continue
        if iteration in indexed[key]:
            issues.append(f"row {number}: duplicate sample {key}/{iteration}")
            continue
        indexed[key][iteration] = r
        failures = []
        if not all(isinstance(r.get(k), dict) for k in ("engine", "duckdb", "comparison")):
            issues.append(f"row {number}: engine, duckdb and comparison must be objects")
            continue
        if r.get("sql_sha256") != sql_hashes[key[:2]]:
            failures.append("SQL hash mismatch")
        if r.get("timing_boundary") != m["timing_boundary"]:
            failures.append("timing boundary mismatch")
        if r.get("comparison", {}).get("ok") is not True:
            failures.append("answer not validated")
        if r.get("comparison", {}).get("policy") != policies[key[:2]]:
            failures.append("result policy mismatch")
        query = query_contracts[key[:2]]
        if query["result_policy"] == "unordered_limit" or query.get("offset_rows", 0) != 0 or query.get("oracle_output_columns") is not None:
            comparison = r["comparison"]
            expected_contract = {"limit_rows": query.get("limit_rows"), "offset_rows": query.get("offset_rows", 0),
                                 "oracle_output_columns": query.get("oracle_output_columns"), "oracle_order_by": query.get("order_by", [])}
            if comparison.get("expected_is_complete") is not True or any(comparison.get(field) != value for field, value in expected_contract.items()):
                failures.append("oracle slice contract mismatch")
        for side in ("engine", "duckdb"):
            evidence = r.get(side, {})
            if evidence.get("status") != "completed":
                failures.append(f"{side}: {evidence.get('status', 'missing status')}")
            elif not positive(evidence.get("ms")):
                failures.append(f"{side}: invalid elapsed time")
        calibration = r.get("calibration_ms")
        if not (isinstance(calibration, list) and len(calibration) == 3 and all(map(positive, calibration))):
            failures.append("invalid fresh calibration")
        elif positive(r.get("engine", {}).get("ms")):
            if r["engine"]["ms"] > 10 * statistics.median(calibration):
                failures.append("exceeded measured 10x ceiling")
        if m.get("gpu_residency_policy", "mixed") == "required":
            failures.extend(resident_sample_issues(r))
        if failures:
            issues.append(f"row {number} {key}/{iteration}: " + "; ".join(failures))
    for key in sorted(expected):
        missing = set(range(1, m["samples_per_query"] + 1)) - indexed[key].keys()
        if missing:
            issues.append(f"missing {key}: iterations {sorted(missing)}")
    result = {"version": VERSION, "complete": not issues, "issues": issues, "groups": [],
              "latency_leadership": False,
              "certification": "incomplete: resource, concurrency and regression gates are separate"}
    if issues:
        return result
    for w in m["workloads"]:
        for track in m["tracks"]:
            blocks = [[[ (indexed[(w["id"], q["id"], track, session)][i]["engine"]["ms"],
                           indexed[(w["id"], q["id"], track, session)][i]["duckdb"]["ms"])
                         for i in range(1, m["samples_per_query"] + 1)]
                       for session in m["sessions"]] for q in w["queries"]]
            per_session = [aggregates([b[s] for b in blocks]) for s in range(len(m["sessions"]))]
            a = aggregates([[pair for session in b for pair in session] for b in blocks])
            a["query_ratios"] = {q["id"]: aggregates([[pair for block in b for pair in block]])["suite_ratio"]
                                 for q, b in zip(w["queries"], blocks)}
            a["ci95"] = intervals(blocks, repetitions)
            a["per_session"] = dict(zip(m["sessions"], per_session))
            a["workload"], a["track"] = w["id"], track
            a["leadership"] = (track in CPU_TRACKS and len(m["sessions"]) >= 3
                               and m["samples_per_query"] >= 10 and leadership(a)
                               and all(map(leadership, per_session))
                               and all(v[1] < 1 for v in a["ci95"].values()))
            result["groups"].append(a)
    result["latency_leadership"] = (m["profile"] == "main_latency"
        and {"tpch_sf10", "job_full", "clickbench_full"} <= {w["id"] for w in m["workloads"]}
        and set(CPU_TRACKS) <= set(m["tracks"])
        and all(g["leadership"] for g in result["groups"] if g["track"] in CPU_TRACKS))
    return result

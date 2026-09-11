"""Typed, disk-backed Arrow comparison. Does not load whole results into RAM.

Strict identical-schema exact bags can use a bounded DuckDB full-key aggregate
over lossless Parquet staging. Other exact rows are counted in SQLite. Floating rows are matched within exact-column
buckets by augmenting paths, avoiding order-dependent greedy tolerance matches.
Large ambiguous float buckets fail explicitly rather than exceeding the budget.
"""
import json
import math
import sqlite3
import tempfile
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa


class ValidationError(ValueError):
    pass


def family(t):
    if pa.types.is_dictionary(t):
        return family(t.value_type)
    if pa.types.is_integer(t) or pa.types.is_decimal(t):
        return "exact_number"
    if pa.types.is_floating(t):
        return "float"
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return "string"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "timestamp:" + str(t.tz)
    if pa.types.is_boolean(t):
        return "bool"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary"
    raise ValidationError(f"unsupported logical type: {t}")


def exact(value):
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, (int, Decimal)):
        # Decimal.normalize uses context precision and can round large values.
        # as_integer_ratio is exact for all finite integer/decimal values.
        n, d = value.as_integer_ratio()
        return ["number", str(n), str(d)]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, bytes):
        return ["binary", value.hex()]
    if isinstance(value, (date, datetime, time)):
        return ["temporal", value.isoformat()]
    raise ValidationError(f"unsupported exact value: {type(value).__name__}")


def encoded(values):
    return json.dumps([exact(v) for v in values], ensure_ascii=True, separators=(",", ":"))


def float_equal(a, b, rel, absolute):
    if a is None or b is None:
        return a is b
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return math.isclose(a, b, rel_tol=rel, abs_tol=absolute)


def _match(actual, expected, rel, absolute, owner=None):
    """Return right->left assignment covering all left rows, or None."""
    if len(actual) > len(expected):
        return None
    owner = {} if owner is None else dict(owner)
    for start in range(len(actual)):
        if start in owner.values():
            continue
        queue, visited_left, visited_right = [start], {start}, set()
        parent = {}
        free = None
        for left in queue:
            for right, candidate in enumerate(expected):
                if right in visited_right:
                    continue
                if not all(float_equal(a, b, rel, absolute) for a, b in zip(actual[left], candidate)):
                    continue
                visited_right.add(right)
                parent[right] = left
                if right not in owner:
                    free = right
                    break
                previous = owner[right]
                if previous not in visited_left:
                    visited_left.add(previous)
                    queue.append(previous)
            if free is not None:
                break
        if free is None:
            return None
        while True:
            left = parent[free]
            previous_right = next((r for r, l in owner.items() if l == left), None)
            owner[free] = left
            if previous_right is None:
                break
            free = previous_right
    return owner


def matching(actual, expected, rel, absolute, required=None):
    """Match all actual rows while covering every mandatory expected row.

    Optional expected rows are used only for an ORDER BY/LIMIT boundary tie.
    Seed mandatory coverage first, then augment without unmatching any covered
    expected vertex. Greedy tolerance matching is incorrect in either phase.
    """
    if required is None:
        required = [True] * len(expected)
    mandatory = [i for i, needed in enumerate(required) if needed]
    seed = _match([expected[i] for i in mandatory], actual, rel, absolute)
    if seed is None:
        return False
    owner = {mandatory[left]: right for right, left in seed.items()}
    return _match(actual, expected, rel, absolute, owner) is not None


def compare_key(left, right, order_by):
    for spec in order_by:
        i = spec["column"]
        a, b = left[i], right[i]
        if a is None or b is None:
            if a is b:
                continue
            return (-1 if a is None else 1) * (1 if spec["nulls"] == "first" else -1)
        if isinstance(a, float) and math.isnan(a):
            c = 0 if isinstance(b, float) and math.isnan(b) else 1
        elif isinstance(b, float) and math.isnan(b):
            c = -1
        else:
            c = (a > b) - (a < b)
        if c:
            return c if spec["direction"] == "asc" else -c
    return 0


def rows(reader):
    for batch in reader:
        for offset in range(0, batch.num_rows, 4096):
            chunk = batch.slice(offset, 4096)
            columns = [c.to_pylist() for c in chunk.columns]
            if not columns:
                for _ in range(chunk.num_rows):
                    yield ()
            else:
                yield from zip(*columns)



# Limits native equality work per call. Slices share the current IPC batch's
# backing buffers: this bounds comparison segments, not arbitrary IPC batch
# decode size. The caller must still run inside the validation memory cap.
_ALIGNED_SLICE_ROWS = 4096


def aligned_exact_streams(actual, expected):
    """Prove complete aligned equality or decline to the exact bag comparator.

    Only called after supported logical-domain validation and exact schema
    equality. Batch order is never assumed: inequality declines rather than
    declaring a bag mismatch. No Python scalar conversion or fingerprints.
    """
    detail = {"attempted": True, "matched_rows": 0, "segments": 0,
              "slice_rows": _ALIGNED_SLICE_ROWS, "peak_segment_rows": 0}
    batches = [None, None]
    offsets = [0, 0]
    readers = [actual, expected]
    while True:
        for side in (0, 1):
            while batches[side] is None or offsets[side] == batches[side].num_rows:
                batches[side] = next(readers[side], None)
                offsets[side] = 0
                if batches[side] is None or batches[side].num_rows:
                    break
        if any(batch is None for batch in batches):
            if all(batch is None for batch in batches):
                detail["equal"] = True
            else:
                detail.update(equal=False, decline_reason="different stream lengths")
            return detail
        count = min(_ALIGNED_SLICE_ROWS, *(batch.num_rows - offset for batch, offset in zip(batches, offsets)))
        detail["segments"] += 1
        detail["peak_segment_rows"] = max(detail["peak_segment_rows"], count)
        try:
            for column in range(batches[0].num_columns):
                left = batches[0].column(column).slice(offsets[0], count)
                right = batches[1].column(column).slice(offsets[1], count)
                if not left.equals(right):
                    detail.update(equal=False, decline_reason="aligned values or encodings differ")
                    return detail
        except pa.ArrowNotImplementedError:
            detail.update(equal=False, decline_reason="native equality unsupported")
            return detail
        detail["matched_rows"] += count
        offsets = [offset + count for offset in offsets]


def compare_files(actual_path, expected_path, *, scratch, policy="bag", order_by=None,
                  rel_tol=1e-10, abs_tol=1e-8, max_float_bucket=2000, limit_rows=None, expected_is_complete=False,
                  offset_rows=0, oracle_output_columns=None):
    if policy == "unordered_limit" or type(offset_rows) is not int or offset_rows != 0 or oracle_output_columns is not None:
        return compare_slice_files(actual_path, expected_path, scratch=scratch, policy=policy,
            order_by=order_by, rel_tol=rel_tol, abs_tol=abs_tol, max_float_bucket=max_float_bucket,
            limit_rows=limit_rows, offset_rows=offset_rows, expected_is_complete=expected_is_complete,
            oracle_output_columns=oracle_output_columns)
    detail = {"ok": False, "policy": policy, "status": "validation_error"}
    try:
        if policy not in ("bag", "ordered", "limit_ties"):
            raise ValidationError("unknown result policy")
        limited = policy == "limit_ties"
        if limited and (expected_is_complete is not True or type(limit_rows) is not int or limit_rows < 0):
            raise ValidationError("LIMIT ties require an explicitly complete, unlimited ordered oracle and nonnegative limit_rows")
        if not all(type(x) in (int, float) and math.isfinite(x) and x >= 0 for x in (rel_tol, abs_tol)):
            raise ValidationError("invalid float tolerance")
        if type(max_float_bucket) is not int or max_float_bucket < 1:
            raise ValidationError("invalid float bucket bound")
        with pa.memory_map(str(actual_path), "r") as af, pa.memory_map(str(expected_path), "r") as ef:
            ar, er = pa.ipc.open_stream(af), pa.ipc.open_stream(ef)
            detail["actual_schema"], detail["expected_schema"] = str(ar.schema), str(er.schema)
            types = [family(f.type) for f in ar.schema]
            if types != [family(f.type) for f in er.schema]:
                raise ValidationError("incompatible logical schemas")
            if policy == "bag" and rel_tol == 0 and abs_tol == 0:
                if ar.schema.equals(er.schema, check_metadata=True):
                    # Buffered readers release completed batches instead of
                    # retaining an ever-growing file-backed resident mapping.
                    # The outer mappings have only read schema metadata here.
                    with pa.OSFile(str(actual_path), "rb") as fast_af, pa.OSFile(str(expected_path), "rb") as fast_ef:
                        fast_ar, fast_er = pa.ipc.open_stream(fast_af), pa.ipc.open_stream(fast_ef)
                        if (not fast_ar.schema.equals(ar.schema, check_metadata=True)
                            or not fast_er.schema.equals(er.schema, check_metadata=True)):
                            raise ValidationError("input schema changed during validation")
                        aligned = aligned_exact_streams(fast_ar, fast_er)
                    detail["aligned_exact"] = aligned
                    if aligned["equal"]:
                        detail.update(ok=True, status="validated", comparison_strategy="arrow_aligned_exact",
                                      actual_rows=aligned["matched_rows"], expected_rows=aligned["matched_rows"])
                        return detail
                    # Restart both streams: the fallback must inspect every row,
                    # including the prefix already compared successfully.
                    af.seek(0)
                    ef.seek(0)
                    ar, er = pa.ipc.open_stream(af), pa.ipc.open_stream(ef)
                else:
                    detail["aligned_exact"] = {"attempted": False, "decline_reason": "schemas are not identical"}
                from .exact_bag import try_exact_bag
                exact_bag = try_exact_bag(actual_path, expected_path, scratch=scratch)
                detail["exact_bag"] = exact_bag
                if exact_bag["attempted"]:
                    # A completed full-key proof or an explicit failure is final.
                    # Only an unsupported domain may restart the old comparator;
                    # resource errors must not be hidden by another expensive run.
                    detail.update({key: value for key, value in exact_bag.items()
                                   if key not in ("attempted", "decline_reason")})
                    detail["comparison_strategy"] = "duckdb_parquet_exact_bag"
                    return detail
                detail["comparison_strategy"] = "sqlite_exact_bag"
            if policy in ("ordered", "limit_ties"):
                if not isinstance(order_by, list) or not order_by:
                    raise ValidationError("ordered result requires explicit output key columns")
                for spec in order_by:
                    if (not isinstance(spec, dict) or type(spec.get("column")) is not int or not 0 <= spec["column"] < len(types)
                        or spec.get("direction") not in ("asc", "desc")
                        or spec.get("nulls") not in ("first", "last")):
                        raise ValidationError("invalid order key")
            boundary, oracle_count = None, 0
            if limited:
                # First bounded pass finds the cutoff; the second streams all
                # eligible rows, including ties missing from a limited oracle.
                previous = None
                for row in rows(er):
                    if previous is not None and compare_key(previous, row, order_by) > 0:
                        raise ValidationError("unlimited oracle is not ordered")
                    previous = row
                    oracle_count += 1
                    if oracle_count == limit_rows:
                        boundary = row
                ef.seek(0)
                er = pa.ipc.open_stream(ef)
                detail["oracle_rows"] = oracle_count
                detail["limit_rows"] = limit_rows
            floats = [i for i, t in enumerate(types) if t == "float"]
            fixed = [i for i, t in enumerate(types) if t != "float"]
            with tempfile.TemporaryDirectory(prefix="compare-", dir=scratch) as directory:
                db = sqlite3.connect(str(Path(directory) / "rows.sqlite"))
                try:
                    db.execute("PRAGMA cache_size=-8192")
                    db.execute("PRAGMA temp_store=FILE")
                    db.execute("CREATE TABLE exact_rows (key TEXT PRIMARY KEY, actual INTEGER NOT NULL, expected INTEGER NOT NULL, mandatory INTEGER NOT NULL)")
                    db.execute("CREATE TABLE floating (key TEXT, side INTEGER, value TEXT, mandatory INTEGER)")
                    db.execute("CREATE INDEX floating_key ON floating(key, side)")
                    counts = []
                    for side, reader in enumerate((ar, er)):
                        previous = None
                        count = 0
                        for row in rows(reader):
                            if policy in ("ordered", "limit_ties") and previous is not None and compare_key(previous, row, order_by) > 0:
                                raise ValidationError(f"{'actual' if side == 0 else 'expected'} order violation at row {count + 1}")
                            previous = row
                            mandatory = 1
                            if limited and side == 1:
                                if limit_rows == 0:
                                    continue
                                if boundary is not None:
                                    relation = compare_key(row, boundary, order_by)
                                    if relation > 0:
                                        continue
                                    mandatory = int(relation < 0)
                            key = encoded(row[i] for i in fixed)
                            if floats:
                                payload = json.dumps([row[i] for i in floats], allow_nan=True)
                                db.execute("INSERT INTO floating VALUES (?, ?, ?, ?)", (key, side, payload, mandatory))
                            else:
                                db.execute("INSERT INTO exact_rows VALUES (?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET actual=actual+excluded.actual, expected=expected+excluded.expected, mandatory=mandatory+excluded.mandatory",
                                           (key, int(side == 0), int(side == 1), mandatory if side == 1 else 0))
                            count += 1
                            if count % 4096 == 0:
                                db.commit()
                        counts.append(count)
                    db.commit()
                    detail["actual_rows"], detail["expected_rows"] = counts
                    required_count = min(limit_rows, oracle_count) if limited else counts[1]
                    if counts[0] != required_count:
                        raise ValidationError("row count mismatch")
                    if not floats:
                        mismatch = db.execute("SELECT key, actual, expected, mandatory FROM exact_rows WHERE actual > expected OR actual < mandatory LIMIT 1").fetchone()
                        if mismatch:
                            raise ValidationError(f"exact value/multiplicity mismatch: {mismatch!r}")
                    else:
                        for (key,) in db.execute("SELECT DISTINCT key FROM floating"):
                            groups, required = [], []
                            for side in (0, 1):
                                n = db.execute("SELECT COUNT(*) FROM floating WHERE key=? AND side=?", (key, side)).fetchone()[0]
                                if n > max_float_bucket:
                                    raise ValidationError(f"ambiguous float bucket exceeds validation bound {max_float_bucket}")
                                entries = list(db.execute(
                                    "SELECT value, mandatory FROM floating WHERE key=? AND side=?", (key, side)))
                                groups.append([json.loads(x[0]) for x in entries])
                                if side == 1:
                                    required = [bool(x[1]) for x in entries]
                            if not matching(*groups, rel_tol, abs_tol, required=required):
                                raise ValidationError("floating value/multiplicity mismatch")
                finally:
                    db.close()
        detail.update(ok=True, status="validated")
    except (ValueError, pa.ArrowException, OSError, sqlite3.Error) as exc:
        detail["reason"] = str(exc)
    return detail


def compare_slice_files(actual_path, expected_path, *, scratch, policy, order_by,
                        rel_tol, abs_tol, max_float_bucket, limit_rows,
                        offset_rows, expected_is_complete, oracle_output_columns):
    """Validate an arbitrary permitted slice of a complete oracle.

    Equal ORDER BY keys form groups. A rank interval has a fixed cardinality
    in each group even though either boundary can choose arbitrary tied rows.
    Actual positions therefore identify their oracle group without needing
    hidden keys in the timed result. Membership is a typed capacitated matching
    within that group, never a comparison to one arbitrary reference subset.
    """
    detail = {"ok": False, "policy": policy, "status": "validation_error",
              "limit_rows": limit_rows, "offset_rows": offset_rows,
              "oracle_output_columns": oracle_output_columns, "oracle_order_by": order_by or [],
              "expected_is_complete": expected_is_complete}
    try:
        ordered = policy in ("ordered", "limit_ties")
        limited = policy in ("limit_ties", "unordered_limit")
        if policy not in ("ordered", "limit_ties", "unordered_limit"):
            raise ValidationError("slice comparison requires ordered or limited policy")
        if expected_is_complete is not True:
            raise ValidationError("slice comparison requires an explicitly complete oracle")
        if type(offset_rows) is not int or offset_rows < 0:
            raise ValidationError("offset_rows must be a nonnegative integer")
        if limited and (type(limit_rows) is not int or limit_rows < 0):
            raise ValidationError("limited slice requires nonnegative limit_rows")
        if not limited and (limit_rows is not None or offset_rows != 0):
            raise ValidationError("unlimited ordered comparison cannot specify a slice")
        if not all(type(x) in (int, float) and math.isfinite(x) and x >= 0 for x in (rel_tol, abs_tol)):
            raise ValidationError("invalid float tolerance")
        if type(max_float_bucket) is not int or max_float_bucket < 1:
            raise ValidationError("invalid float bucket bound")
        with pa.memory_map(str(actual_path), "r") as af, pa.memory_map(str(expected_path), "r") as ef:
            ar, er = pa.ipc.open_stream(af), pa.ipc.open_stream(ef)
            detail["actual_schema"], detail["expected_schema"] = str(ar.schema), str(er.schema)
            mapping = list(range(len(ar.schema))) if oracle_output_columns is None else oracle_output_columns
            if (not isinstance(mapping, list) or len(mapping) != len(ar.schema)
                or any(type(i) is not int or not 0 <= i < len(er.schema) for i in mapping)
                or len(set(mapping)) != len(mapping)):
                raise ValidationError("invalid oracle output column mapping")
            if oracle_output_columns is None and len(ar.schema) != len(er.schema):
                raise ValidationError("extra oracle columns require an explicit output mapping")
            types = [family(field.type) for field in ar.schema]
            if types != [family(er.schema.field(index).type) for index in mapping]:
                raise ValidationError("incompatible logical schemas")
            if ordered:
                if not isinstance(order_by, list) or not order_by:
                    raise ValidationError("ordered result requires explicit oracle key columns")
                for spec in order_by:
                    if (not isinstance(spec, dict) or type(spec.get("column")) is not int
                        or not 0 <= spec["column"] < len(er.schema)
                        or spec.get("direction") not in ("asc", "desc")
                        or spec.get("nulls") not in ("first", "last")):
                        raise ValidationError("invalid order key")
                    family(er.schema.field(spec["column"]).type)
            elif order_by:
                raise ValidationError("unordered LIMIT cannot specify order keys")
            visible_order = ([dict(spec, column=mapping.index(spec["column"])) for spec in order_by]
                             if ordered and all(spec["column"] in mapping for spec in order_by) else None)
            floats = [index for index, kind in enumerate(types) if kind == "float"]
            fixed = [index for index, kind in enumerate(types) if kind != "float"]
            with tempfile.TemporaryDirectory(prefix="compare-slice-", dir=scratch) as directory:
                db = sqlite3.connect(str(Path(directory) / "rows.sqlite"))
                try:
                    db.execute("PRAGMA cache_size=-8192")
                    db.execute("PRAGMA temp_store=FILE")
                    db.execute("CREATE TABLE groups (id INTEGER PRIMARY KEY, n INTEGER, required INTEGER)")
                    db.execute("CREATE TABLE exact_rows (key TEXT PRIMARY KEY, actual INTEGER, expected INTEGER)")
                    db.execute("CREATE TABLE floating (key TEXT, side INTEGER, value TEXT)")
                    db.execute("CREATE INDEX floating_key ON floating(key, side)")
                    def insert(group, row, side):
                        key = encoded([group, *(row[index] for index in fixed)])
                        if floats:
                            db.execute("INSERT INTO floating VALUES (?, ?, ?)", (key, side, json.dumps([row[index] for index in floats], allow_nan=True)))
                        else:
                            db.execute("INSERT INTO exact_rows VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET actual=actual+excluded.actual, expected=expected+excluded.expected", (key, int(side == 0), int(side == 1)))
                    total, group, group_start, previous = 0, 0, 0, None
                    upper = offset_rows + limit_rows if limited else None
                    def finish_group(end):
                        required = max(0, (min(end, upper) if upper is not None else end) - max(group_start, offset_rows))
                        db.execute("INSERT INTO groups VALUES (?, ?, ?)", (group, end - group_start, required))
                    for row in rows(er):
                        relation = compare_key(previous, row, order_by) if ordered and previous is not None else 0
                        if relation > 0:
                            raise ValidationError("unlimited oracle is not ordered")
                        if relation < 0:
                            finish_group(total)
                            group += 1
                            group_start = total
                        insert(group, tuple(row[index] for index in mapping), 1)
                        total += 1
                        previous = row
                        if total % 4096 == 0: db.commit()
                    if total: finish_group(total)
                    required_count = min(limit_rows, max(0, total - offset_rows)) if limited else total
                    groups = iter(db.execute("SELECT id, required FROM groups WHERE required>0 ORDER BY id"))
                    current = next(groups, None)
                    used, actual_count, previous = 0, 0, None
                    for row in rows(ar):
                        if current is None:
                            raise ValidationError("row count mismatch: extra actual rows")
                        if visible_order and previous is not None and compare_key(previous, row, visible_order) > 0:
                            raise ValidationError(f"actual order violation at row {actual_count + 1}")
                        insert(current[0], row, 0)
                        used += 1
                        actual_count += 1
                        previous = row
                        if used == current[1]:
                            current, used = next(groups, None), 0
                        if actual_count % 4096 == 0: db.commit()
                    detail.update(actual_rows=actual_count, expected_rows=required_count, oracle_rows=total)
                    if actual_count != required_count:
                        raise ValidationError("row count mismatch")
                    db.commit()
                    if not floats:
                        mismatch = db.execute("SELECT key, actual, expected FROM exact_rows WHERE actual > expected LIMIT 1").fetchone()
                        if mismatch:
                            raise ValidationError(f"exact value/multiplicity or order-group mismatch: {mismatch!r}")
                    else:
                        # Buckets unused by actual output need no matching; the
                        # group cardinality proves mandatory interior coverage.
                        for (key,) in db.execute("SELECT DISTINCT key FROM floating WHERE side=0"):
                            values = []
                            for side in (0, 1):
                                n = db.execute("SELECT COUNT(*) FROM floating WHERE key=? AND side=?", (key, side)).fetchone()[0]
                                if n > max_float_bucket:
                                    raise ValidationError(f"ambiguous float bucket exceeds validation bound {max_float_bucket}")
                                values.append([json.loads(row[0]) for row in db.execute("SELECT value FROM floating WHERE key=? AND side=?", (key, side))])
                            if not matching(*values, rel_tol, abs_tol, required=[False] * len(values[1])):
                                raise ValidationError("floating value/multiplicity or order-group mismatch")
                finally:
                    db.close()
        detail.update(ok=True, status="validated")
    except (ValidationError, pa.ArrowException, OSError, sqlite3.Error) as error:
        detail["reason"] = str(error)
    return detail

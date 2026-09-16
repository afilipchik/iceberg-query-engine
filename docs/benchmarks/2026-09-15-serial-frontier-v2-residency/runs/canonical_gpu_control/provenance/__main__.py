"""Run via the repository's memory-capped wrapper; see scripts/benchmark/README.md."""
import argparse
import json
from pathlib import Path

from .contract import ContractError, report, validate_manifest
from .prepare import write_json


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    commands = p.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--workload", choices=["tpch"], required=True)
    prepare.add_argument("--scale-factor", type=int, choices=[1, 10, 100], required=True)
    prepare.add_argument("--output", required=True)
    convert = commands.add_parser("convert")
    convert.add_argument("--dataset", required=True)
    convert.add_argument("--track", choices=["native", "iceberg", "lance", "decoded_ipc"], required=True)
    convert.add_argument("--engine-binary", required=True)
    convert.add_argument("--output", required=True)
    convert.add_argument("--memory-gib", type=int, default=4)
    convert.add_argument("--process-cap-gib", type=int, default=12)
    convert.add_argument("--validation-timeout-seconds", type=int, default=600)
    validate = commands.add_parser("validate")
    validate.add_argument("--manifest", help="check evidence manifest structure only")
    validate.add_argument("--actual")
    validate.add_argument("--expected")
    validate.add_argument("--scratch")
    validate.add_argument("--policy", choices=["bag", "ordered", "limit_ties", "unordered_limit"], default="bag")
    validate.add_argument("--order-by", help="JSON file of output-column order specifications")
    validate.add_argument("--limit-rows", type=int)
    validate.add_argument("--offset-rows", type=int, default=0)
    validate.add_argument("--oracle-output-columns", help="JSON file mapping actual output ordinals to complete oracle output ordinals")
    validate.add_argument("--expected-is-complete", action="store_true")
    validate.add_argument("--rel-tol", type=float, default=1e-10)
    validate.add_argument("--abs-tol", type=float, default=1e-8)
    validate.add_argument("--output")
    reports = commands.add_parser("report")
    reports.add_argument("--manifest", required=True)
    reports.add_argument("--samples", required=True)
    reports.add_argument("--output", required=True)
    reports.add_argument("--gate", choices=["complete", "latency"], default="latency")
    run = commands.add_parser("run")
    run.add_argument("--dataset", required=True)
    run.add_argument("--engine-binary", required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--track", choices=["raw_parquet", "native", "iceberg", "lance", "decoded_ipc", "gpu", "gpu_control"], default="raw_parquet")
    run.add_argument("--provider-manifest")
    run.add_argument("--reference-lance-io-limit", type=int,
                     help="Explicit reference-only Lance process I/O quota; requires fresh matched baseline")
    run.add_argument("--gpu-control-manifest")
    run.add_argument("--gpu-residency", choices=["mixed", "required"], default="mixed")
    run.add_argument("--host-arrow-preloaded", action="store_true")
    run.add_argument("--gpu-preparation-timeout-seconds", type=int, default=120)
    run.add_argument("--samples", type=int, default=10)
    run.add_argument("--sessions", type=int, default=3)
    run.add_argument("--threads", type=int, default=16)
    run.add_argument("--memory-gib", type=int, default=40)
    run.add_argument("--process-cap-gib", type=int, default=48)
    return p


def read_json(path):
    return json.loads(Path(path).read_text())


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == "prepare":
            from .prepare import prepare_tpch
            result = prepare_tpch(args.output, args.scale_factor)
            print(json.dumps({"status": result["status"], "dataset": str(Path(args.output) / "dataset.json")}))
            return 0
        if args.command == "convert":
            from .providers import convert
            result = convert(args)
            print(json.dumps({"status": result["status"], "provider_manifest": str(Path(args.output) / "provider.json")}))
            return 0
        if args.command == "run":
            from .run import run
            result = run(args)
            print(json.dumps({"complete": result["complete"], "latency_leadership": result["latency_leadership"],
                              "issues": len(result["issues"])}))
            return 0 if result["complete"] else 1
        if args.command == "validate":
            if args.manifest:
                if args.actual or args.expected:
                    raise ContractError("choose manifest structure or result validation")
                validate_manifest(read_json(args.manifest))
                result = {"ok": True, "status": "manifest_structure_valid", "answers_validated": False}
            else:
                if not all((args.actual, args.expected, args.scratch)):
                    raise ContractError("result validation requires --actual, --expected and --scratch")
                from .compare import compare_files
                result = compare_files(args.actual, args.expected, scratch=args.scratch,
                                       policy=args.policy,
                                       order_by=read_json(args.order_by) if args.order_by else None,
                                       limit_rows=args.limit_rows, offset_rows=args.offset_rows,
                                       oracle_output_columns=read_json(args.oracle_output_columns) if args.oracle_output_columns else None,
                                       expected_is_complete=args.expected_is_complete,
                                       rel_tol=args.rel_tol, abs_tol=args.abs_tol)
            if args.output:
                write_json(args.output, result)
            print(json.dumps(result, allow_nan=False))
            return 0 if result["ok"] else 1
        if args.command == "report":
            with Path(args.samples).open() as source:
                result = report(read_json(args.manifest), (json.loads(line) for line in source if line.strip()))
            write_json(args.output, result)
            print(json.dumps(result, allow_nan=False))
            return 0 if result["complete" if args.gate == "complete" else "latency_leadership"] else 1
    except (ContractError, ValueError, TypeError, KeyError, OSError) as error:
        result = {"ok": False, "complete": False, "latency_leadership": False,
                  "status": "invalid_evidence", "reason": str(error)}
        if args.command in ("report", "validate") and args.output:
            try:
                write_json(args.output, result)
            except OSError:
                pass
        print(json.dumps(result))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

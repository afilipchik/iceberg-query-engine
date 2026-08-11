#!/usr/bin/env python3
"""Validate the Kubernetes artifacts as far as is possible WITHOUT a cluster.

The development machine cannot run Docker or kind (see the header of
scripts/kind_test.sh), so `kubectl apply --dry-run=server` — the only check that
would validate these against a real API server — is unavailable. This script is
the honest substitute. It checks three classes of mistake that a schema
validator would NOT catch and that only show up as a broken cluster:

  1. Cross-object consistency: the StatefulSet's serviceName, the Services'
     selectors and targetPorts, the container's named port, and the DNS name
     passed to --peers-dns all have to agree. Any one of them being wrong
     produces a cluster that starts and never forms.
  2. Argument/environment agreement: --peers-dns-port vs the container port,
     QE_ADVERTISE_ADDR vs POD_IP, QE_MEMORY_LIMIT referenced before it is
     defined.
  3. Shutdown and memory arithmetic: terminationGracePeriodSeconds must exceed
     drain + shutdown grace, or Kubernetes SIGKILLs a node that was shutting
     down correctly; the cgroup memory limit must exceed the engine's own
     budget, or every spill decision races the OOM killer.

What this does NOT check: field spelling against the real OpenAPI schema,
admission policies, whether the image builds, whether CoreDNS behaves as
assumed, or anything at all about runtime. Run scripts/kind_test.sh on a
Docker-capable machine for that.

Usage:  .venv/bin/python scripts/validate_k8s_manifests.py
"""

from __future__ import annotations

import pathlib
import re
import sys

try:
    import yaml
except ImportError:
    sys.exit("PyYAML is required: .venv/bin/pip install pyyaml")

ROOT = pathlib.Path(__file__).resolve().parent.parent
FILES = [
    "k8s/statefulset.yaml",
    "k8s/service.yaml",
    "k8s/service-headless.yaml",
    "kind-cluster.yaml",
]

failures = 0


def check(cond: bool, msg: str) -> None:
    global failures
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        failures += 1


def to_bytes(v: str) -> int:
    m = re.match(r"^(\d+)(Gi|Mi|Ki|G|M|K)?$", str(v))
    if not m:
        raise ValueError(f"unparseable quantity: {v!r}")
    unit = m.group(2) or ""
    scale = {
        "": 1,
        "Ki": 1 << 10,
        "Mi": 1 << 20,
        "Gi": 1 << 30,
        "K": 10**3,
        "M": 10**6,
        "G": 10**9,
    }[unit]
    return int(m.group(1)) * scale


def main() -> int:
    docs = {}
    print("-- parse --")
    for f in FILES:
        path = ROOT / f
        try:
            docs[f] = yaml.safe_load(path.read_text())
        except Exception as e:  # noqa: BLE001
            print(f"  FAIL {f}: {e}")
            return 1
        print(f"  ok   {f}  apiVersion={docs[f].get('apiVersion')} kind={docs[f].get('kind')}")

    ss = docs["k8s/statefulset.yaml"]
    svc = docs["k8s/service.yaml"]
    hsvc = docs["k8s/service-headless.yaml"]
    kind = docs["kind-cluster.yaml"]

    print("\n-- object identity --")
    check(ss["apiVersion"] == "apps/v1" and ss["kind"] == "StatefulSet", "StatefulSet apiVersion/kind")
    check(svc["apiVersion"] == "v1" and svc["kind"] == "Service", "Service apiVersion/kind")
    check(hsvc["apiVersion"] == "v1" and hsvc["kind"] == "Service", "headless Service apiVersion/kind")
    # Kubernetes' headless marker is the literal STRING "None", which is also
    # what a YAML parser produces for a bare `None` (it is not a null keyword).
    check(hsvc["spec"].get("clusterIP") == "None", "headless Service has clusterIP: None")
    check(
        hsvc["spec"].get("publishNotReadyAddresses") is True,
        "headless publishes not-ready addresses (without this, every pod waits "
        "for DNS that waits for a ready pod)",
    )

    print("\n-- cross-object consistency --")
    check(ss["spec"]["serviceName"] == hsvc["metadata"]["name"], "serviceName matches the headless Service")
    labels = ss["spec"]["template"]["metadata"]["labels"]
    check(ss["spec"]["selector"]["matchLabels"] == labels, "StatefulSet selector matches pod labels")
    check(svc["spec"]["selector"].items() <= labels.items(), "Service selector matches pod labels")
    check(hsvc["spec"]["selector"].items() <= labels.items(), "headless selector matches pod labels")

    c = ss["spec"]["template"]["spec"]["containers"][0]
    ports = {p["name"]: p["containerPort"] for p in c["ports"]}
    check("http" in ports and ports["http"] == 7777, "container port 'http' = 7777")
    for s in (svc, hsvc):
        tp = s["spec"]["ports"][0]["targetPort"]
        check(tp in ports, f"{s['metadata']['name']}: targetPort {tp!r} resolves to a named container port")

    print("\n-- args and environment --")
    args = c["args"][0]
    check(f"--peers-dns {hsvc['metadata']['name']}" in args, "--peers-dns names the headless Service")
    check(f"--peers-dns-port {ports['http']}" in args, "--peers-dns-port matches the container port")
    check("exec /usr/local/bin/query_engine" in args, "exec: the engine is PID 1 and receives SIGTERM directly")
    check('QE_NODE_ID="${HOSTNAME##*-}"' in args, "QE_NODE_ID derived from the StatefulSet pod ordinal")

    env = {e["name"]: e for e in c["env"]}
    check(env["POD_IP"]["valueFrom"]["fieldRef"]["fieldPath"] == "status.podIP", "POD_IP from the downward API")
    check(env["QE_ADVERTISE_ADDR"]["value"] == f"$(POD_IP):{ports['http']}", "QE_ADVERTISE_ADDR expands POD_IP")
    for ref in re.findall(r"\$\{([A-Z_]+)\}", args):
        if ref == "HOSTNAME":
            continue
        check(ref in env, f"${{{ref}}} used in args is defined in env")

    print("\n-- probes --")
    check(all(p in c for p in ("startupProbe", "livenessProbe", "readinessProbe")), "all three probes present")
    check(c["livenessProbe"]["httpGet"]["path"] == "/healthz", "liveness -> /healthz (no table/peer/disk dependency)")
    check(c["readinessProbe"]["httpGet"]["path"] == "/readyz", "readiness -> /readyz")
    check(c["startupProbe"]["httpGet"]["path"] == "/readyz", "startup -> /readyz (covers a long table load)")
    startup_budget = c["startupProbe"]["periodSeconds"] * c["startupProbe"]["failureThreshold"]
    check(startup_budget >= 60, f"startup probe allows {startup_budget}s for the table load")

    print("\n-- shutdown and memory arithmetic --")
    grace_s = ss["spec"]["template"]["spec"]["terminationGracePeriodSeconds"]
    drain_ms = int(re.search(r"--drain-ms (\d+)", args).group(1))
    sgrace_ms = int(re.search(r"--shutdown-grace-ms (\d+)", args).group(1))
    check(
        grace_s * 1000 >= drain_ms + sgrace_ms,
        f"terminationGracePeriodSeconds {grace_s}s >= drain {drain_ms}ms + grace {sgrace_ms}ms",
    )
    check(
        to_bytes(c["resources"]["limits"]["memory"]) > to_bytes(env["QE_MEMORY_LIMIT"]["value"]),
        f"cgroup limit {c['resources']['limits']['memory']} exceeds the engine budget "
        f"{env['QE_MEMORY_LIMIT']['value']}",
    )

    print("\n-- volumes and security --")
    mounts = {m["name"]: m for m in c["volumeMounts"]}
    vols = {v["name"] for v in ss["spec"]["template"]["spec"]["volumes"]}
    check(set(mounts) <= vols, "every volumeMount has a matching volume")
    check(mounts["data"].get("readOnly") is True, "dataset mounted read-only")
    check(
        c["securityContext"]["readOnlyRootFilesystem"] is True and mounts["spill"]["mountPath"] == "/tmp",
        "read-only root filesystem with a writable /tmp for the spill directory",
    )
    check(c["securityContext"]["allowPrivilegeEscalation"] is False, "no privilege escalation")
    check(ss["spec"]["template"]["spec"]["securityContext"]["runAsNonRoot"] is True, "runs as non-root")

    print("\n-- kind cluster --")
    check(kind["kind"] == "Cluster" and kind["apiVersion"] == "kind.x-k8s.io/v1alpha4", "kind apiVersion/kind")
    roles = [n["role"] for n in kind["nodes"]]
    check(roles.count("control-plane") == 1 and roles.count("worker") == 3, f"1 control-plane + 3 workers: {roles}")
    host_path = ss["spec"]["template"]["spec"]["volumes"][0]["hostPath"]["path"]
    check(
        all(any(m["containerPath"] == host_path for m in n.get("extraMounts", [])) for n in kind["nodes"]),
        f"every kind node mounts the dataset at {host_path} (hostPath resolves per node)",
    )
    node_port = svc["spec"]["ports"][0]["nodePort"]
    mapped = [m["containerPort"] for n in kind["nodes"] for m in n.get("extraPortMappings", [])]
    check(node_port in mapped, f"NodePort {node_port} is mapped to the host by kind")

    print()
    if failures:
        print(f"RESULT: {failures} FAILURE(S)")
        return 1
    print("RESULT: ALL CHECKS PASS")
    print("REMINDER: these manifests remain UNVALIDATED-ON-CLUSTER. Nothing here")
    print("has been applied to a Kubernetes API server. Run scripts/kind_test.sh")
    print("on a Docker-capable machine to change that.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

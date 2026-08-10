#!/usr/bin/env bash
# Validate the engine's NUMA code path on a REAL multi-node topology.
#
# WHY THIS EXISTS
# This development machine is a single-socket i9-13900KF: `numactl --hardware`
# reports exactly one node, so the NUMA path in src/execution/topology.rs can
# never execute here. Unit tests against synthetic sysfs fixtures prove the
# PARSER works; they cannot prove the engine behaves correctly when the kernel
# actually reports two nodes with a distance matrix. This script closes that
# gap by booting the real binary inside QEMU/KVM with an emulated 2-node NUMA
# topology.
#
# WHAT IT PROVES
#   * the engine detects 2 nodes, their CPU sets, and the ACPI SLIT distances
#   * worker preference order interleaves ACROSS nodes (so both memory
#     controllers are used) instead of the P-core-first order it picks on the
#     hybrid host
#   * queries still return correct results under NUMA placement
#
# WHAT IT CANNOT PROVE  <-- read this before quoting any timing from it
# Both emulated nodes are backed by the SAME physical memory controller on a
# single-node host, so there is no real remote-access penalty to avoid. NUMA
# placement therefore shows NO speedup here and may be marginally slower from
# the added pinning constraint. A genuine NUMA performance benefit can only be
# measured on real multi-socket hardware.
#
# REQUIREMENTS: qemu-system-x86_64, /dev/kvm, busybox (static), cpio, gzip, curl.
# The kernel must have CONFIG_NUMA=y -- Alpine's `virt` kernel does NOT (it
# silently ignores QEMU's SRAT and reports 1 node), so we use `lts`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$ROOT/.scratch/numavm"
BIN="${BIN:-$ROOT/target/release/query_engine}"
NODES="${NODES:-2}"
VCPUS="${VCPUS:-4}"
MEM_PER_NODE="${MEM_PER_NODE:-4G}"
KURL="https://dl-cdn.alpinelinux.org/alpine/v3.21/releases/x86_64/netboot/vmlinuz-lts"

[ -x "$BIN" ] || { echo "missing $BIN (cargo build --release)"; exit 2; }
[ -e /dev/kvm ] || echo "WARNING: /dev/kvm absent; this will be very slow"
mkdir -p "$WORK"

if [ ! -f "$WORK/vmlinuz-lts" ]; then
  echo "downloading NUMA-capable kernel..."
  curl -sSfL -o "$WORK/vmlinuz-lts" "$KURL"
fi

echo "building initramfs from $BIN ..."
R="$WORK/root"
rm -rf "$R"; mkdir -p "$R"/{bin,proc,sys,dev,tmp}
cp "$(command -v busybox)" "$R/bin/"
cp "$BIN" "$R/bin/query_engine"
# Ship the loader and every shared library at its real absolute path so the
# dynamic linker resolves exactly as it does on the host.
ldd "$BIN" | grep -oE '/[^ ]+\.so[^ ]*' | sort -u | while read -r lib; do
  mkdir -p "$R$(dirname "$lib")"; cp -L "$lib" "$R$lib"
done

cat > "$R/init" <<'INIT'
#!/bin/busybox sh
/bin/busybox --install -s /bin
export PATH=/bin:/usr/bin
mount -t proc none /proc; mount -t sysfs none /sys; mount -t devtmpfs none /dev
mount -t tmpfs -o size=4G none /tmp
echo; echo "=============== GUEST NUMA TOPOLOGY (kernel view) ==============="
for n in /sys/devices/system/node/node[0-9]*; do
  [ -d "$n" ] || continue
  echo "$(basename $n) cpulist=$(cat $n/cpulist) distance=$(cat $n/distance)"
done
echo; echo "=============== ENGINE TOPOLOGY DETECTION ==============="
/bin/query_engine topology 2>&1 | head -24
echo; echo "=============== CORRECTNESS UNDER NUMA PLACEMENT ==============="
/bin/query_engine generate-parquet --sf 0.01 --output /tmp/d >/dev/null 2>&1 \
  && echo "test data generated"
echo "--- NUMA placement ACTIVE ---"
/bin/query_engine benchmark-parquet --path /tmp/d --iterations 1 2>&1 | tail -2
echo "--- placement DISABLED (QE_TOPOLOGY=0) ---"
QE_TOPOLOGY=0 /bin/query_engine benchmark-parquet --path /tmp/d --iterations 1 2>&1 | tail -2
echo; echo "NOTE: timings here are NOT a NUMA benefit measurement -- both"
echo "emulated nodes share one physical memory controller on this host."
echo "=============== DONE ==============="
poweroff -f
INIT
chmod +x "$R/init"
( cd "$R" && find . -print0 | cpio --null -o --format=newc 2>/dev/null | gzip -1 > "$WORK/initramfs.gz" )

# Build the -numa arguments: N nodes, vCPUs dealt evenly, non-trivial distances.
ARGS=(); PER=$(( VCPUS / NODES ))
for i in $(seq 0 $((NODES-1))); do
  ARGS+=(-object "memory-backend-ram,size=$MEM_PER_NODE,id=m$i")
  ARGS+=(-numa "node,nodeid=$i,memdev=m$i,cpus=$((i*PER))-$((i*PER+PER-1))")
done
for i in $(seq 0 $((NODES-1))); do for j in $(seq $((i+1)) $((NODES-1))); do
  ARGS+=(-numa "dist,src=$i,dst=$j,val=21")
done; done

echo "booting $NODES-node NUMA guest ($VCPUS vCPUs, $MEM_PER_NODE per node)..."
exec qemu-system-x86_64 -enable-kvm -nographic -no-reboot \
  -m "$((${MEM_PER_NODE%G} * NODES))G" -smp "$VCPUS" "${ARGS[@]}" \
  -kernel "$WORK/vmlinuz-lts" -initrd "$WORK/initramfs.gz" \
  -append "console=ttyS0 quiet rdinit=/init" 2>&1 | sed 's/\r//'

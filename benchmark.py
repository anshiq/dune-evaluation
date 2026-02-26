import argparse
import json
import os
import random
import shutil
import sys
import time
from pathlib import Path

# Add parent dir to path so we can run from anywhere
sys.path.insert(0, str(Path(__file__).parent))

from generate import generate_records, get_record_sizes, RANDOM_SEED
from strategies import SingleFileStorage, ChunkedStorage, IndividualFileStorage

RANDOM_READ_COUNT = 1000
DATA_BASE = os.path.join(os.path.dirname(__file__), "data")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def get_random_indices(n_records: int, count: int, seed: int = RANDOM_SEED) -> list:
    rng = random.Random(seed + 1)   # different seed from data generation
    return rng.sample(range(n_records), min(count, n_records))


def drop_os_cache():
    """Best-effort cache drop. Works on Linux with sudo;"""
    try:
        os.system("sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null")
    except Exception:
        pass


def run_benchmark(strategy_cls, data_dir, records, random_indices,
                  drop_cache=False, label=None):
    strategy = strategy_cls(data_dir)
    results = []

    # --- WRITE ---
    print(f"  [{label}] Writing {len(records):,} records...", end=" ", flush=True)
    r = strategy.write_all(records)
    print(f"{r.total_time:.2f}s  ({r.avg_latency_ms:.4f} ms/rec)")
    results.append(r)

    if drop_cache:
        drop_os_cache()

    # --- READ SEQUENTIAL ---
    print(f"  [{label}] Sequential read...", end=" ", flush=True)
    r = strategy.read_sequential()
    print(f"{r.total_time:.2f}s  ({r.avg_latency_ms:.4f} ms/rec)")
    results.append(r)

    if drop_cache:
        drop_os_cache()

    # --- READ RANDOM ---
    print(f"  [{label}] Random read ({len(random_indices)} records)...", end=" ", flush=True)
    r = strategy.read_random(random_indices)
    print(f"{r.total_time:.2f}s  ({r.avg_latency_ms:.4f} ms/rec)")
    results.append(r)

    return results


def print_summary_table(all_results):
    """Print a formatted summary table to stdout."""
    print("\n" + "=" * 100)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 100)

    header = (f"{'Strategy':<22} {'Phase':<6} {'Pattern':<12} "
              f"{'Total(s)':>10} {'Avg(ms/rec)':>12} "
              f"{'CPU_u(s)':>9} {'CPU_s(s)':>9} "
              f"{'SysCalls_R':>11} {'SysCalls_W':>11}")
    print(header)
    print("-" * 100)

    for r in all_results:
        sc_r = str(r.syscalls_read) if r.syscalls_read is not None else "N/A"
        sc_w = str(r.syscalls_write) if r.syscalls_write is not None else "N/A"
        print(f"{r.strategy:<22} {r.phase:<6} {r.pattern:<12} "
              f"{r.total_time:>10.3f} {r.avg_latency_ms:>12.4f} "
              f"{r.cpu_user:>9.3f} {r.cpu_sys:>9.3f} "
              f"{sc_r:>11} {sc_w:>11}")

    print("=" * 100)


def main():
    parser = argparse.ArgumentParser(description="DUNE Storage Benchmark")
    parser.add_argument("--records", type=int, default=100_000,
                        help="Number of records to generate (default: 100000)")
    parser.add_argument("--output", type=str,
                        default=os.path.join(RESULTS_DIR, "results.json"),
                        help="Output JSON path")
    parser.add_argument("--drop-cache", action="store_true",
                        help="Attempt to drop OS page cache between benchmarks (needs root)")
    parser.add_argument("--skip-individual", action="store_true",
                        help="Skip individual files strategy (very slow for 100k records)")
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"DUNE Fine-Grained Storage Benchmark")
    print(f"{'='*60}")
    print(f"Generating {args.records:,} records (seed={RANDOM_SEED})...", end=" ", flush=True)
    t0 = time.perf_counter()
    records = generate_records(n=args.records)
    t1 = time.perf_counter()
    stats = get_record_sizes(records)
    print(f"done in {t1-t0:.2f}s")
    print(f"  Total data: {stats['total_mb']:.1f} MB  "
          f"Avg size: {stats['avg_bytes']:.0f} bytes")

    random_indices = get_random_indices(args.records, RANDOM_READ_COUNT)
    print(f"  Random read sample: {len(random_indices)} records\n")

    # Clean previous data
    if os.path.exists(DATA_BASE):
        shutil.rmtree(DATA_BASE)
    os.makedirs(DATA_BASE)

    all_results = []

   
    # Single File Strategy
    print("─" * 60)
    print("STRATEGY 1: Single Large File")
    print("─" * 60)
    results = run_benchmark(
        SingleFileStorage,
        os.path.join(DATA_BASE, "single"),
        records, random_indices,
        drop_cache=args.drop_cache,
        label="SingleFile"
    )
    all_results.extend(results)

    # Chunked Files Strategy
    print("\n" + "─" * 60)
    print("STRATEGY 2: Chunked Files (1000 records/file)")
    print("─" * 60)
    results = run_benchmark(
        ChunkedStorage,
        os.path.join(DATA_BASE, "chunked"),
        records, random_indices,
        drop_cache=args.drop_cache,
        label="Chunked"
    )
    all_results.extend(results)

    # Individual Files Strategy
    if not args.skip_individual:
        print("\n" + "─" * 60)
        print("STRATEGY 3: Individual Files (1 file/record)")
        print(f"  ⚠  This creates {args.records:,} files — may take several minutes")
        print("─" * 60)
        results = run_benchmark(
            IndividualFileStorage,
            os.path.join(DATA_BASE, "individual"),
            records, random_indices,
            drop_cache=args.drop_cache,
            label="Individual"
        )
        all_results.extend(results)
    else:
        print("\n[Skipping Individual Files strategy]")

    #  Summary table
    print_summary_table(all_results)

    #  Save JSON results
    output = {
        "meta": {
            "n_records": args.records,
            "random_read_count": RANDOM_READ_COUNT,
            "random_seed": RANDOM_SEED,
            "total_data_mb": round(stats["total_mb"], 2),
            "avg_record_bytes": round(stats["avg_bytes"], 1),
        },
        "benchmarks": [r.to_dict() for r in all_results]
    }
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved → {args.output}")

    # OBSERVATIONS:
    print("\nKEY OBSERVATIONS:")
    write_results = {r.strategy: r for r in all_results if r.phase == "write"}
    if len(write_results) > 1:
        strategies = list(write_results.keys())
        fastest = min(write_results.values(), key=lambda r: r.total_time)
        slowest = max(write_results.values(), key=lambda r: r.total_time)
        ratio = slowest.total_time / fastest.total_time
        print(f"  Write: {slowest.strategy} is {ratio:.1f}× slower than {fastest.strategy}")

    seq_results = {r.strategy: r for r in all_results
                   if r.phase == "read" and r.pattern == "sequential"}
    if len(seq_results) > 1:
        fastest = min(seq_results.values(), key=lambda r: r.total_time)
        slowest = max(seq_results.values(), key=lambda r: r.total_time)
        ratio = slowest.total_time / fastest.total_time
        print(f"  Seq Read: {slowest.strategy} is {ratio:.1f}× slower than {fastest.strategy}")

    rand_results = {r.strategy: r for r in all_results
                    if r.phase == "read" and r.pattern == "random"}
    if len(rand_results) > 1:
        fastest = min(rand_results.values(), key=lambda r: r.total_time)
        slowest = max(rand_results.values(), key=lambda r: r.total_time)
        ratio = slowest.total_time / fastest.total_time
        print(f"  Rand Read: {slowest.strategy} is {ratio:.1f}× slower than {fastest.strategy}")

    print()


if __name__ == "__main__":
    main()

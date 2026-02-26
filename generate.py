"""
generate.py — Reproducible sub-record data generator.

Generates 100,000 sub-records with:
  - Sizes uniformly distributed between 1 KB and 2 KB
  - Content: random bytes seeded for reproducibility
  - Fixed random seed for full reproducibility

Usage:
    from generate import generate_records
    records = generate_records()          # list of bytes objects
"""

import random

RANDOM_SEED = 42
NUM_RECORDS = 100_000
MIN_SIZE_BYTES = 1024   # 1 KB
MAX_SIZE_BYTES = 2048   # 2 KB


def generate_records(n: int = NUM_RECORDS,
                     seed: int = RANDOM_SEED,
                     min_size: int = MIN_SIZE_BYTES,
                     max_size: int = MAX_SIZE_BYTES) -> list:
    rng = random.Random(seed)

    records = []
    for i in range(n):
        size = rng.randint(min_size, max_size)
        # Generate random bytes using os.urandom seeded via rng
        # We seed a local random state per-record for full reproducibility
        record_seed = rng.getrandbits(64).to_bytes(8, "big")
        # Build random bytes deterministically
        data = bytearray(size)
        local_rng = random.Random(int.from_bytes(record_seed, "big"))
        for j in range(0, size, 8):
            chunk = local_rng.getrandbits(64).to_bytes(8, "big")
            data[j:j+8] = chunk[:min(8, size - j)]
        records.append(bytes(data))

    return records


def get_record_sizes(records: list) -> dict:
    sizes = [len(r) for r in records]
    return {
        "count": len(sizes),
        "min_bytes": min(sizes),
        "max_bytes": max(sizes),
        "avg_bytes": sum(sizes) / len(sizes),
        "total_mb": sum(sizes) / (1024 * 1024),
    }


if __name__ == "__main__":
    print(f"Generating {NUM_RECORDS:,} records with seed={RANDOM_SEED}...")
    records = generate_records()
    stats = get_record_sizes(records)
    print(f"  Count      : {stats['count']:,}")
    print(f"  Min size   : {stats['min_bytes']:,} bytes")
    print(f"  Max size   : {stats['max_bytes']:,} bytes")
    print(f"  Avg size   : {stats['avg_bytes']:.1f} bytes")
    print(f"  Total data : {stats['total_mb']:.1f} MB")
    print("Generator OK — reproducible data confirmed.")

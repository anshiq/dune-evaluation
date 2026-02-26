import os
from typing import List
from base import BaseStorage, BenchmarkResult

BUCKET_SIZE = 1000   # files per subdirectory


def _record_path(base_dir: str, idx: int) -> str:
    bucket = idx // BUCKET_SIZE
    bucket_dir = os.path.join(base_dir, f"b{bucket:03d}")
    return os.path.join(bucket_dir, f"rec_{idx:06d}.bin")


class IndividualFileStorage(BaseStorage):
    """One binary file per sub-record — maximum write parallelism, highest open() overhead."""

    def __init__(self, base_dir: str):
        super().__init__(base_dir, "IndividualFiles")
        self._n_records = 0

    # Write

    def write_all(self, records: List[bytes]) -> BenchmarkResult:
        n_buckets = (len(records) + BUCKET_SIZE - 1) // BUCKET_SIZE
        for b in range(n_buckets):
            os.makedirs(os.path.join(self.base_dir, f"b{b:03d}"), exist_ok=True)

        t, cu, cs, io = self._start_measurement()

        for idx, record in enumerate(records):
            path = _record_path(self.base_dir, idx)
            with open(path, "wb") as f:
                f.write(record)

        self._n_records = len(records)
        return self._end_measurement(t, cu, cs, io, "write", "write", len(records))

    # Read — sequential

    def read_sequential(self) -> BenchmarkResult:
        n = self._n_records or self._count_records()
        t, cu, cs, io = self._start_measurement()

        for idx in range(n):
            path = _record_path(self.base_dir, idx)
            with open(path, "rb") as f:
                _ = f.read()

        return self._end_measurement(t, cu, cs, io, "read", "sequential", n)

    # Read — random

    def read_random(self, indices: List[int]) -> BenchmarkResult:
        t, cu, cs, io = self._start_measurement()

        for idx in indices:
            path = _record_path(self.base_dir, idx)
            with open(path, "rb") as f:
                _ = f.read()

        return self._end_measurement(t, cu, cs, io, "read", "random", len(indices))

    # Helper

    def _count_records(self) -> int:
        count = 0
        for root, dirs, files in os.walk(self.base_dir):
            count += sum(1 for f in files if f.startswith("rec_") and f.endswith(".bin"))
        return count

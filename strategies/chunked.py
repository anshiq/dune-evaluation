import os
import struct
from typing import List
from base import BaseStorage, BenchmarkResult

CHUNK_SIZE = 1000        # records per chunk file
INDEX_ENTRY_FMT = ">IQI"  # uint32 chunk_id, uint64 offset_in_chunk, uint32 size
INDEX_ENTRY_BYTES = struct.calcsize(INDEX_ENTRY_FMT)  # 16 bytes


def _chunk_filename(chunk_id: int) -> str:
    return f"chunk_{chunk_id:04d}.bin"


class ChunkedStorage(BaseStorage):

    INDEX_FILE = "index.bin"

    def __init__(self, base_dir: str):
        super().__init__(base_dir, "ChunkedFiles")
        self.index_path = os.path.join(base_dir, self.INDEX_FILE)
        self._index = []    # list of (chunk_id, offset, size)

    # Write

    def write_all(self, records: List[bytes]) -> BenchmarkResult:
        t, cu, cs, io = self._start_measurement()

        n_chunks = (len(records) + CHUNK_SIZE - 1) // CHUNK_SIZE
        index_entries = []

        for chunk_id in range(n_chunks):
            start = chunk_id * CHUNK_SIZE
            end = min(start + CHUNK_SIZE, len(records))
            chunk_records = records[start:end]

            chunk_path = os.path.join(self.base_dir, _chunk_filename(chunk_id))
            offset = 0
            with open(chunk_path, "wb") as cf:
                for record in chunk_records:
                    size = len(record)
                    cf.write(record)
                    index_entries.append((chunk_id, offset, size))
                    offset += size

        # Write binary index
        with open(self.index_path, "wb") as ix:
            for chunk_id, offset, size in index_entries:
                ix.write(struct.pack(INDEX_ENTRY_FMT, chunk_id, offset, size))

        return self._end_measurement(t, cu, cs, io, "write", "write", len(records))

    # Index loading

    def _load_index(self):
        if self._index:
            return
        with open(self.index_path, "rb") as f:
            raw = f.read()
        n = len(raw) // INDEX_ENTRY_BYTES
        self._index = [
            struct.unpack_from(INDEX_ENTRY_FMT, raw, i * INDEX_ENTRY_BYTES)
            for i in range(n)
        ]

    # Read — sequential

    def read_sequential(self) -> BenchmarkResult:
        self._load_index()
        t, cu, cs, io = self._start_measurement()

        current_chunk_id = -1
        cf = None
        try:
            for chunk_id, offset, size in self._index:
                if chunk_id != current_chunk_id:
                    if cf:
                        cf.close()
                    chunk_path = os.path.join(self.base_dir, _chunk_filename(chunk_id))
                    cf = open(chunk_path, "rb")
                    current_chunk_id = chunk_id
                cf.seek(offset)
                _ = cf.read(size)
        finally:
            if cf:
                cf.close()

        return self._end_measurement(t, cu, cs, io, "read", "sequential", len(self._index))

    # Read — random

    def read_random(self, indices: List[int]) -> BenchmarkResult:
        self._load_index()
        t, cu, cs, io = self._start_measurement()
        open_files = {}
        try:
            for idx in indices:
                chunk_id, offset, size = self._index[idx]
                if chunk_id not in open_files:
                    chunk_path = os.path.join(self.base_dir, _chunk_filename(chunk_id))
                    open_files[chunk_id] = open(chunk_path, "rb")
                f = open_files[chunk_id]
                f.seek(offset)
                _ = f.read(size)
        finally:
            for f in open_files.values():
                f.close()

        return self._end_measurement(t, cu, cs, io, "read", "random", len(indices))

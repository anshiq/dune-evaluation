import os
import struct
from typing import List
from base import BaseStorage, BenchmarkResult

INDEX_ENTRY_FMT = ">QQ"   # big-endian uint64 offset + uint64 size
INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FMT)  # 16 bytes


class SingleFileStorage(BaseStorage):
    DATA_FILE = "records.bin" # to save data 
    INDEX_FILE = "index.bin" # to save index of starting of data and offset ..

    def __init__(self, base_dir: str):
        super().__init__(base_dir, "SingleFile")
        self.data_path = os.path.join(base_dir, self.DATA_FILE)
        self.index_path = os.path.join(base_dir, self.INDEX_FILE)
        self._index = []   # list of (offset, size) tuples 

    # Write

    def write_all(self, records: List[bytes]) -> BenchmarkResult:
        t, cu, cs, io = self._start_measurement()

        with open(self.data_path, "wb") as df, \
             open(self.index_path, "wb") as ix:
            offset = 0
            for record in records:
                size = len(record)
                df.write(record)
                ix.write(struct.pack(INDEX_ENTRY_FMT, offset, size))
                offset += size

        return self._end_measurement(t, cu, cs, io, "write", "write", len(records))

    # Index loading

    def _load_index(self):
        if self._index:
            return
        with open(self.index_path, "rb") as f:
            raw = f.read()
        n = len(raw) // INDEX_ENTRY_SIZE
        self._index = [
            struct.unpack_from(INDEX_ENTRY_FMT, raw, i * INDEX_ENTRY_SIZE)
            for i in range(n)
        ]

    # Read — sequential

    def read_sequential(self) -> BenchmarkResult:
        self._load_index()
        t, cu, cs, io = self._start_measurement()

        with open(self.data_path, "rb") as f:
            for _offset, size in self._index:
                _ = f.read(size)

        return self._end_measurement(t, cu, cs, io, "read", "sequential", len(self._index))

    # Read — random

    def read_random(self, indices: List[int]) -> BenchmarkResult:
        self._load_index()
        t, cu, cs, io = self._start_measurement()

        with open(self.data_path, "rb") as f:
            for idx in indices:
                offset, size = self._index[idx]
                f.seek(offset)
                _ = f.read(size)

        return self._end_measurement(t, cu, cs, io, "read", "random", len(indices))

from abc import ABC, abstractmethod
from typing import List
import time
import os
import resource


def get_io_stats():
    """Read I/O stats from /proc/self/io (Linux only). Returns dict or None."""
    try:
        stats = {}
        with open("/proc/self/io", "r") as f:
            for line in f:
                key, val = line.strip().split(": ")
                stats[key] = int(val)
        return stats
    except Exception:
        return None


def get_cpu_time():
    """Return (user_time, system_time) in seconds."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime, usage.ru_stime


class BenchmarkResult:
    def __init__(self, phase: str, strategy: str, pattern: str,
                 total_time: float, n_records: int,
                 cpu_user: float = 0.0, cpu_sys: float = 0.0,
                 io_before=None, io_after=None):
        self.phase = phase          # "write" or "read"
        self.strategy = strategy
        self.pattern = pattern      # "sequential" or "random" or "write"
        self.total_time = total_time
        self.n_records = n_records
        self.avg_latency_ms = (total_time / n_records) * 1000
        self.cpu_user = cpu_user
        self.cpu_sys = cpu_sys

        # Bonus: I/O stats delta
        self.syscalls_read = None
        self.syscalls_write = None
        self.bytes_read = None
        self.bytes_written = None

        if io_before and io_after:
            self.syscalls_read = io_after.get("syscr", 0) - io_before.get("syscr", 0)
            self.syscalls_write = io_after.get("syscw", 0) - io_before.get("syscw", 0)
            self.bytes_read = io_after.get("rchar", 0) - io_before.get("rchar", 0)
            self.bytes_written = io_after.get("wchar", 0) - io_before.get("wchar", 0)

    def to_dict(self):
        return {
            "phase": self.phase,
            "strategy": self.strategy,
            "pattern": self.pattern,
            "total_time_s": round(self.total_time, 4),
            "n_records": self.n_records,
            "avg_latency_ms": round(self.avg_latency_ms, 4),
            "cpu_user_s": round(self.cpu_user, 4),
            "cpu_sys_s": round(self.cpu_sys, 4),
            "syscalls_read": self.syscalls_read,
            "syscalls_write": self.syscalls_write,
            "bytes_read": self.bytes_read,
            "bytes_written": self.bytes_written,
        }

    def __repr__(self):
        bonus = ""
        if self.syscalls_read is not None:
            bonus = (f"  syscalls(r/w)={self.syscalls_read}/{self.syscalls_write} "
                     f"bytes(r/w)={self.bytes_read}/{self.bytes_written}")
        return (f"[{self.strategy:20s}] {self.phase:5s}/{self.pattern:10s} | "
                f"total={self.total_time:.3f}s  avg={self.avg_latency_ms:.4f}ms/rec "
                f"cpu(u/s)={self.cpu_user:.3f}/{self.cpu_sys:.3f}s{bonus}")


class BaseStorage(ABC):
    """Abstract storage strategy. All strategies share this interface."""

    def __init__(self, base_dir: str, strategy_name: str):
        self.base_dir = base_dir
        self.strategy_name = strategy_name
        os.makedirs(base_dir, exist_ok=True)

    @abstractmethod
    def write_all(self, records: List[bytes]) -> BenchmarkResult:
        """Write all records. Returns benchmark timing."""
        pass

    @abstractmethod
    def read_sequential(self) -> BenchmarkResult:
        """Read all records in order. Returns benchmark timing."""
        pass

    @abstractmethod
    def read_random(self, indices: List[int]) -> BenchmarkResult:
        """Read records at the given indices (0-based). Returns benchmark timing."""
        pass

    def _start_measurement(self):
        io = get_io_stats()
        cpu_u, cpu_s = get_cpu_time()
        t = time.perf_counter()
        return t, cpu_u, cpu_s, io

    def _end_measurement(self, start_t, start_cpu_u, start_cpu_s, start_io,
                          phase, pattern, n_records):
        elapsed = time.perf_counter() - start_t
        end_io = get_io_stats()
        end_cpu_u, end_cpu_s = get_cpu_time()
        return BenchmarkResult(
            phase=phase,
            strategy=self.strategy_name,
            pattern=pattern,
            total_time=elapsed,
            n_records=n_records,
            cpu_user=end_cpu_u - start_cpu_u,
            cpu_sys=end_cpu_s - start_cpu_s,
            io_before=start_io,
            io_after=end_io,
        )

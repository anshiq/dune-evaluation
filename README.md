# DUNE Fine-Grained Storage Evaluation

A benchmarking framework for evaluating different storage strategies for handling large numbers of small binary records. This project simulates a typical DUNE (Data Under Neutron Experiment) use case where thousands of small binary records need to be stored and retrieved efficiently.

---

## Quick Start

```bash
# Run with default settings (100,000 records)
python benchmark.py
```

That's it! The benchmark will:
1. Generate 100,000 reproducible records (1-2KB each)
2. Run all 3 storage strategies (Write + Sequential Read + Random Read)
3. Display results in a formatted table
4. Save JSON results to `results/results.json`

---

## Requirements

- Python 3.7+
- Linux or macOS (I/O stats work best on Linux)
- No external dependencies (uses only standard library)

---

## Project Structure

```
.
├── benchmark.py          # Main benchmark runner
├── base.py              # Abstract base class for storage strategies
├── generate.py          # Reproducible test data generator
├── strategies/
│   ├── __init__.py
│   ├── single_file.py  # Strategy 1: Single large file
│   ├── chunked.py      # Strategy 2: Chunked files
│   └── individual.py  # Strategy 3: Individual files
├── data/                # Generated data (created at runtime)
└── results/             # Benchmark results (created at runtime)
```

---

## Usage Options

### Basic Usage

```bash
# Default run (100,000 records)
python benchmark.py

# Custom number of records
python benchmark.py --records 50000
python benchmark.py --records 10000   # Quick test
```

### Skipping Slow Strategies

```bash
# Skip individual files (very slow for 100K records)
python benchmark.py --skip-individual
```

### Cache Management

```bash
# Drop OS cache between benchmarks (Linux only, requires root)
sudo python benchmark.py --drop-cache
```

### Output Control

```bash
# Custom output location
python benchmark.py --output results/my_benchmark.json
```

---

## Understanding the Output

### Console Output Example

```
============================================================
BENCHMARK RESULTS SUMMARY
============================================================
Strategy               Phase   Pattern       Total(s)   Avg(ms/rec)   CPU_u(s)   CPU_s(s)   SysCalls_R   SysCalls_W
----------------------------------------------------------------------------------------------------
SingleFile             write   write            2.345       0.0235       1.234       0.123         N/A         N/A
SingleFile             read    sequential       0.456       0.0046       0.234       0.045        100000       0
SingleFile             read    random           0.123       0.1230       0.067       0.012         1000         0
ChunkedFiles           write   write            2.567       0.0257       1.345       0.134         N/A         N/A
...
```

### JSON Output

Results are saved to `results/results.json`:

```json
{
  "meta": {
    "n_records": 100000,
    "random_read_count": 1000,
    "random_seed": 42,
    "total_data_mb": 150.2,
    "avg_record_bytes": 1536
  },
  "benchmarks": [
    {
      "phase": "write",
      "strategy": "SingleFile",
      "pattern": "write",
      "total_time_s": 2.345,
      "n_records": 100000,
      "avg_latency_ms": 0.0235,
      "cpu_user_s": 1.234,
      "cpu_sys_s": 0.123
    }
  ]
}
```

---

## What Gets Measured

Each benchmark captures:

| Metric | Description |
|--------|-------------|
| `total_time` | Wall-clock time in seconds |
| `avg_latency_ms` | Time per record in milliseconds |
| `cpu_user` | User CPU time (seconds) |
| `cpu_sys` | System CPU time (seconds) |
| `syscalls_read` | Number of read syscalls (Linux only) |
| `syscalls_write` | Number of write syscalls (Linux only) |
| `bytes_read` | Total bytes read (Linux only) |
| `bytes_written` | Total bytes written (Linux only) |

---

## Storage Strategies Explained

### 1. Single File (`SingleFileStorage`)
- All records in one binary file
- Sidecar index for random access
- **Best for**: Sequential reads, simple workflows

### 2. Chunked Files (`ChunkedStorage`)
- 1000 records per chunk file
- Binary index maps record → (chunk, offset, size)
- **Best for**: Balanced workloads, parallelism

### 3. Individual Files (`IndividualFileStorage`)
- One file per record
- No index needed (filename encodes position)
- **Best for**: Maximum isolation, selective access

---

## Interpreting Results

### Expected Performance

| Operation | Usually Fastest | Why |
|----------|----------------|-----|
| Write | Single File | Sequential I/O, minimal syscall overhead |
| Sequential Read | Single File | Contiguous data, excellent cache locality |
| Random Read | Varies | Single has index lookup, Individual has file open overhead |

### Key Observations

1. **Single File** typically wins on sequential reads due to OS cache efficiency
2. **Individual Files** has highest syscall overhead (100K open/close operations)
3. **Chunked** provides balance between parallelism and efficiency

---

## Troubleshooting

### "Too many open files" error

```bash
# Check current limit
ulimit -n

# Increase limit (macOS)
ulimit -n 10240
```

### Slow Individual Files strategy

This is expected! Creating 100,000 files takes time. Use `--skip-individual` for faster iteration.

### I/O stats show N/A

I/O stats (`/proc/self/io`) are only available on Linux. On macOS, you'll see "N/A" for syscall counts.

### Cache not dropping

The `--drop-cache` option requires root privileges on Linux:
```bash
sudo python benchmark.py --drop-cache
```

---

## Example Workflows

### Quick Smoke Test
```bash
python benchmark.py --records 1000 --skip-individual
```

### Full Benchmark with Cache Clearing
```bash
sudo python benchmark.py --drop-cache
```

### Compare Strategies Only (No I/O Stats)
```bash
python benchmark.py --records 50000
```

### Analyze Results in Python
```bash
python -c "
import json
with open('results/results.json') as f:
    data = json.load(f)
    for b in data['benchmarks']:
        print(f\"{b['strategy']:20s} {b['phase']:6s} {b['pattern']:10s} {b['total_time_s']:.3f}s\")
"
```

---

## Data Generation

Records are generated with:
- **Count**: 100,000 (configurable)
- **Size**: 1-2 KB (uniform random)
- **Seed**: 42 (fixed for reproducibility)

To generate data separately:
```python
from generate import generate_records, get_record_sizes

records = generate_records(n=10000)  # Generate 10K records
stats = get_record_sizes(records)
print(f"Total: {stats['total_mb']:.1f} MB")
```

---

## Adding New Strategies

To add a new storage strategy:

1. Create `strategies/my_strategy.py`
2. Inherit from `BaseStorage`:
   ```python
   from base import BaseStorage

   class MyStorage(BaseStorage):
       def __init__(self, base_dir):
           super().__init__(base_dir, "MyStrategy")

       def write_all(self, records):
           # Your implementation
           pass

       def read_sequential(self):
           # Your implementation
           pass

       def read_random(self, indices):
           # Your implementation
           pass
   ```

3. Import and run in `benchmark.py`:
   ```python
   from strategies import SingleFileStorage, ChunkedStorage, IndividualFileStorage
   from strategies.my_strategy import MyStorage

   # Add to benchmark loop
   results = run_benchmark(MyStorage, ...)
   ```

---

## Performance Tips

1. **Use `--skip-individual`** during development
2. **Run multiple times** and average results
3. **Use `--drop-cache`** for more accurate cold-start measurements
4. **Check I/O stats** on Linux for deeper insights
5. **Monitor system resources** with `htop` during runs

---

## Files Generated

During benchmark execution, these directories are created:

```
data/
├── single/           # Single file strategy
│   ├── records.bin
│   └── index.bin
├── chunked/          # Chunked strategy
│   ├── chunk_0000.bin
│   ├── chunk_0001.bin
│   └── index.bin
└── individual/      # Individual files strategy
    ├── b000/
    ├── b001/
    └── ...

results/
└── results.json     # Benchmark results
```

These are automatically cleaned on each run.

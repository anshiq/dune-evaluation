# My Learnings
## Project Overview
This project benchmarks three different storage strategies for handling large numbers of small binary records (sub-records). It evaluates write performance, sequential read, and random read patterns to understand the trade-offs between different file organization approaches.
The project simulates a real DUNE experiment use case where 100,000 small binary sub-records (1–2 KB each, ~146 MB total) need to be stored and retrieved efficiently across three layouts: a single flat file, chunked files (1,000 records each), and individual per-record files.

## What I Learned
1. Reproducibility through seeded randomness
I learned how to use a fixed random seed (seed=42) to make stochastic data generation fully deterministic. This is essential in benchmarking — without it, every run produces different record sizes and content, making results incomparable. The same principle applies broadly in scientific computing and ML experiments where reproducibility is a first-class requirement.
2. Why filesystems hate large numbers of tiny files
I already had an intuition about this from experience with node_modules and system caches (while deleting), but now I understand that why at a systems level. Every file requires its own inode (a kernel data structure storing metadata), and opening a file triggers multiple syscalls (open, read, close). With 100,000 individual files, sequential reading produced 200,002 syscalls versus ~37,500 for a single file — a 279.7× slowdown in wall-clock time (34.4s vs 0.12s). The kernel's read-ahead prefetcher, which dramatically accelerates sequential I/O on contiguous data, simply cannot help when data is scattered across thousands of independent files.
3. Indexing and the chunking
I learned how binary index files work in practice — storing fixed-width (offset, size) pairs (16 bytes each using struct packing) that allow O(1) random access into large data files without scanning. More importantly, I learned why chunking is the architectural sweet spot: Chunked Files achieved write performance within 10% of a single flat file, random read within 30%, while also enabling chunk-level parallelism — different clients can write or read different chunks simultaneously with no synchronization needed. This is exactly the design DUNE's FORM I/O framework uses.
4. Reading kernel I/O statistics via /proc/self/io
This was completely new to me. Linux exposes per-process I/O counters at /proc/self/io, including rchar/wchar (bytes transferred), syscr/syscw (read/write syscall counts), and more. Reading this before and after a benchmark gives you precise syscall deltas without any external profiling tools. Combined with resource.getrusage() for CPU user/system time split, you get a full picture of where time is actually spent — I/O wait, kernel overhead, or actual computation.
5. The real cost is syscall overhead, not data volume
Perhaps the most counterintuitive insight: all three strategies move the same ~146 MB of data. The dramatic performance differences (up to 279×) come entirely from how many times the kernel is invoked, not from the bytes transferred. Individual file write spent 8.6s in kernel (system) CPU time vs 1.5s for Single File — same data, 5.7× more kernel work. This reframes how to think about I/O optimization: reducing syscalls often matters more than reducing bytes.
6. OS read-ahead prefetching is a hidden performance multiplier
Sequential reads on a single file (0.12s) were far faster than the raw throughput of the storage would predict, because the Linux kernel speculatively reads ahead into page cache. This optimization only works on contiguous, predictable access patterns. Chunked files partially benefit from it (one file at a time). Individual files get no benefit at all since the kernel can't predict which inode comes next.


## Learning Sources: 
1. Claude / Chatgpt
2. https://www.geeksforgeeks.org/python/inheritance-in-python/
3. https://realpython.com/read-write-files-python/
4. https://www.geeksforgeeks.org/operating-systems/different-types-of-system-calls-in-os/
5. https://www.geeksforgeeks.org/operating-systems/inode-in-operating-system/
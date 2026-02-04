# GPU Acceleration Benchmark Results

**Date:** 2026-02-04
**System:** AMD Ryzen (20 threads, 3.6 GHz), 25.6 MB L3 cache
**GPU:** NVIDIA GeForce RTX 5070 Ti (sm_120, 16 GB VRAM, 70 SMs)
**Environment:** WSL2, CUDA 13.1
**Build:** Release (-O3), AVX2 SIMD enabled

## Summary

| Approach | 1 MB | 10 MB | 50 MB | 100 MB | 250 MB |
|----------|------|-------|-------|--------|--------|
| **CPU SplitFields** | **1.27 GB/s** | **1.57 GB/s** | **1.56 GB/s** | **1.53 GB/s** | **1.53 GB/s** |
| CPU CountRows | 10.6 GB/s | 10.1 GB/s | 7.4 GB/s | 7.4 GB/s | 7.6 GB/s |
| GPU Full | 0.21 GB/s | 0.25 GB/s | 0.20 GB/s | 0.17 GB/s | 0.16 GB/s |
| Hybrid GPU+CPU | 0.18 GB/s | 0.20 GB/s | 0.16 GB/s | 0.14 GB/s | 0.13 GB/s |

**Key finding: The CPU SIMD approach is 6-10x faster than the GPU approach for field boundary detection.** The CPU SIMD row counting is even faster at 7-10 GB/s.

## Detailed Results

### CPU Baseline: SplitFields (field boundary counting)

| Size | Time (ms) | Throughput (GB/s) |
|------|-----------|-------------------|
| 1 MB | 0.77 | 1.27 |
| 10 MB | 6.23 | 1.57 |
| 50 MB | 31.3 | 1.56 |
| 100 MB | 63.7 | 1.53 |
| 250 MB | 160 | 1.53 |
| 10 MB (quoted) | 7.58 | 1.29 |
| 100 MB (quoted) | 74.5 | 1.31 |

### CPU Baseline: ChunkFinder row counting (SIMD)

| Size | Time (ms) | Throughput (GB/s) |
|------|-----------|-------------------|
| 1 MB | 0.09 | 10.6 |
| 10 MB | 0.97 | 10.1 |
| 50 MB | 6.59 | 7.4 |
| 100 MB | 13.3 | 7.4 |
| 250 MB | 32.0 | 7.6 |
| 10 MB (quoted) | 1.05 | 9.3 |
| 100 MB (quoted) | 13.3 | 7.4 |

### GPU Full Pipeline

| Size | Time (ms) | Throughput (GB/s) | H2D (ms) | Kernel (ms) | D2H (ms) |
|------|-----------|-------------------|-----------|-------------|-----------|
| 1 MB | 4.64 | 0.21 | - | - | - |
| 10 MB | 40.6 | 0.25 | 1.4 | 1.7 | 38.4 |
| 50 MB | 246 | 0.20 | - | - | - |
| 100 MB | 579 | 0.17 | 11.6 | 7.8 | 559 |
| 250 MB | 1510 | 0.16 | - | - | - |
| 10 MB (quoted) | 38.4 | 0.25 | - | - | - |
| 100 MB (quoted) | 576 | 0.17 | - | - | - |

### Hybrid GPU+CPU

| Size | Time (ms) | Throughput (GB/s) |
|------|-----------|-------------------|
| 1 MB | 5.53 | 0.18 |
| 10 MB | 48.2 | 0.20 |
| 50 MB | 314 | 0.16 |
| 100 MB | 687 | 0.14 |
| 250 MB | 1831 | 0.13 |
| 10 MB (quoted) | 48.3 | 0.20 |
| 100 MB (quoted) | 649 | 0.15 |

## Analysis

### Why GPU is slower

1. **D2H transfer dominates (95%+ of time):** After fixing timing events, the real bottleneck is clear: copying boundary positions from GPU to host takes 38 ms for 10 MB and 559 ms for 100 MB. This is because the positions array is large (one entry per delimiter/newline).

2. **Kernel execution is actually fast:** The GPU kernels complete in 1.7 ms for 10 MB and 7.8 ms for 100 MB (~5.9 GB/s and ~12.8 GB/s kernel-only throughput). This is competitive with CPU SIMD.

3. **H2D transfer is efficient:** At 10 MB, H2D is only 1.4 ms (~7.1 GB/s). PCIe transfer of raw data is not the bottleneck.

4. **Output size is the problem:** For a 10-column CSV, every row produces ~10 boundary positions (9 commas + 1 newline). A 100 MB file produces millions of 4-byte positions that must be copied back, taking far longer than the original data transfer.

5. **Hybrid adds overhead:** The CPU sort + scan of positions adds additional time on top of GPU time.

### Quote handling impact

- **CPU:** ~17% throughput reduction with quoted fields (1.57 → 1.29 GB/s)
- **GPU:** No measurable difference (0.25 → 0.25 GB/s at 10 MB)
- GPU quote handling with CUB prefix scan doesn't add significant overhead because the kernel is already bottlenecked on atomic writes.

### CPU SIMD is fast

- ChunkFinder row counting: 7-10 GB/s (approaches memory bandwidth)
- SplitFields field boundary: 1.3-1.6 GB/s (still very fast, more work per byte)
- Both scale linearly with data size (good cache behavior)

## Conclusion

**GPU acceleration is not beneficial for CSV field boundary detection in the current implementation.** The CPU SIMD approach (using Highway's portable SIMD with CLMUL prefix XOR for quote parity) is 6-10x faster.

### Root causes

1. **Output amplification:** Field boundary detection produces output proportional to the number of fields, not the input size. For a 10-column CSV, the positions array is ~40% the size of the input data, making D2H transfer expensive.

2. **GPU kernels are fast, PCIe is the bottleneck:** The kernel achieves 5-13 GB/s (competitive with CPU SIMD), but moving results across PCIe negates the advantage.

3. **CPU SIMD is already near optimal:** Highway's 64-byte-at-a-time scanning with boundary caching achieves 1.5 GB/s for full field detection and 7-10 GB/s for row counting. The CLMUL instruction provides 1-2 cycle prefix XOR, matching what CUB does with much less overhead.

### What would make GPU worthwhile

Based on the Kumaigorodski et al. paper (76 GB/s), GPU acceleration could help if:

1. **Full parsing pipeline on GPU:** Keep data on GPU for multiple passes (boundary detection → type inference → value extraction), avoiding round-trip transfers
2. **Stream processing:** Use CUDA streams with pinned memory for overlapped H2D and kernel execution
3. **Larger data sizes (1+ GB):** When data is already in GPU memory (e.g., GPU-direct storage)
4. **Avoid atomic writes:** Use prefix sum to compute output offsets, then write without contention

### Recommendation

Keep the GPU module as an experimental/optional feature for future exploration, but **do not integrate GPU acceleration into the default parsing path.** The CPU SIMD implementation is already excellent for the common case.

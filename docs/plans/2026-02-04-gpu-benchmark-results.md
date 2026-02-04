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
| 10 MB | 38.7 | 0.25 | 1.57 | 37.8 | 0.003 |
| 50 MB | 246 | 0.20 | - | - | - |
| 100 MB | 564 | 0.17 | 16.1 | 548 | 0.006 |
| 250 MB | 1510 | 0.16 | 69.0 | 1440 | 0.003 |
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

1. **Kernel execution dominates (97% of time):** The kernel takes 37.8 ms for 10 MB and 1440 ms for 250 MB. This is only ~170 MB/s throughput on the kernel alone, far below the GPU's memory bandwidth.

2. **Low GPU occupancy:** The current kernel uses `atomicAdd` for unordered output, which serializes threads. Each thread processes one byte, but the atomic contention on the output counter destroys parallelism.

3. **H2D transfer is modest:** At 10 MB, H2D is only 1.57 ms (~6.4 GB/s). At 250 MB, H2D is 69 ms (~3.6 GB/s). PCIe transfer is not the bottleneck.

4. **D2H transfer is negligible:** Positions array is small, D2H < 0.01 ms.

5. **Hybrid is even slower:** The sort step on CPU (`std::sort` on positions) adds significant overhead on top of GPU time.

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

1. **CSV parsing is fundamentally sequential:** Quote state is a prefix dependency — each byte's interpretation depends on all preceding quote characters. While CUB prefix scan solves this on GPU, the overhead is high for this simple operation.

2. **CPU SIMD is already near optimal:** Highway's 64-byte-at-a-time scanning with boundary caching achieves excellent throughput. The CLMUL instruction provides 1-2 cycle prefix XOR, matching what CUB does with much more overhead.

3. **Insufficient parallelism:** Field boundary detection is a single-pass scan that processes 1 byte → 1 bit. This doesn't have enough arithmetic intensity to justify GPU transfer overhead.

### What would make GPU worthwhile

Based on the Kumaigorodski et al. paper (76 GB/s), GPU acceleration could help if:

1. **Full parsing pipeline on GPU:** Keep data on GPU for multiple passes (boundary detection → type inference → value extraction), avoiding round-trip transfers
2. **Stream processing:** Use CUDA streams with pinned memory for overlapped H2D and kernel execution
3. **Larger data sizes (1+ GB):** When data is already in GPU memory (e.g., GPU-direct storage)
4. **Avoid atomic writes:** Use prefix sum to compute output offsets, then write without contention

### Recommendation

Keep the GPU module as an experimental/optional feature for future exploration, but **do not integrate GPU acceleration into the default parsing path.** The CPU SIMD implementation is already excellent for the common case.

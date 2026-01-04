# Fuzz Testing for simdcsv

Fuzz testing infrastructure using libFuzzer.

## Building

```bash
cmake -B build-fuzz -DENABLE_FUZZING=ON -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
cmake --build build-fuzz
```

## Running

```bash
./build-fuzz/fuzz_csv_parser build-fuzz/fuzz_corpus -max_len=65536
```

## OSS-Fuzz

See `oss-fuzz/` for Google OSS-Fuzz integration.

#!/bin/bash -eu
mkdir -p build && cd build
cmake .. -DCMAKE_C_COMPILER="$CC" -DCMAKE_CXX_COMPILER="$CXX" -DCMAKE_C_FLAGS="$CFLAGS" -DCMAKE_CXX_FLAGS="$CXXFLAGS" -DCMAKE_BUILD_TYPE=Release
cmake --build . --target simdcsv_lib -j$(nproc)
cd ..
$CXX $CXXFLAGS -std=c++17 -I./include -DSIMDCSV_BENCHMARK_MODE fuzz/fuzz_csv_parser.cpp -o $OUT/fuzz_csv_parser build/libsimdcsv_lib.a build/_deps/highway-build/libhwy.a $LIB_FUZZING_ENGINE
$CXX $CXXFLAGS -std=c++17 -I./include -DSIMDCSV_BENCHMARK_MODE fuzz/fuzz_dialect_detection.cpp -o $OUT/fuzz_dialect_detection build/libsimdcsv_lib.a build/_deps/highway-build/libhwy.a $LIB_FUZZING_ENGINE
$CXX $CXXFLAGS -std=c++17 -I./include -DSIMDCSV_BENCHMARK_MODE fuzz/fuzz_parse_auto.cpp -o $OUT/fuzz_parse_auto build/libsimdcsv_lib.a build/_deps/highway-build/libhwy.a $LIB_FUZZING_ENGINE

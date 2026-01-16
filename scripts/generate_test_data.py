#!/usr/bin/env python3
"""
Generate test data files for hypothesis-driven benchmarks.

Standard test files:
- narrow_1M: 1M rows × 10 cols (3 int, 3 double, 4 string) - Primary benchmark
- wide_100K: 100K rows × 100 cols (mixed) - Tests wide data
- narrow_10M: 10M rows × 5 cols (simple) - Large file test
- quoted_heavy: 1M rows × 10 cols (50% quoted fields) - Tests quote handling
- escape_heavy: 1M rows × 10 cols (strings with "") - Tests escape handling

Usage:
    python scripts/generate_test_data.py [output_dir]

If output_dir is not specified, defaults to benchmark/test_data/
"""

import argparse
import random
import string
from pathlib import Path


def random_string(min_len: int = 5, max_len: int = 20) -> str:
    """Generate a random alphanumeric string."""
    length = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def random_quoted_string(min_len: int = 5, max_len: int = 20) -> str:
    """Generate a random string that should be quoted (contains comma or newline)."""
    length = random.randint(min_len, max_len)
    base = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))
    # Add a comma or space to force quoting
    return f'"{base}, extra"'


def random_escape_string(min_len: int = 5, max_len: int = 15) -> str:
    """Generate a random string with embedded quotes that need escaping."""
    length = random.randint(min_len, max_len)
    base = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    # Add escaped quotes
    return f'"{base}""inside""{random_string(3, 5)}"'


def generate_narrow_1m(output_path: Path) -> None:
    """
    Generate narrow_1M.csv: 1M rows × 10 cols (3 int, 3 double, 4 string).
    Primary benchmark file for most hypothesis tests.
    """
    print(f"Generating narrow_1M.csv...")
    with open(output_path / "narrow_1M.csv", "w") as f:
        # Header
        headers = ["int1", "int2", "int3", "dbl1", "dbl2", "dbl3",
                   "str1", "str2", "str3", "str4"]
        f.write(",".join(headers) + "\n")

        # Data rows
        for i in range(1_000_000):
            row = [
                str(random.randint(-1000000, 1000000)),
                str(random.randint(0, 9999)),
                str(random.randint(-100, 100)),
                f"{random.uniform(-1000, 1000):.6f}",
                f"{random.uniform(0, 1):.10f}",
                f"{random.uniform(-1e10, 1e10):.2e}",
                random_string(),
                random_string(1, 10),
                random_string(10, 30),
                random_string(),
            ]
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'narrow_1M.csv'}")


def generate_wide_100k(output_path: Path) -> None:
    """
    Generate wide_100K.csv: 100K rows × 100 cols (mixed types).
    Tests wide data performance.
    """
    print(f"Generating wide_100K.csv...")
    with open(output_path / "wide_100K.csv", "w") as f:
        # Header: 25 int, 25 double, 50 string
        headers = [f"int{i}" for i in range(25)]
        headers += [f"dbl{i}" for i in range(25)]
        headers += [f"str{i}" for i in range(50)]
        f.write(",".join(headers) + "\n")

        # Data rows
        for i in range(100_000):
            row = []
            # 25 integers
            for _ in range(25):
                row.append(str(random.randint(-1000000, 1000000)))
            # 25 doubles
            for _ in range(25):
                row.append(f"{random.uniform(-1000, 1000):.6f}")
            # 50 strings
            for _ in range(50):
                row.append(random_string(5, 15))
            f.write(",".join(row) + "\n")

            if (i + 1) % 10000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'wide_100K.csv'}")


def generate_narrow_10m(output_path: Path) -> None:
    """
    Generate narrow_10M.csv: 10M rows × 5 cols (simple types).
    Large file test.
    """
    print(f"Generating narrow_10M.csv...")
    with open(output_path / "narrow_10M.csv", "w") as f:
        # Header
        headers = ["id", "value", "amount", "name", "code"]
        f.write(",".join(headers) + "\n")

        # Data rows
        for i in range(10_000_000):
            row = [
                str(i),
                str(random.randint(0, 9999)),
                f"{random.uniform(0, 10000):.2f}",
                random_string(5, 15),
                random_string(3, 8),
            ]
            f.write(",".join(row) + "\n")

            if (i + 1) % 1000000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'narrow_10M.csv'}")


def generate_quoted_heavy(output_path: Path) -> None:
    """
    Generate quoted_heavy.csv: 1M rows × 10 cols with 50% quoted fields.
    Tests quote handling overhead.
    """
    print(f"Generating quoted_heavy.csv...")
    with open(output_path / "quoted_heavy.csv", "w") as f:
        # Header
        headers = [f"col{i}" for i in range(10)]
        f.write(",".join(headers) + "\n")

        # Data rows - 50% of fields are quoted
        for i in range(1_000_000):
            row = []
            for _ in range(10):
                if random.random() < 0.5:
                    row.append(random_quoted_string())
                else:
                    row.append(random_string())
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'quoted_heavy.csv'}")


def generate_escape_heavy(output_path: Path) -> None:
    """
    Generate escape_heavy.csv: 1M rows × 10 cols with strings containing "".
    Tests escape sequence handling.
    """
    print(f"Generating escape_heavy.csv...")
    with open(output_path / "escape_heavy.csv", "w") as f:
        # Header
        headers = [f"col{i}" for i in range(10)]
        f.write(",".join(headers) + "\n")

        # Data rows - most fields have escape sequences
        for i in range(1_000_000):
            row = []
            for _ in range(10):
                if random.random() < 0.7:  # 70% have escape sequences
                    row.append(random_escape_string())
                else:
                    row.append(random_string())
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'escape_heavy.csv'}")


def generate_variable_sizes(output_path: Path) -> None:
    """
    Generate test files for H1 test matrix:
    (rows, cols) ∈ [(10K, 10), (100K, 10), (1M, 10), (100K, 100), (100K, 1000)]
    """
    test_matrix = [
        (10_000, 10, "10k_10c"),
        (100_000, 10, "100k_10c"),
        (1_000_000, 10, "1m_10c"),
        (100_000, 100, "100k_100c"),
        (100_000, 1000, "100k_1000c"),
    ]

    for nrows, ncols, name in test_matrix:
        filename = f"matrix_{name}.csv"
        print(f"Generating {filename}...")

        with open(output_path / filename, "w") as f:
            # Header
            headers = [f"col{i}" for i in range(ncols)]
            f.write(",".join(headers) + "\n")

            # Data rows - simple integers for consistent field sizes
            for i in range(nrows):
                row = [str(random.randint(0, 9999)) for _ in range(ncols)]
                f.write(",".join(row) + "\n")

                if nrows >= 100000 and (i + 1) % 100000 == 0:
                    print(f"  {i + 1:,} rows written...")

        print(f"  Done: {output_path / filename}")


def generate_threading_test(output_path: Path) -> None:
    """
    Generate a ~500MB file for H3 threading benchmark.
    """
    print(f"Generating threading_500mb.csv (~500MB)...")
    target_size = 500 * 1024 * 1024  # 500MB

    with open(output_path / "threading_500mb.csv", "w") as f:
        # Header - 20 columns for reasonable width
        headers = [f"col{i}" for i in range(20)]
        f.write(",".join(headers) + "\n")

        # Estimate: ~20 cols * ~10 chars each = ~200 bytes/row
        # 500MB / 200 = ~2.5M rows
        current_size = 0
        row_count = 0

        while current_size < target_size:
            row = []
            for j in range(20):
                if j < 5:
                    row.append(str(random.randint(-1000000, 1000000)))
                elif j < 10:
                    row.append(f"{random.uniform(-1000, 1000):.4f}")
                else:
                    row.append(random_string(5, 15))

            line = ",".join(row) + "\n"
            f.write(line)
            current_size += len(line)
            row_count += 1

            if row_count % 500000 == 0:
                print(f"  {row_count:,} rows, {current_size / (1024*1024):.1f}MB written...")

    print(f"  Done: {output_path / 'threading_500mb.csv'} ({current_size / (1024*1024):.1f}MB, {row_count:,} rows)")


def generate_nyc_taxi_synthetic(output_path: Path, nrows: int = 1_000_000) -> None:
    """
    Generate synthetic NYC Taxi-like data with realistic column types.

    This synthetic data mimics the NYC Taxi schema with:
    - Integer IDs (VendorID, passenger_count, etc.)
    - Floating point amounts (fare_amount, trip_distance, etc.)
    - Timestamps (pickup/dropoff datetime)
    - Location IDs (PULocationID, DOLocationID)
    - Payment types and rate codes

    Useful for H4 (escape sequences - rare in numeric data) and
    H5 (type widening - decimal amounts that may need int->float).
    """
    print(f"Generating nyc_taxi_synthetic.csv ({nrows:,} rows)...")

    headers = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge"
    ]

    with open(output_path / "nyc_taxi_synthetic.csv", "w") as f:
        f.write(",".join(headers) + "\n")

        base_date = "2024-01-15 "

        for i in range(nrows):
            # Realistic NYC taxi data distributions
            vendor_id = random.choice([1, 2])
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            pickup_time = f"{base_date}{hour:02d}:{minute:02d}:00"
            duration_mins = max(1, int(random.gauss(15, 10)))
            dropoff_hour = (hour + duration_mins // 60) % 24
            dropoff_minute = (minute + duration_mins % 60) % 60
            dropoff_time = f"{base_date}{dropoff_hour:02d}:{dropoff_minute:02d}:00"

            passenger_count = random.choices([1, 2, 3, 4, 5, 6], weights=[70, 15, 8, 4, 2, 1])[0]
            trip_distance = max(0.1, random.gauss(3.0, 2.5))
            ratecode_id = random.choices([1, 2, 3, 4, 5, 6], weights=[90, 5, 2, 1, 1, 1])[0]
            store_and_fwd = random.choice(["N", "Y"]) if random.random() < 0.01 else "N"
            pu_location = random.randint(1, 265)
            do_location = random.randint(1, 265)
            payment_type = random.choices([1, 2, 3, 4], weights=[70, 25, 3, 2])[0]

            # Fare calculations
            base_fare = 3.0
            per_mile = 2.5
            fare_amount = base_fare + (trip_distance * per_mile)
            extra = random.choice([0.0, 0.5, 1.0, 2.5]) if random.random() < 0.3 else 0.0
            mta_tax = 0.5
            tip_amount = round(fare_amount * random.uniform(0, 0.3), 2) if payment_type == 1 else 0.0
            tolls_amount = round(random.uniform(0, 10), 2) if random.random() < 0.1 else 0.0
            improvement_surcharge = 0.3
            congestion_surcharge = 2.5 if pu_location <= 90 else 0.0
            total_amount = fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge

            row = [
                str(vendor_id),
                pickup_time,
                dropoff_time,
                str(passenger_count),
                f"{trip_distance:.2f}",
                str(ratecode_id),
                store_and_fwd,
                str(pu_location),
                str(do_location),
                str(payment_type),
                f"{fare_amount:.2f}",
                f"{extra:.2f}",
                f"{mta_tax:.2f}",
                f"{tip_amount:.2f}",
                f"{tolls_amount:.2f}",
                f"{improvement_surcharge:.2f}",
                f"{total_amount:.2f}",
                f"{congestion_surcharge:.2f}"
            ]
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'nyc_taxi_synthetic.csv'}")


def generate_type_widening_test(output_path: Path, nrows: int = 1_000_000) -> None:
    """
    Generate test data for H5: Type widening detection.

    Creates a file where some columns start as integers but contain
    floats later, simulating real-world type inference challenges.

    Column layout:
    - col0: Always integer
    - col1: Integer until row 50K, then floats (widening needed after sample)
    - col2: Integer until row 500K, then floats (late widening)
    - col3: Always float
    - col4: Integer with occasional floats (sparse widening)
    """
    print(f"Generating type_widening_test.csv ({nrows:,} rows)...")

    headers = ["always_int", "early_widen", "late_widen", "always_float", "sparse_widen"]

    with open(output_path / "type_widening_test.csv", "w") as f:
        f.write(",".join(headers) + "\n")

        for i in range(nrows):
            always_int = random.randint(0, 1000000)

            # Early widening: ints for first 50K rows, then floats
            if i < 50000:
                early_widen = str(random.randint(0, 1000))
            else:
                early_widen = f"{random.uniform(0, 1000):.2f}"

            # Late widening: ints for first 500K rows, then floats
            if i < 500000:
                late_widen = str(random.randint(0, 1000))
            else:
                late_widen = f"{random.uniform(0, 1000):.2f}"

            always_float = f"{random.uniform(-1000, 1000):.4f}"

            # Sparse widening: 99% ints, 1% floats randomly distributed
            if random.random() < 0.01:
                sparse_widen = f"{random.uniform(0, 1000):.2f}"
            else:
                sparse_widen = str(random.randint(0, 1000))

            row = [str(always_int), early_widen, late_widen, always_float, sparse_widen]
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'type_widening_test.csv'}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate test data files for hypothesis-driven benchmarks"
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="benchmark/test_data",
        help="Output directory for test files (default: benchmark/test_data/)"
    )
    parser.add_argument(
        "--small",
        action="store_true",
        help="Generate only small test files (skip 10M and 500MB files)"
    )
    parser.add_argument(
        "--matrix-only",
        action="store_true",
        help="Generate only the H1 test matrix files"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    parser.add_argument(
        "--taxi",
        action="store_true",
        help="Generate NYC Taxi-like synthetic data for H4/H5 testing"
    )
    parser.add_argument(
        "--h5",
        action="store_true",
        help="Generate type widening test data for H5 testing"
    )
    parser.add_argument(
        "--all-h4h5",
        action="store_true",
        help="Generate all H4/H5 test data (taxi + type widening)"
    )

    args = parser.parse_args()

    # Set random seed for reproducibility
    random.seed(args.seed)

    # Create output directory
    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {output_path.absolute()}")
    print(f"Random seed: {args.seed}")
    print()

    if args.matrix_only:
        generate_variable_sizes(output_path)
        return

    if args.taxi or args.all_h4h5:
        generate_nyc_taxi_synthetic(output_path)
        print()

    if args.h5 or args.all_h4h5:
        generate_type_widening_test(output_path)
        print()

    if args.taxi or args.h5 or args.all_h4h5:
        # If only generating H4/H5 data, return early
        if not (args.small or args.matrix_only):
            return

    # Generate standard test files
    generate_narrow_1m(output_path)
    print()

    generate_wide_100k(output_path)
    print()

    if not args.small:
        generate_narrow_10m(output_path)
        print()

    generate_quoted_heavy(output_path)
    print()

    generate_escape_heavy(output_path)
    print()

    # Generate H1 test matrix files
    generate_variable_sizes(output_path)
    print()

    if not args.small:
        # Generate large file for threading tests
        generate_threading_test(output_path)
        print()

    print("All test files generated successfully!")

    # Print summary
    print("\nGenerated files:")
    for f in sorted(output_path.glob("*.csv")):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name}: {size_mb:.1f}MB")


if __name__ == "__main__":
    main()

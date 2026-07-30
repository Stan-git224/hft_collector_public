use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::time::Duration;
use orderbook_rust::book::OrderBook;

fn bench_price_parsing_nano(c: &mut Criterion) {
    let sample_price = "90042.1234";

    c.bench_function("parser_parse_price_to_i64_fixed_point", |b| {
        b.iter(|| {
            // core: hand-rolled byte-array shifting, aiming for C++-level speed
            // fast_parse_price is an associated function exposed for testing the core behavior directly
            let _price_i64 = OrderBook::fast_parse_price(black_box(sample_price));
        })
    });
}

fn bench_book_snapshot_load(c: &mut Criterion) {
    c.bench_function("orderbook_snapshot_load_zero_alloc", |b| {
        b.iter_with_setup(
            || {
                // simulate the cleaned &str slices from a REST snapshot
                let bids = [
                    ("90000.10000000", "1.52000000"),
                    ("89999.00000000", "0.23000000"),
                ];
                let asks = [
                    ("90000.20000000", "0.11000000"),
                    ("90001.50000000", "4.10000000"),
                ];
                let book = OrderBook::new(8);
                (book, bids, asks)
            },
            |(mut book, bids, asks)| {
                // core: load the snapshot, call the inlined fast_parse, update the BTreeMap
                book.load_snapshot(165, &bids, &asks);
            }
        )
    });
}


fn bench_hot_path_diff_update(c: &mut Criterion) {
    c.bench_function("hot_path_update_levels_zero_copy", |b| {
        b.iter_with_setup(
            || {
                let init_bids = [
                    ("90000.00000000", "1.50000000"),
                    ("89999.00000000", "2.30000000"),
                    ("89998.00000000", "5.00000000"),
                ];
                let init_asks = [
                    ("90001.00000000", "0.50000000"),
                    ("90002.00000000", "1.10000000"),
                    ("90003.00000000", "4.00000000"),
                ];
                let mut fresh_book = OrderBook::new(8);
                fresh_book.load_snapshot(100, &init_bids, &init_asks);

                // simulate the diff slices the WSS pushes in every microsecond
                let diff_bids = [
                    ("90000.00000000", "2.88000000"),  // update
                    ("89999.50000000", "0.50000000"),  // insert
                    ("89998.00000000", "0.00000000"),  // delete
                ];
                let diff_asks = [
                    ("90001.00000000", "0.00000000"), // delete
                    ("190004.0000000", "3.15000000"),  // insert
                ];
                (fresh_book, diff_bids, diff_asks)
            },
            |(mut fresh_book, diff_bids, diff_asks)| {
                // core: WSS hot-path diff parsing plus red-black tree node changes
                fresh_book.update_levels(&diff_bids, &diff_asks);
            }
        )
    });
}


fn bench_pure_numeric_hot_path(c: &mut Criterion) {

    // 1. simulate the clean data the front-line Processor already parsed (i64 price, f64 qty)
    // update / insert / delete (0.0)
    let pre_parsed_bids = [
        (9000000000000i64, 2.88f64),
        (8999950000000i64, 0.50f64),
        (8999800000000i64, 0.00f64),
    ];
    let pre_parsed_asks = [
        (9000100000000i64, 0.00f64),
        (9000400000000i64, 3.15f64),
    ];

    c.bench_function("zero_alloc_pure_numeric_update", |b| {
        b.iter_with_setup(
            || {
                let init_bids = [
                    ("90000.00000000", "1.50000000"),
                    ("89999.00000000", "2.30000000"),
                    ("89998.00000000", "5.00000000"),
                ];
                let init_asks = [
                    ("90001.00000000", "0.50000000"),
                    ("90002.00000000", "1.10000000"),
                    ("90003.00000000", "4.00000000"),
                ];
                let mut fresh_book = OrderBook::new(8);
                fresh_book.load_snapshot(100, &init_bids, &init_asks);
                return (fresh_book, pre_parsed_bids, pre_parsed_asks);
            },
    |(mut fresh_book, pb, pa)| {
                // pure-numeric interface: pass by reference, no allocation
                fresh_book.update_pure_numbers(&pb, &pa);
            }
        )
    });
}



criterion_group!{
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(5));
    // run the core high-frequency performance benchmarks
    targets = bench_price_parsing_nano, bench_book_snapshot_load,bench_hot_path_diff_update, bench_pure_numeric_hot_path//, bench_hot_path_diff_update
}
criterion_main!(benches);

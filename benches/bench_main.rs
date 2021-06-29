use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::load_triples::benches,
}

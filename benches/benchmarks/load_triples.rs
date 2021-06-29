use criterion::{black_box, criterion_group, criterion_main, Criterion};

use lasso::Rodeo;
use rdfs_materialization::load_encode_triples::load3nt;

pub fn load_triples(c: &mut Criterion) {
    c.bench_function("load encode triple", |b| {
        b.iter(|| {
            black_box({
                let triples = load3nt(0, "./tests/data/", "lubm50.nt");
                let mut rodeo = Rodeo::default();

                for triple in triples {
                    let s = &triple.0[..];
                    let p = &triple.1[..];
                    let o = &triple.2[..];

                    let key_s = rodeo.get_or_intern(s);
                    let key_p = rodeo.get_or_intern(p);
                    let key_o = rodeo.get_or_intern(o);
                }
            })
        })
    });
}

criterion_group!(benches, load_triples);

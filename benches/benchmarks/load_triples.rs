use criterion::{black_box, criterion_group, criterion_main, Criterion};
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::JoinCore;
use lasso::{Key, Rodeo, Spur};
use rdfs_materialization::load_encode_triples::{load3enc, load3nt};
use timely::dataflow::operators::Map;
// Avg runtime of 650 msecs
pub fn load_encoded_triples(c: &mut Criterion) {
    c.bench_function("load encoded triples", |b| {
	b.iter(|| {
	    black_box({
		let abox_triples = load3enc("./tests/data/", "lubm50.ntenc");
		let tbox_triples = load3enc("./tests/data/", "tbox.ntenc");

		let mut abox_triple_count: usize = 0usize;
		let mut tbox_triple_count: usize = 0usize;

		let (mut s, mut p, mut o) = (0usize, 0usize, 0usize);

		let timer = ::std::time::Instant::now();

		tbox_triples.for_each(|triple| {
		    
		    s = triple.0;
		    p = triple.1;
		    o = triple.2;

		    tbox_triple_count = tbox_triple_count + 1;
		    
		});

		abox_triples.for_each(|triple| {
		    
		    s = triple.0;
		    p = triple.1;
		    o = triple.2;

		    abox_triple_count = abox_triple_count + 1;
		    
		});

	    })
	})
    });

}
// Avg runtime of 4000 msecs
pub fn load_encode_triples(c: &mut Criterion) {
    c.bench_function("load encode triple", |b| {
        b.iter(|| {
            black_box({
                let triples = load3nt("./tests/data/", "lubm50.nt");
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

criterion_group!(benches, load_encoded_triples, load_encode_triples);

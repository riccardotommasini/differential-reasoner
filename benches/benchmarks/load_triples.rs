use criterion::{black_box, criterion_group, criterion_main, Criterion};
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::JoinCore;
use lasso::{Key, Rodeo, Spur};
use rdfs_materialization::load_encode_triples::{load3enc, load3nt};
use timely::dataflow::operators::Map;

pub fn load_encode_triples(c: &mut Criterion) {
    c.bench_function("load encode triple", |b| {
        b.iter(|| {
            black_box({
                let triples = load3nt(0, "./tests/data/", "lubm5.nt");
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

pub fn load_encode_insert_triples(c: &mut Criterion) {
    c.bench_function("load encode insert triple", |b| {
        b.iter(|| {
            black_box({
                let triples = load3nt(0, "./tests/data/", "lubm5.nt");
                let mut rodeo = Rodeo::default();
                /// URI of the rdfs:subClassOf
                pub static RDFS_SUB_CLASS_OF: &str =
                    "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
                /// URI of the rdfs:subPropertyOf
                pub static RDFS_SUB_PROPERTY_OF: &str =
                    "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
                /// URI of the rdfs::domain
                pub static RDFS_DOMAIN: &str = "<http://www.w3.org/2000/01/rdf-schema#domain>";
                /// URI of the rdfs::range
                pub static RDFS_RANGE: &str = "<http://www.w3.org/2000/01/rdf-schema#range>";
                /// URI of rdf:type
                pub static RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
                /// URI of owl:TransitiveProperty
                pub static OWL_TRANSITIVE_PROPERTY: &str =
                    "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
                /// URI of owl:inverseOf
                pub static OWL_INVERSE_OF: &str = "<http://www.w3.org/2002/07/owl#inverseOf>";

                rodeo.get_or_intern(RDFS_SUB_CLASS_OF);
                rodeo.get_or_intern(RDFS_SUB_PROPERTY_OF);
                rodeo.get_or_intern(RDFS_DOMAIN);
                rodeo.get_or_intern(RDFS_RANGE);
                rodeo.get_or_intern(RDF_TYPE);
                rodeo.get_or_intern(OWL_TRANSITIVE_PROPERTY);
                rodeo.get_or_intern(OWL_INVERSE_OF);

                timely::execute_directly(move |worker| {
                    let mut input_stream = worker.dataflow::<(), _, _>(move |outer| {
                        let (_abox_in, abox) =
                            outer.new_collection::<(usize, usize, usize), isize>();

                        /*
                        let sp_o = abox.map(|(s, p, o)| ((s, p), o)).arrange_by_key();
                        let op_o = abox.map(|(s, p, o)| ((o, p), s)).arrange_by_key();

                        sp_o.join_core(&op_o, |&(_y, p), &z, &x| Some((x, p, z)));
                         */
                        (_abox_in)
                    });

                    for triple in triples {
                        let s = &triple.0[..];
                        let p = &triple.1[..];
                        let o = &triple.2[..];

                        let key_s = rodeo.get_or_intern(s);
                        let key_p = rodeo.get_or_intern(p);
                        let key_o = rodeo.get_or_intern(o);

                        let key_s_int = key_s.into_usize();
                        let key_p_int = key_p.into_usize();
                        let key_o_int = key_o.into_usize();

                        input_stream.insert((key_s_int, key_p_int, key_o_int));
                    }
                    input_stream.flush();
                    input_stream.advance_to(());
                })
            })
        })
    });
}

criterion_group!(benches, load_encode_insert_triples, load_encode_triples);

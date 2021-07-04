/* use rdfs_materialization; */

mod utils;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Threshold;

use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::trace::cursor::CursorDebug;
use differential_dataflow::trace::TraceReader;
use lasso::{Key, Rodeo, Spur};
use rdfs_materialization::load_encode_triples::{load3enc, load3nt};
use rdfs_materialization::rdfs_materialization::*;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::Probe;
use timely::progress::frontier::AntichainRef;

#[test]
fn loading_triples() {
    let triples = load3nt(0, "./tests/data/", "tiny_abox.nt");

    let mut length = 0;

    for triple in triples {
        length = length + 1;
    }

    assert_eq!(length, 7);
}

#[test]
fn loading_encoding_triples() {
    let triples = load3nt(0, "./tests/data/", "tiny_abox.nt");
    let mut rodeo = Rodeo::default();

    let mut length = 0;

    for triple in triples {
        length = length + 1;

        let s = &triple.0[..];
        let p = &triple.1[..];
        let o = &triple.2[..];

        let key_s = rodeo.get_or_intern(s);
        let key_p = rodeo.get_or_intern(p);
        let key_o = rodeo.get_or_intern(o);

        assert_eq!(Some(key_s), rodeo.get(triple.0));
        assert_eq!(Some(key_p), rodeo.get(triple.1));
        assert_eq!(Some(key_o), rodeo.get(triple.2));
    }

    assert_eq!(length, 7);
}

#[test]
fn loading_encoding_inserting_triples() {
    let abox_triples = load3nt(0, "./tests/data/", "tiny_abox.nt");
    let tbox_triples = load3nt(0, "./tests/data/", "full_tbox.nt");
    let mut rodeo = Rodeo::default();

    /// URI of the rdfs:subClassOf
    pub static RDFS_SUB_CLASS_OF: &str = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
    /// URI of the rdfs:subPropertyOf
    pub static RDFS_SUB_PROPERTY_OF: &str = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
    /// URI of the rdfs::domain
    pub static RDFS_DOMAIN: &str = "<http://www.w3.org/2000/01/rdf-schema#domain>";
    /// URI of the rdfs::range
    pub static RDFS_RANGE: &str = "<http://www.w3.org/2000/01/rdf-schema#range>";
    /// URI of rdf:type
    pub static RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
    /// URI of owl:TransitiveProperty
    pub static OWL_TRANSITIVE_PROPERTY: &str = "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
    /// URI of owl:inverseOf
    pub static OWL_INVERSE_OF: &str = "<http://www.w3.org/2002/07/owl#inverseOf>";

    rodeo.get_or_intern(RDFS_SUB_CLASS_OF);
    //println!("{:?}", rodeo.get_or_intern(RDFS_SUB_CLASS_OF));
    rodeo.get_or_intern(RDFS_SUB_PROPERTY_OF);
    //println!("{:?}", rodeo.get_or_intern(RDFS_SUB_PROPERTY_OF));
    rodeo.get_or_intern(RDFS_DOMAIN);
    //println!("{:?}", rodeo.get_or_intern(RDFS_DOMAIN));
    rodeo.get_or_intern(RDFS_RANGE);
    //println!("{:?}", rodeo.get_or_intern(RDFS_RANGE));
    rodeo.get_or_intern(RDF_TYPE);
    //println!("{:?}", rodeo.get_or_intern(RDF_TYPE).into_usize());
    rodeo.get_or_intern(OWL_TRANSITIVE_PROPERTY);
    rodeo.get_or_intern(OWL_INVERSE_OF);

    let (mut tbox_summaries, mut abox_summaries) = timely::execute_directly(move |worker| {
        let mut tbox_probe = Handle::new();
        let mut abox_probe = Handle::new();
        let (mut tbox_input_stream, mut abox_input_stream, mut tbox_trace, mut abox_trace) = worker
            .dataflow::<usize, _, _>(|outer| {
                let (mut _abox_in, mut abox) =
                    outer.new_collection::<(usize, usize, usize), isize>();
                let (mut _tbox_in, mut tbox) =
                    outer.new_collection::<(usize, usize, usize), isize>();

                // tbox reasoning

                let sco = rule_11(&tbox);
                let spo = rule_5(&tbox);
                let tbox = tbox.concat(&sco).concat(&spo).distinct();

                // indexing the tbox
                let tbox_by_s = tbox.map(|(s, p, o)| (s, (p, o))).arrange_by_key();

                let tbox_trace = tbox_by_s.trace.clone();

                tbox_by_s
                    .as_collection(|_, v| *v)
                    .consolidate()
                    //.inspect(move |x| println!("{:?}", x))
                    .probe_with(&mut tbox_probe);

                let sco_assertions = tbox_by_s.filter(|s, (p, o)| p == &0usize);
                let spo_assertions = tbox_by_s.filter(|s, (p, o)| p == &1usize);
                let domain_assertions = tbox_by_s.filter(|s, (p, o)| p == &2usize);
                let range_assertions = tbox_by_s.filter(|s, (p, o)| p == &3usize);
                //let transitivity_assertions = tbox_by_s.filter(|s, (p, o)| p == &4usize);
                //let inverseof_assertions = tbox_by_s.filter(|s, (p, o)| p == &5usize);

                // preparing the abox

                let abox_by_o = abox.map(|(s, p, o)| (o, (s, p)));
                let abox_by_p = abox.map(|(s, p, o)| (p, (s, o)));

                let type_assertions = abox_by_o.filter(|(o, (s, p))| p == &4usize);

                // abox reasoning
                let sco_type = type_assertions
                    .iterate(|inner| {
                        let arr = inner.arrange_by_key();
                        let tbox_in = sco_assertions.enter(&inner.scope());

                        tbox_in
                            .join_core(&arr, |key, &(sco, y), &(z, type_)| Some((y, (z, type_))))
                            .concat(inner)
                            .distinct()
                    })
                    .map(|(y, (z, type_))| (z, type_, y))
                    .inspect(|x| println!("SCO Type inference: {:?}", x));

                let spo_type = abox_by_p
                    .iterate(|inner| {
                        let arr = inner.arrange_by_key();
                        let tbox_in = spo_assertions.enter(&inner.scope());

                        tbox_in
                            .join_core(&arr, |key, &(spo, b), &(x, y)| Some((b, (x, y))))
                            .concat(inner)
                            .distinct()
                    })
                    .map(|(b, (x, y))| (x, b, y))
                    .inspect(|x| println!("SPO Type inference: {:?}", x));

                abox = abox.concat(&sco_type).concat(&spo_type).distinct();

                let abox_by_p = abox.map(|(s, p, o)| (p, (s, o))).arrange_by_key();

                let domain_type = domain_assertions
                    .join_core(&abox_by_p, |a, &(domain, x), &(y, z)| Some((y, 4usize, x)))
                    .inspect(|x| println!("DOMAIN Type inference: {:?}", x));

                let range_type = range_assertions
                    .join_core(&abox_by_p, |a, &(range, x), &(y, z)| Some((z, 4usize, x)))
                    .inspect(|x| println!("RANGE Type inference: {:?}", x));

                abox = abox.concat(&domain_type).concat(&range_type).distinct();

                let abox_by_s = abox.arrange_by_self();

                let abox_trace = abox_by_s.trace.clone();

                abox_by_s
                    .as_collection(|_, v| *v)
                    .consolidate()
                    //.inspect(move |x| println!("{:?}", x))
                    .probe_with(&mut abox_probe);

                /*abox
                .probe_with(&mut probe);*/

                (_tbox_in, _abox_in, tbox_trace, abox_trace)
            });

        tbox_triples.for_each(|triple| {
            let s = &triple.0[..];
            let p = &triple.1[..];
            let o = &triple.2[..];

            let key_s = rodeo.get_or_intern(s);
            let key_p = rodeo.get_or_intern(p);
            let key_o = rodeo.get_or_intern(o);

            let key_s_int = key_s.into_usize();
            let key_p_int = key_p.into_usize();
            let key_o_int = key_o.into_usize();

            tbox_input_stream.insert((key_s_int, key_p_int, key_o_int));
        });
        tbox_input_stream.advance_to(1);
        tbox_input_stream.flush();
        worker.step_while(|| tbox_probe.less_than(tbox_input_stream.time()));

        abox_triples.for_each(|triple| {
            let s = &triple.0[..];
            let p = &triple.1[..];
            let o = &triple.2[..];

            let key_s = rodeo.get_or_intern(s);
            let key_p = rodeo.get_or_intern(p);
            let key_o = rodeo.get_or_intern(o);

            let key_s_int = key_s.into_usize();
            let key_p_int = key_p.into_usize();
            let key_o_int = key_o.into_usize();

            abox_input_stream.insert((key_s_int, key_p_int, key_o_int));
        });

        abox_input_stream.advance_to(1);
        abox_input_stream.flush();
        worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
        worker.step();

        let (mut tbox_cursor, tbox_storage) = tbox_trace.cursor();
        let (mut abox_cursor, abox_storage) = abox_trace.cursor();

        (
            tbox_cursor.to_vec(&tbox_storage),
            abox_cursor.to_vec(&abox_storage),
        )
    });

    for summary in tbox_summaries.drain(..) {
        println!("Tbox entry: {:?}", summary)
    }

    for summary in abox_summaries.drain(..) {
        println!("Abox entry: {:?}", summary)
    }
}

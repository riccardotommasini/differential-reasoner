/* use rdfs_materialization; */

mod utils;

use std::u32;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::CountTotal;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Threshold;
use lasso::{Key, Rodeo, Spur};
use rdfs_materialization::load_encode_triples::{load3enc, load3nt};
use rdfs_materialization::rdfs_materialization::*;
use timely::dataflow::operators::Map;

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

    timely::execute_directly(move |worker| {
        let (mut tbox_input_stream, mut abox_input_stream) = worker.dataflow::<(), _, _>(|outer| {
            let (mut _abox_in, mut abox) = outer.new_collection::<(usize, usize, usize), isize>();
            let (mut _tbox_in, mut tbox) = outer.new_collection::<(usize, usize, usize), isize>();

            // our tbox has very little data so this part really doesnt matter much

            let sco = rule_11(&tbox);
            let spo = rule_5(&tbox);
            let tbox = tbox.concat(&sco).concat(&spo);

            let tbox_by_s = tbox.map(|(s, p, o)| (s, (p, o))).arrange_by_key();
            let sco_assertions = tbox_by_s.filter(|s, (p, o)| p == &0usize);
            let spo_assertions = tbox_by_s.filter(|s, (p, o)| p == &1usize);

            let abox_by_o = abox.map(|(s, p, o)| (o, (s, p)));
            let abox_by_p = abox.map(|(s, p, o)| (p, (s, o)));

            let type_assertions = abox_by_o.filter(|(o, (s, p))| p == &4usize);
            //.inspect(|x| println!("Type assertion: {:?}", x));

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

            let abox = abox.concat(&sco_type).concat(&spo_type).distinct();

            (_tbox_in, _abox_in)
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
        tbox_input_stream.flush();

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
        abox_input_stream.flush();
    });
}

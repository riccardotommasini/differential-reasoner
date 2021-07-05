use crate::load_encode_triples::load3nt;

fn temporary() {

    // let tbox_triples = load3nt(0, "./tests/data/", "tbox.nt");
    // let abox_triples = load3nt(0, "./tests/data/", "lubm50.nt");
    // let mut tbox_encoded = File::create("tbox.ntenc").unwrap();
    // let mut abox_encoded = File::create("lubm50.ntenc").unwrap();

    // let mut rodeo = Rodeo::default();

    // /// URI of the rdfs:subClassOf
    // let rdfsco: &str = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
    // /// URI of the rdfs:subPropertyOf
    // let rdfspo:  &str = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
    // /// URI of the rdfs::domain
    // let rdfsd:  &str = "<http://www.w3.org/2000/01/rdf-schema#domain>";
    // /// URI of the rdfs::range
    // let rdfsr:  &str = "<http://www.w3.org/2000/01/rdf-schema#range>";
    // /// URI of rdf:type
    // let rdft: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
    // /// URI of owl:TransitiveProperty
    // let owltr:  &str = "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
    // /// URI of owl:inverseOf
    // let owlio:  &str = "<http://www.w3.org/2002/07/owl#inverseOf>";

    // rodeo.get_or_intern(rdfsco);
    // println!("{:?}", rodeo.get_or_intern(rdfsco).into_usize());
    // rodeo.get_or_intern(rdfspo);
    // println!("{:?}", rodeo.get_or_intern(rdfspo).into_usize());
    // rodeo.get_or_intern(rdfsd);
    // println!("{:?}", rodeo.get_or_intern(rdfsd).into_usize());
    // rodeo.get_or_intern(rdfsr);
    // println!("{:?}", rodeo.get_or_intern(rdfsr).into_usize());
    // rodeo.get_or_intern(rdft);
    // println!("{:?}", rodeo.get_or_intern(rdft).into_usize());
    // rodeo.get_or_intern(owltr);
    // println!("{:?}", rodeo.get_or_intern(owltr).into_usize());
    // rodeo.get_or_intern(owlio);
    // println!("{:?}", rodeo.get_or_intern(owlio).into_usize());

    // tbox_triples.for_each(|triple| {
    // 	let s = &triple.0[..];
    // 	let p = &triple.1[..];
    // 	let o = &triple.2[..];

    // 	let key_s = rodeo.get_or_intern(s);
    // 	let key_p = rodeo.get_or_intern(p);
    // 	let key_o = rodeo.get_or_intern(o);

    // 	let key_s_int = key_s.into_usize();
    // 	let key_p_int = key_p.into_usize();
    // 	let key_o_int = key_o.into_usize();

    // 	writeln!(&mut tbox_encoded, "{:?} {:?} {:?}", key_s_int, key_p_int, key_o_int);
    // });

    // abox_triples.for_each(|triple| {
    // 	let s = &triple.0[..];
    // 	let p = &triple.1[..];
    // 	let o = &triple.2[..];

    // 	let key_s = rodeo.get_or_intern(s);
    // 	let key_p = rodeo.get_or_intern(p);
    // 	let key_o = rodeo.get_or_intern(o);

    // 	let key_s_int = key_s.into_usize();
    // 	let key_p_int = key_p.into_usize();
    // 	let key_o_int = key_o.into_usize();

    // 	writeln!(&mut abox_encoded, "{:?} {:?} {:?}", key_s_int, key_p_int, key_o_int);
    // });
}

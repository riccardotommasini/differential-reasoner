/* use rdfs_materialization; */

mod utils;

use lasso::Rodeo;
use rdfs_materialization::load_encode_triples::{load3enc, load3nt};

/* use rdfs_materialization::rdfs_materialization::*; */

#[test]
fn loading_triples() {
    let triples = load3enc(0, "./tests/data/", "tiny_abox.ntenc");

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

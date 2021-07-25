use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::Collection;
use timely::dataflow::Scope;

type EncodedTriple = (usize, usize, usize);

pub fn efficient_transitivity<G>(
    data: &Collection<G, EncodedTriple>,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let by_subject_predicate = data.map(|(x, p, y)| ((x, p), y)).arrange_by_key();
    let by_object_predicate = data.map(|(x, p, y)| ((y, p), x)).arrange_by_key();

    by_subject_predicate
        .join_core(&by_object_predicate, |&(_y, p), &z, &x| Some((x, p, z)))
        .concat(&data)
        .distinct()
}

pub fn inverseof_rule<G>(data: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let data_by_p = data.map(|(x, p, y)| (p, (x, y)));
    /// 6 = INVERSE_OF
    let inverse_only = data_by_p.filter(move |(p, (_x, _y))| *p == 6);

    let arranged_inverse_only_by_p0 = inverse_only.map(|(_p, (x, y))| (x, y)).arrange_by_key();

    let arranged_inverse_only_by_p1 = inverse_only.map(|(_p, (x, y))| (y, x)).arrange_by_key();

    let arranged_data_by_p = data_by_p.arrange_by_key();

    let left_inverse_only_by_p0 = arranged_inverse_only_by_p0
        .join_core(&arranged_data_by_p, |&_p0, &p1, &(x, y)| Some((y, p1, x)));

    let right_inverse_only_by_p1 = arranged_inverse_only_by_p1
        .join_core(&arranged_data_by_p, |&_p1, &p0, &(y, x)| Some((x, p0, y)));

    left_inverse_only_by_p0.concat(&right_inverse_only_by_p1)
}

pub fn trans_property_rule<G>(data: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let data_arranged_by_p = data.map(|(x, p, y)| (p, (x, y))).arrange_by_key();
    /// 5 = TRANSITIVE_PROPERTY
    let trans_only_p = data
        .filter(move |(_x, p, y)| *p == 5 && *y == 6)
        .map(|(x, _p, _y)| (x, ()))
        .arrange_by_key();

    let trans_only_triples =
        trans_only_p.join_core(&data_arranged_by_p, |&p, &(), &(x, y)| Some((x, p, y)));

    trans_only_triples.iterate(|inner| efficient_transitivity(inner))
}

/// Eleventh rule: [x, SCO, z] <= [x, SCO, y], [y, SCO, z]
pub fn rule_11<G>(data_collection: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    data_collection
        .filter(move |(s, p, o)| p == &0usize)
        .iterate(|inner| efficient_transitivity(inner))
}

/// Fifth rule: [x, SPO, z] <= [x, SPO, y], [y, SPO, z]
pub fn rule_5<G>(data_collection: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    data_collection
        .filter(move |(s, p, o)| p == &1usize)
        .iterate(|inner| efficient_transitivity(inner))
}

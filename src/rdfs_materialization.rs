use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::Collection;
use timely::dataflow::Scope;

type EncodedTriple = (usize, usize, usize);

pub fn efficient_transitivity<G>(data: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
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
    /// 0 = SCO
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
    /// 1 = SPO
    data_collection
        .filter(move |(s, p, o)| p == &1usize)
        .iterate(|inner| efficient_transitivity(inner))
}

/// Ninth rule: [z, TYPE, y] <= [x, SCO, y], [z, TYPE, x]
pub fn rule_9<G>(data_collection: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let sco_only = data_collection.filter(move |triple| triple.1 == 0);
    /// 4 = TYPE
    let candidates = data_collection
        .filter(move |triple| triple.1 == 4)
        .map(|triple| (triple.2.clone(), (triple)))
        .join(&sco_only.map(|triple| (triple.0, ())))
        .map(|(_key, (triple, ()))| triple);

    candidates.iterate(|inner| {
        let sco_only_in = sco_only.enter(&inner.scope());
        inner
            .map(|triple| (triple.2, (triple.0, triple.1)))
            .join(&sco_only_in.map(|triple| (triple.0, (triple.1, triple.2))))
            .map(|(_key, ((x, typ), (_sco, b)))| (x, typ, b))
            .concat(&inner)
            .distinct()
    })
}

/// Seventh rule: [x, b, y] <= [a, SPO, b], [x, a, y]
pub fn rule_7<G>(data_collection: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let spo_only_out = data_collection.filter(move |triple| triple.1 == 1);

    let candidates = data_collection
        .map(|triple| ((triple.1, triple)))
        .join(&spo_only_out.map(|triple| ((triple.0), ())))
        .map(|(_, (triple, ()))| triple);

    candidates.iterate(|inner| {
        let spo_only = spo_only_out.enter(&inner.scope());
        inner
            .map(|triple| (triple.1, (triple.0, triple.2)))
            .join(&spo_only.map(|triple| (triple.0, (triple.1, triple.2))))
            .map(|(_key, ((x, y), (_spo, p)))| (x, p, y))
            .concat(&inner)
            .distinct()
    })
}

/// Second rule: [y, TYPE, x] <= [a, DOMAIN, x],[y, a, z]
pub fn rule_2<G>(data_collection: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    /// 2 = DOMAIN
    let only_domain = data_collection.filter(move |triple| triple.1 == 2);

    let candidates = data_collection
        .map(|triple| (triple.1, triple))
        .join(&only_domain.map(|triple| (triple.0, ())))
        .map(|(_, (triple, ()))| triple);

    candidates
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .join(&only_domain.map(|triple| (triple.0, (triple.1, triple.2))))
        .map(move |(_key, ((a, _b), (_dom, d)))| (a, 4, d))
}

/// Third rule: [z, TYPE, x) <= [a, RANGE, x),[y, a, z)
pub fn rule_3<G>(data_collection: &Collection<G, EncodedTriple>) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    /// 3 = RANGE
    let only_range = data_collection.filter(move |triple| triple.1 == 3);

    let candidates = data_collection
        .map(|triple| ((triple.1, triple)))
        .join(&only_range.map(|triple| (triple.0, ())))
        .map(|(_, (triple, ()))| triple);

    candidates
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .join(&only_range.map(|triple| (triple.0, (triple.1, triple.2))))
        .map(move |(_key, ((_a, b), (_ran, r)))| (b, 4, r))
}

/// Standard rule application
pub fn materialize<G>(
    input_stream: &Collection<G, (usize, usize, usize)>,
) -> Collection<G, (usize, usize, usize)>
where
    G::Timestamp: Lattice,
    G: Scope,
{
    let sco_transitive_closure = rule_11(&input_stream);

    let spo_transitive_closure = rule_5(&input_stream);

    let input_stream = input_stream
        .concat(&sco_transitive_closure)
        .concat(&spo_transitive_closure);

    let input_stream = trans_property_rule(&input_stream).concat(&input_stream);

    let input_stream = inverseof_rule(&input_stream).concat(&input_stream);

    let spo_type_rule = rule_7(&input_stream);

    let input_stream = input_stream.concat(&spo_type_rule).distinct();

    let input_stream = inverseof_rule(&input_stream).concat(&input_stream);

    let domain_type_rule = rule_2(&input_stream);

    let input_stream = input_stream.concat(&domain_type_rule);

    let range_type_rule = rule_3(&input_stream);

    let input_stream = input_stream.concat(&range_type_rule);

    let sco_type_rule = rule_9(&input_stream);

    let input_stream = input_stream.concat(&sco_type_rule).distinct();

    input_stream
}

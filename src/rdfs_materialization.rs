use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::Collection;
use timely::dataflow::Scope;

type EncodedTriple = (u32, u32, u32);

pub fn inverseof_rule<G>(
    data: &Collection<G, EncodedTriple>,
    inverseof_value: u32,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let data_by_p = data.map(|(x, p, y)| (p, (x, y)));

    let inverse_only = data_by_p.filter(move |(p, (_x, _y))| *p == inverseof_value);

    let arranged_inverse_only_by_p0 = inverse_only.map(|(_p, (x, y))| (x, y)).arrange_by_key();

    let arranged_inverse_only_by_p1 = inverse_only.map(|(_p, (x, y))| (y, x)).arrange_by_key();

    let arranged_data_by_p = data_by_p.arrange_by_key();

    let left_inverse_only_by_p0 = arranged_inverse_only_by_p0
        .join_core(&arranged_data_by_p, |&_p0, &p1, &(x, y)| Some((y, p1, x)));

    let right_inverse_only_by_p1 = arranged_inverse_only_by_p1
        .join_core(&arranged_data_by_p, |&_p1, &p0, &(y, x)| Some((x, p0, y)));

    left_inverse_only_by_p0.concat(&right_inverse_only_by_p1)
}

pub fn trans_property_rule<G>(
    data: &Collection<G, EncodedTriple>,
    type_value: u32,
    transitivity_value: u32,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let data_arranged_by_p = data.map(|(x, p, y)| (p, (x, y))).arrange_by_key();

    let trans_only_p = data
        .filter(move |(_x, p, y)| *p == type_value && *y == transitivity_value)
        .map(|(x, _p, _y)| (x, ()))
        .arrange_by_key();

    let trans_only_triples =
        trans_only_p.join_core(&data_arranged_by_p, |&p, &(), &(x, y)| Some((x, p, y)));

    trans_only_triples.iterate(|inner| {
        let by_subject_predicate = inner.map(|(x, p, y)| ((x, p), y)).arrange_by_key();
        let by_object_predicate = inner.map(|(x, p, y)| ((y, p), x)).arrange_by_key();

        by_subject_predicate
            .join_core(&by_object_predicate, |&(_y, p), &z, &x| Some((x, p, z)))
            .concat(&inner)
            .distinct()
    })
}

pub fn rule_1<G>(
    data_collection: &Collection<G, EncodedTriple>,
    sco_value: u32,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    data_collection
        .filter(move |triple| triple.1 == sco_value)
        .iterate(|inner| {
            inner
                .map(|triple| (triple.2, (triple.0, triple.1)))
                .join(&inner.map(|triple| (triple.0, (triple.1, triple.2))))
                .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))| (subj1, pred1, obj2))
                .concat(&inner)
                .distinct()
        })
}

/// Second rule: T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)
pub fn rule_2<G>(
    data_collection: &Collection<G, EncodedTriple>,
    spo_value: u32,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    data_collection
        .filter(move |triple| triple.1 == spo_value)
        .iterate(|inner| {
            inner
                .map(|triple| (triple.2, (triple.0, triple.1)))
                .join(&inner.map(|triple| (triple.0, (triple.1, triple.2))))
                .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))| (subj1, pred1, obj2))
                .concat(&inner)
                .distinct()
        })
}

/// Third rule: T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)
pub fn rule_3<G>(
    data_collection: &Collection<G, EncodedTriple>,
    type_value: u32,
    sco_value: u32,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let sco_only = data_collection.filter(move |triple| triple.1 == sco_value);

    let candidates = data_collection
        .filter(move |triple| triple.1 == type_value)
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

/// Fourth rule: T(x, p, y) <= T(p1, SPO, p),T(x, p1, y)
pub fn rule_4<G>(
    data_collection: &Collection<G, EncodedTriple>,
    spo_value: u32,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let spo_only_out = data_collection.filter(move |triple| triple.1 == spo_value);

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

/// Fifth rule: T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)
pub fn rule_5<G>(
    data_collection: &Collection<G, EncodedTriple>,
    domain_value: u32,
    type_value: u32,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let only_domain = data_collection.filter(move |triple| triple.1 == domain_value);

    let candidates = data_collection
        .map(|triple| (triple.1, triple))
        .join(&only_domain.map(|triple| (triple.0, ())))
        .map(|(_, (triple, ()))| triple);

    candidates
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .join(&only_domain.map(|triple| (triple.0, (triple.1, triple.2))))
        .map(move |(_key, ((a, _b), (_dom, d)))| (a, type_value, d))
}

/// Sixth rule: T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)
pub fn rule_6<G, V>(
    data_collection: &Collection<G, EncodedTriple>,
    range_value: u32,
    type_value: u32,
) -> Collection<G, EncodedTriple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let only_range = data_collection.filter(move |triple| triple.1 == range_value);

    let candidates = data_collection
        .map(|triple| ((triple.1, triple)))
        .join(&only_range.map(|triple| (triple.0, ())))
        .map(|(_, (triple, ()))| triple);

    candidates
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .join(&only_range.map(|triple| (triple.0, (triple.1, triple.2))))
        .map(move |(_key, ((_a, b), (_ran, r)))| (b, type_value, r))
}

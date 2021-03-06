use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::iterate;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::order::Product;

type EncodedTriple = (usize, usize, usize);
type EncodedTripleByS = (usize, (usize, usize));

fn tbox_spo_sco_materialization<G>(
    tbox: &Collection<G, EncodedTriple>,
    outer: &mut G,
) -> Collection<G, EncodedTripleByS>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    outer.region_named("Tbox transitive rules", |inn| {
        let tbox = tbox.enter(inn);

        let tbox_by_o = tbox.map(|(s, p, o)| (o, (p, s)));

        let tbox_by_s = tbox.map(|(s, p, o)| (s, (p, o)));

        let sco_ass_by_o = tbox_by_o.filter(|(_, (p, _))| p == &0usize);

        let sco_ass_by_s = sco_ass_by_o.map(|(o, (p, s))| (s, (p, o)));

        let spo_ass_by_o = tbox_by_o.filter(|(_, (p, _))| p == &1usize);

        let spo_ass_by_s = spo_ass_by_o.map(|(o, (p, s))| (s, (p, o)));

        let (spo, sco) = inn.iterative::<usize, _, _>(|inner| {
            let sco_var =
                iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));
            let spo_var =
                iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));

            let sco_new = sco_var.distinct();

            let spo_new = spo_var.distinct();

            let sco_new_arr = sco_new.arrange_by_key();
            let spo_new_arr = spo_new.arrange_by_key();

            let sco_ass_by_s = sco_ass_by_s.enter(inner);
            let spo_ass_by_s = spo_ass_by_s.enter(inner);

            let sco_ass_by_o = sco_ass_by_o.enter(inner);
            let spo_ass_by_o = spo_ass_by_o.enter(inner);

            let sco_ass_by_o_arr = sco_ass_by_o.arrange_by_key();
            let spo_ass_by_o_arr = spo_ass_by_o.arrange_by_key();

            let sco_iter_step = sco_ass_by_o_arr
                .join_core(&sco_new_arr, |&_, &(p, s), &(_, o_prime)| {
                    Some((s, (p, o_prime)))
                });

            let spo_iter_step = spo_ass_by_o_arr
                .join_core(&spo_new_arr, |&_, &(p, s), &(_, o_prime)| {
                    Some((s, (p, o_prime)))
                });

            sco_var.set(&sco_ass_by_s.concat(&sco_iter_step));
            spo_var.set(&spo_ass_by_s.concat(&spo_iter_step));

            (sco_new.leave(), spo_new.leave())
        });

        tbox_by_s.concat(&sco).concat(&spo).leave()
    })
}

fn abox_sco_type_materialization<G>(
    tbox_sco_assertions: &Collection<G, EncodedTripleByS>,
    abox_class_assertions: &Collection<G, EncodedTripleByS>,
    outer: &mut G,
) -> Collection<G, EncodedTripleByS>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    outer.region_named("CAX-SCO", |inn| {
        let sco_assertions = tbox_sco_assertions.enter(inn);
        let class_assertions = abox_class_assertions.enter(inn);

        let class_assertions_arranged = class_assertions.arrange_by_key();

        sco_assertions
            .join_core(
                &class_assertions_arranged,
                |_key, &(_sco, y), &(z, type_)| Some((y, (z, type_))),
            )
            .leave()
    })
}

fn abox_domain_and_range_type_materialization<G>(
    tbox_domain_assertions: &Collection<G, EncodedTripleByS>,
    tbox_range_assertions: &Collection<G, EncodedTripleByS>,
    abox_property_assertions: &Collection<G, EncodedTripleByS>,
    outer: &mut G,
) -> (
    Collection<G, EncodedTripleByS>,
    Collection<G, EncodedTripleByS>,
)
where
    G: Scope,
    G::Timestamp: Lattice,
{
    outer.region_named("Domain and Range type rules", |inner| {
        let property_assertions = abox_property_assertions.enter(inner);

        let p_s_arr = property_assertions
            .map(|(p, (s, _o))| (p, s))
            .distinct()
            .arrange_by_key_named("Arrange (p, s) for PRP-DOM");

        let p_o_arr = property_assertions
            .map(|(p, (_s, o))| (p, o))
            .distinct()
            .arrange_by_key_named("Arrange (p, o) for PRP-RNG");

        let domain_assertions = tbox_domain_assertions.enter(inner);

        let domain_type =
            domain_assertions.join_core(&p_s_arr, |_p, &(_, x), &y| Some((x, (y, 4usize))));

        let range_assertions = tbox_range_assertions.enter(inner);

        let range_type =
            range_assertions.join_core(&p_o_arr, |_p, &(_, x), &z| Some((x, (z, 4usize))));

        (domain_type.leave(), range_type.leave())
    })
}

pub fn rdfs<G>(
    tbox: &Collection<G, EncodedTriple>,
    abox: &Collection<G, EncodedTriple>,
    outer: &mut G,
) -> (Collection<G, EncodedTriple>, Collection<G, EncodedTriple>)
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let tbox = tbox_spo_sco_materialization(tbox, outer);

    let sco_assertions = tbox.filter(|(_s, (p, _o))| p == &0usize);
    let spo_assertions = tbox.filter(|(_s, (p, _o))| p == &1usize);
    let domain_assertions = tbox.filter(|(_s, (p, _o))| p == &2usize);
    let range_assertions = tbox.filter(|(_s, (p, _o))| p == &3usize);

    let class_assertions = abox
        .map(|(s, p, o)| (o, (s, p)))
        .filter(|(_o, (_s, p))| p == &4usize);
    let property_assertions = abox
        .map(|(s, p, o)| (p, (s, o)))
        .filter(|(p, (_s, _o))| p != &4usize);

    let property_materialization = outer.region_named("Abox transitive property rules", |inn| {
        let property_assertions_arr = property_assertions
            .enter(inn)
            .arrange_by_key_named("Arrange property assertions for Abox PRP-SPO1");

        spo_assertions
            .enter(inn)
            .join_core(&property_assertions_arr, |_key, &(_spo, b), &(x, y)| {
                Some((b, (x, y)))
            })
            .distinct()
            .leave()
    });

    let property_assertions = property_materialization.concat(&property_assertions);

    let (domain_type, range_type) = abox_domain_and_range_type_materialization(
        &domain_assertions,
        &range_assertions,
        &property_assertions,
        outer,
    );

    let class_assertions = class_assertions
        .concatenate(vec![domain_type, range_type])
        .consolidate();

    let class_materialization =
        abox_sco_type_materialization(&sco_assertions, &class_assertions, outer);

    let class_assertions = class_assertions.concat(&class_materialization);

    let abox = outer.region_named("Concatenating all rules", |inner| {
        let abox = abox.enter(inner);

        let property_assertions = property_assertions
            .enter(inner)
            .map(|(p, (x, y))| (x, p, y));

        let class_assertions = class_assertions.enter(inner).map(|(y, (x, p))| (x, p, y));

        abox.concat(&property_assertions)
            .concat(&class_assertions)
            .consolidate()
            .leave()
    });

    let tbox = tbox.map(|(s, (p, o))| (s, p, o));

    (tbox, abox)
}

pub fn rdfspp<G>(
    tbox: &Collection<G, EncodedTriple>,
    abox: &Collection<G, EncodedTriple>,
    outer: &mut G,
) -> (Collection<G, EncodedTriple>, Collection<G, EncodedTriple>)
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let tbox = tbox_spo_sco_materialization(tbox, outer);

    let sco_assertions = tbox.filter(|(_s, (p, _o))| p == &0usize);
    let spo_assertions = tbox.filter(|(_s, (p, _o))| p == &1usize);
    let domain_assertions = tbox.filter(|(_s, (p, _o))| p == &2usize);
    let range_assertions = tbox.filter(|(_s, (p, _o))| p == &3usize);
    let general_trans_assertions = tbox.filter(|(_s, (_p, o))| o == &5usize);
    let inverse_of_assertions = tbox.filter(|(_s, (p, _o))| p == &6usize);

    let class_assertions = abox
        .map(|(s, p, o)| (o, (s, p)))
        .filter(|(_o, (_s, p))| p == &4usize);

    let property_assertions = abox
        .map(|(s, p, o)| (p, (s, o)))
        .filter(|(p, (_s, _o))| p != &4usize);

    let property_materialization = outer.region_named("Abox transitive property rules", |inn| {
        let property_assertions = property_assertions.enter(inn);
        let spo_assertions = spo_assertions.enter(inn);
        let general_trans_assertions = general_trans_assertions.enter(inn);
        let inverse_of_assertions = inverse_of_assertions.enter(inn);

        let property_materialization = inn.iterative::<usize, _, _>(|inner| {
            let spo_type_gen_trans_inv_var =
                iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));

            let spo_type_gen_trans_inv_new = spo_type_gen_trans_inv_var.distinct();

            let spo_type_gen_trans_inv_arr = spo_type_gen_trans_inv_new.arrange_by_key();

            let spo_ass = spo_assertions.enter(inner);
            let gen_trans_ass = general_trans_assertions.enter(inner);
            let inverse_ass = inverse_of_assertions.enter(inner);

            let left_inverse_ass = inverse_ass.clone();
            let right_inverse_ass = inverse_ass.map(|(s, (p, o))| (o, (p, s))).arrange_by_key();

            let spo_iter_step = spo_ass
                .join_core(&spo_type_gen_trans_inv_arr, |_key, &(_spo, b), &(x, y)| {
                    Some((b, (x, y)))
                });

            let left_inverse_only_iter_step = left_inverse_ass
                .join_core(&spo_type_gen_trans_inv_arr, |&_, &(_, p1), &(s, o)| {
                    Some((p1, (o, s)))
                });

            let right_inverse_only_iter_step = right_inverse_ass
                .join_core(&spo_type_gen_trans_inv_arr, |&_, &(_, p0), &(o, s)| {
                    Some((p0, (s, o)))
                });

            let trans_p_only = gen_trans_ass.join_core(
                &spo_type_gen_trans_inv_arr,
                |&p, &(_type_kw, _trans_kw), &(s, o)| Some(((s, p), o)),
            );

            let trans_p_only_reverse = trans_p_only.map(|((s, p), o)| ((o, p), s)).arrange_by_key();

            let trans_p_only_arr = trans_p_only.arrange_by_key();

            let gen_trans_iter_step = trans_p_only_reverse
                .join_core(&trans_p_only_arr, |&(_o, p), &s, &o_prime| {
                    Some((p, (s, o_prime)))
                });

            spo_type_gen_trans_inv_var.set(&property_assertions.enter(inner).concatenate(vec![
                spo_iter_step,
                gen_trans_iter_step,
                left_inverse_only_iter_step,
                right_inverse_only_iter_step,
            ]));

            spo_type_gen_trans_inv_new.leave()
        });

        property_materialization.leave()
    });

    let property_assertions = property_materialization.concat(&property_assertions);

    let (domain_type, range_type) = abox_domain_and_range_type_materialization(
        &domain_assertions,
        &range_assertions,
        &property_assertions,
        outer,
    );

    let class_assertions = class_assertions
        .concatenate(vec![domain_type, range_type])
        .consolidate();

    let class_materialization =
        abox_sco_type_materialization(&sco_assertions, &class_assertions, outer);

    let class_assertions = class_assertions.concat(&class_materialization);

    let abox = outer.region_named("Concatenating all rules", |inner| {
        let abox = abox.enter(inner);

        let property_assertions = property_assertions
            .enter(inner)
            .map(|(p, (x, y))| (x, p, y));

        let class_assertions = class_assertions.enter(inner).map(|(y, (x, p))| (x, p, y));

        abox.concat(&property_assertions)
            .concat(&class_assertions)
            .consolidate()
            .leave()
    });

    (tbox.map(|(s, (p, o))| (s, p, o)), abox)
}

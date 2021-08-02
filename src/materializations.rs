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

pub fn rdfspp<G>(
    tbox: &Collection<G, EncodedTriple>,
    abox: &Collection<G, EncodedTriple>,
    outer: &mut G,
) -> (Collection<G, EncodedTriple>, Collection<G, EncodedTriple>)
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let tbox = outer.region_named("Tbox transitive rules", |inn| {
        let tbox = tbox.enter(inn);

        let tbox_by_o = tbox.map(|(s, p, o)| (o, (p, s)));

        let tbox_by_s = tbox.map(|(s, p, o)| (s, (p, o)));

        let sco_ass_by_o = tbox_by_o.filter(|(_, (p, _))| p == &0usize);

        let sco_ass_by_s = sco_ass_by_o.map(|(o, (p, s))| (s, (p, o)));

        let spo_ass_by_o = tbox_by_o.filter(|(_, (p, _))| p == &1usize);

        let spo_ass_by_s = spo_ass_by_o.map(|(o, (p, s))| (s, (p, o)));

        let (spo, sco) = inn.iterative::<usize, _, _>(|inner| {
            let spo_var =
                iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));
            let sco_var =
                iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));

            let spo_new = sco_var.distinct();
            let sco_new = sco_var.distinct();

            let sco_new_arr = sco_new.arrange_by_key();
            let spo_new_arr = spo_new.arrange_by_key();

            let sco_ass_by_s = sco_ass_by_s.enter(inner);
            let spo_ass_by_s = spo_ass_by_s.enter(inner);

            let sco_ass_by_o = sco_ass_by_o.enter(inner);
            let spo_ass_by_o = spo_ass_by_o.enter(inner);

            let sco_ass_by_o_arr = sco_ass_by_o.arrange_by_key();
            let spo_ass_by_o_arr = spo_ass_by_o.arrange_by_key();

            let sco_iter_step = sco_ass_by_o_arr
                .join_core(&sco_new_arr, |&_, &(_, s), &(p, o_prime)| {
                    Some((s, (p, o_prime)))
                });

            let spo_iter_step = spo_ass_by_o_arr
                .join_core(&spo_new_arr, |&_, &(_, s), &(p, o_prime)| {
                    Some((s, (p, o_prime)))
                });

            sco_var.set(&sco_ass_by_s.concat(&sco_iter_step));
            spo_var.set(&spo_ass_by_s.concat(&spo_iter_step));

            (sco_new.leave(), spo_new.leave())
        });

        tbox_by_s.concat(&sco).concat(&spo).consolidate().leave()
    });

    let tbox_by_s = tbox.arrange_by_key();
    let sco_assertions = tbox_by_s.filter(|s, (p, o)| p == &0usize);
    let spo_assertions = tbox_by_s.filter(|s, (p, o)| p == &1usize);
    let domain_assertions = tbox_by_s.filter(|s, (p, o)| p == &2usize);
    let range_assertions = tbox_by_s.filter(|s, (p, o)| p == &3usize);
    let general_trans_assertions = tbox_by_s.filter(|s, (p, o)| o == &5usize);
    let inverse_of_assertions = tbox_by_s.filter(|s, (p, o)| p == &6usize);

    let type_assertions = abox
        .map(|(s, p, o)| (o, (s, p)))
        .filter(|(o, (s, p))| p == &4usize);

    let not_type_assertions_by_p = abox
        .map(|(s, p, o)| (p, (s, o)))
        .filter(|(p, (s, o))| p != &4usize);

    let (sco_type, spo_type) = outer.region_named("Abox transitive Rules", |inn| {
        let type_assertions = type_assertions.enter(inn);
        let not_type_assertions_by_p = not_type_assertions_by_p.enter(inn);
        let sco_assertions = sco_assertions.enter(inn);
        let spo_assertions = spo_assertions.enter(inn);
        let general_trans_assertions = general_trans_assertions.enter(inn);

        let (sco_type, spo_type) = inn.iterative::<usize, _, _>(|inner| {
            let sco_var =
                iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));
            let spo_gen_trans_var =
                iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));

            let sco_new = sco_var.distinct();
            let spo_gen_trans_new = spo_gen_trans_var.distinct();

            let sco_arr = sco_new.arrange_by_key();
            let spo_gen_trans_arr = spo_gen_trans_new.arrange_by_key();

            let sco_ass = sco_assertions.enter(inner);
            let spo_ass = spo_assertions.enter(inner);
            let gen_trans_ass = general_trans_assertions.enter(inner);

            let sco_iter_step = sco_ass.join_core(&sco_arr, |_key, &(_sco, y), &(z, type_)| {
                Some((y, (z, type_)))
            });

            let spo_iter_step = spo_ass
                .join_core(&spo_gen_trans_arr, |_key, &(_spo, b), &(x, y)| {
                    Some((b, (x, y)))
                });

            let trans_only = gen_trans_ass
                .join_core(&spo_gen_trans_arr, |&p, &(_type_kw, _trans_kw), &(s, o)| {
                    Some(((s, p), o))
                });

            let trans_only_reverse = trans_only.map(|((s, p), o)| ((o, p), s)).arrange_by_key();

            let trans_only_arr = trans_only.arrange_by_key();

            let gen_trans_iter_step = trans_only_reverse
                .join_core(&trans_only_arr, |&(_o, p), &s, &o_prime| {
                    Some((p, (s, o_prime)))
                });

            sco_var.set(&type_assertions.enter(inner).concat(&sco_iter_step));

            spo_gen_trans_var.set(
                &not_type_assertions_by_p
                    .enter(inner)
                    .concatenate(vec![spo_iter_step, gen_trans_iter_step]),
            );

            (sco_new.leave(), spo_gen_trans_new.leave())
        });

        (sco_type.leave(), spo_type.leave())
    });

    let not_type_assertions_by_p = spo_type
        //.inspect(|x| println!("SPO and Gen Trans materialization{:?}", x))
        .concat(&not_type_assertions_by_p)
        .consolidate();

    let not_type_assertions_by_p_arr = not_type_assertions_by_p.arrange_by_key();

    let (domain_type, range_type) = outer.region_named("Domain and Range type rules", |inner| {
        let not_type_assertions_by_p_arr = not_type_assertions_by_p_arr.enter(inner);

        let domain_assertions = domain_assertions.enter(inner);

        let domain_type = domain_assertions.join_core(
            &not_type_assertions_by_p_arr,
            |_a, &(_domain, x), &(y, _z)| Some((4usize, (y, x))),
        );

        let range_assertions = range_assertions.enter(inner);

        let range_type = range_assertions.join_core(
            &not_type_assertions_by_p_arr,
            |_a, &(_range, x), &(_y, z)| Some((4usize, (z, x))),
        );

        (domain_type.leave(), range_type.leave())
    });

    let inverse_of_materialization = outer.region_named("Inverse of", |inner| {
        let arranged_data_by_p = not_type_assertions_by_p_arr.enter(inner);

        let inverse_only = inverse_of_assertions.enter(inner);
        let inverse_only_by_s = inverse_only.clone();
        let inverse_only_by_o = inverse_only
            .as_collection(|s, (p, o)| (*o, (*p, *s)))
            .arrange_by_key();

        let left_inverse_only_by_s = inverse_only_by_s
            .join_core(&arranged_data_by_p, |&_p0, &(_inverseof, p1), &(s, o)| {
                Some((o, p1, s))
            });

        let right_inverse_only_by_s = inverse_only_by_o
            .join_core(&arranged_data_by_p, |&_p1, &(_inverseof, p0), &(o, s)| {
                Some((s, p0, o))
            });

        left_inverse_only_by_s
            .concat(&right_inverse_only_by_s)
            .leave()
    });

    let abox = outer.region_named("Concatenating all rules", |inner| {
        let type_assertions = type_assertions.enter(inner);
        let sco_type = sco_type.enter(inner);
        let not_type_assertions_by_p = not_type_assertions_by_p.enter(inner);
        let domain_type = domain_type.enter(inner);
        let range_type = range_type.enter(inner);
        let abox = abox.enter(inner);
        let inverse_of = inverse_of_materialization.enter(inner);

        let not_type_assertions_by_p = not_type_assertions_by_p
            .concat(&domain_type)
            .concat(&range_type)
            .map(|(p, (x, y))| (x, p, y))
            .concat(&inverse_of);

        let type_assertions = type_assertions
            .concat(&sco_type)
            .map(|(y, (z, type_))| (z, type_, y));

        not_type_assertions_by_p
            .concat(&type_assertions)
            .concat(&abox)
            .consolidate()
            .leave()
    });

    let tbox = tbox.map(|(s, (p, o))| (s, p, o));

    (tbox, abox)
}

mod utils;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Threshold;

use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::iterate;
use differential_dataflow::trace::cursor::CursorDebug;
use differential_dataflow::trace::TraceReader;
use rdfs_materialization::load_encode_triples::load3enc;
use rdfs_materialization::rdfs_materialization::*;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::Scope;
use timely::order::Product;

#[test]
fn loading_inserting_triples() {
    let tbox_triples = load3enc("./encoded_data/test/tbox.ntenc");
    let abox_triples = load3enc("./encoded_data/test/abox.ntenc");

    let (tbox_summaries, abox_summaries) = timely::execute_directly(move |worker| {
        let mut tbox_probe = Handle::new();
        let mut abox_probe = Handle::new();

        let (mut tbox_input_stream, mut abox_input_stream, mut tbox_trace, mut abox_trace) = worker
            .dataflow::<usize, _, _>(|outer| {
                let (mut _abox_in, mut abox) =
                    outer.new_collection::<(usize, usize, usize), isize>();
                let (mut _tbox_in, mut tbox) =
                    outer.new_collection::<(usize, usize, usize), isize>();

                tbox = outer.region_named("Tbox transitive rules", |inner| {
                    let tbox = tbox.enter(inner);
                    let sco = rule_11(&tbox);
                    let spo = rule_5(&tbox);

                    tbox.concat(&sco).concat(&spo).consolidate().leave()
                });

                let tbox_by_s = tbox.map(|(s, p, o)| (s, (p, o))).arrange_by_key();

                let tbox_trace = tbox_by_s.trace.clone();

                tbox_by_s
                    .as_collection(|s, (p, o)| (*s, *p, *o))
                    .consolidate()
                    .probe_with(&mut tbox_probe);

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
                        let sco_var = iterate::SemigroupVariable::new(
                            inner,
                            Product::new(Default::default(), 1),
                        );
                        let spo_gen_trans_var = iterate::SemigroupVariable::new(
                            inner,
                            Product::new(Default::default(), 1),
                        );

                        let sco_new = sco_var.distinct();
                        let spo_gen_trans_new = spo_gen_trans_var.distinct();

                        let sco_arr = sco_new.arrange_by_key();
                        let spo_gen_trans_arr = spo_gen_trans_new.arrange_by_key();

                        let sco_ass = sco_assertions.enter(inner);
                        let spo_ass = spo_assertions.enter(inner);
                        let gen_trans_ass = general_trans_assertions.enter(inner);

                        let sco_iter_step = sco_ass
                            .join_core(&sco_arr, |_key, &(sco, y), &(z, type_)| {
                                Some((y, (z, type_)))
                            });

                        let spo_iter_step = spo_ass
                            .join_core(&spo_gen_trans_arr, |_key, &(spo, b), &(x, y)| {
                                Some((b, (x, y)))
                            });

                        let trans_only = gen_trans_ass.join_core(
                            &spo_gen_trans_arr,
                            |&p, &(_type_kw, _trans_kw), &(s, o)| Some(((s, p), o)),
                        );

                        let trans_only_reverse =
                            trans_only.map(|((s, p), o)| ((o, p), s)).arrange_by_key();

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

                let (domain_type, range_type) =
                    outer.region_named("Domain and Range type rules", |inner| {
                        let not_type_assertions_by_p_arr =
                            not_type_assertions_by_p_arr.enter(inner);

                        let domain_assertions = domain_assertions.enter(inner);

                        let domain_type = domain_assertions.join_core(
                            &not_type_assertions_by_p_arr,
                            |a, &(domain, x), &(y, z)| Some((4usize, (y, x))),
                        );

                        let range_assertions = range_assertions.enter(inner);

                        let range_type = range_assertions
                            .join_core(&not_type_assertions_by_p_arr, |a, &(range, x), &(y, z)| {
                                Some((4usize, (z, x)))
                            });

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

                abox = outer.region_named("Concatenating all rules", |inner| {
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
                        .concat(&inverse_of)
                        .consolidate();

                    let type_assertions = type_assertions
                        .concat(&sco_type)
                        .map(|(y, (z, type_))| (z, type_, y))
                        .consolidate();

                    not_type_assertions_by_p
                        .concat(&type_assertions)
                        .concat(&abox)
                        .consolidate()
                        .leave()
                });

                let abox_by_s = abox.arrange_by_self();

                let abox_trace = abox_by_s.trace.clone();

                abox_by_s
                    .as_collection(|_, v| *v)
                    .consolidate()
                    .probe_with(&mut abox_probe);

                (_tbox_in, _abox_in, tbox_trace, abox_trace)
            });

        tbox_triples.for_each(|triple| {
            tbox_input_stream.insert((triple.0, triple.1, triple.2));
        });

        tbox_input_stream.advance_to(1);
        tbox_input_stream.flush();
        worker.step_while(|| tbox_probe.less_than(tbox_input_stream.time()));

        abox_triples.for_each(|triple| {
            abox_input_stream.insert((triple.0, triple.1, triple.2));
        });
        abox_input_stream.advance_to(1);
        abox_input_stream.flush();
        worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));

        let (mut tbox_cursor, tbox_storage) = tbox_trace.cursor();
        let (mut abox_cursor, abox_storage) = abox_trace.cursor();

        (
            tbox_cursor.to_vec(&tbox_storage),
            abox_cursor.to_vec(&abox_storage),
        )
    });

    let tbox_size = tbox_summaries.len();

    let abox_size = abox_summaries.len();

    // for summary in abox_summaries.drain(..) {
    // println!("Abox entry: {:?}", summary)
    // };

    // for summary in tbox_summaries.drain(..) {

    // println!("Tbox entry: {:?}", summary)

    // }

    // This is the amount of materialized tuples that RDFox got with the same set of rules
    assert_eq!(tbox_size + abox_size, 57);
}

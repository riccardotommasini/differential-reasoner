use std::env;
use std::net::TcpStream;

use differential_dataflow::input::Input;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Threshold;
use differential_dataflow::operators::iterate;
use rdfs_materialization::load_encode_triples::load3enc;
use rdfs_materialization::rdfs_materialization::*;
use timely::dataflow::Scope;
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::probe::Handle;
use timely::order::Product;

fn main() {

    let outer_timer = ::std::time::Instant::now();
    timely::execute_from_args(std::env::args(), move |worker| {

	// if let Ok(addr) = std::env::var("DIFFERENTIAL_LOG_ADDR") {
	//     if !addr.is_empty() {
	// 	if let Ok(stream) = std::net::TcpStream::connect(&addr) {
	// 	    differential_dataflow::logging::enable(worker, stream);
	// 	} else {
	// 	    panic!("Could not connect to differential log address: {:?}", addr);
	// 	}
	//     }
	// }
	
	let timer = worker.timer();
	let index = worker.index();
	let peers = worker.peers();

	let part1_data = String::from("part1-lubm50_split");
	let part2_data = String::from("part2-lubm50_split");
	let part3_data = String::from("part3-lubm50_split");
	let index_as_string = index.to_string();
	let suffix = String::from(".ntenc");
	
	let first90 = [part1_data, index_as_string.clone(), suffix.clone()].join("");
	let first90_triples = load3enc("./tests/data/update_data_four/", &first90);
	// let next9 = [part2_data, index_as_string.clone(), suffix.clone()].join("");
	// let next9_triples = load3enc("./tests/data/update_data/", &next9);
	// let next1 =  [part3_data, index_as_string.clone(), suffix.clone()].join("");
	// let next1_triples = load3enc("./tests/data/update_data/", &next1);
	let tbox_triples = load3enc("./tests/data/", "tbox.ntenc");

	let tbox_probe = Handle::new();
	let abox_probe = Handle::new();

        let (mut tbox_input_stream, mut abox_input_stream) = worker.dataflow::<usize, _, _>(|outer| {

	    // Here we are creating the actual stream inputs
            let (mut _abox_in, mut abox) = outer
		.new_collection::<(usize, usize, usize), isize>();
            let (mut _tbox_in, mut tbox) = outer
		.new_collection::<(usize, usize, usize), isize>();

	    tbox = outer.region_named("Tbox materialization", |inner| {
		let tbox = tbox.enter(inner);
		let sco = rule_11(&tbox);
		let spo = rule_5(&tbox);

		tbox
		    .concat(&sco)
		    .concat(&spo)
		    .consolidate()
		    .leave()
	    });

	    let tbox_by_s = tbox
		.map(|(s, p, o)|(s, (p, o)))
		.arrange_by_key();

	    let sco_assertions = tbox_by_s
		.filter(|s, (p, o)| p == &0usize);
	    let spo_assertions = tbox_by_s
		.filter(|s, (p, o)| p == &1usize);
	    let domain_assertions = tbox_by_s
		.filter(|s, (p, o)| p == &2usize);
	    let range_assertions = tbox_by_s
		.filter(|s, (p, o)| p == &3usize);
	    let general_trans_assertions = tbox_by_s
		.filter(|s, (p, o)| o == &5usize);
	    let inverse_of_assertions = tbox_by_s
		.filter(|s, (p, o)| p == &6usize);

	    let (type_assertions, not_type_assertions_by_p) = outer.region_named("Abox splitting", |inner| {

		let abox = abox.enter(inner);

		let type_assertions = abox
		    .map(|(s, p, o)|(o, (s, p)))
		    .filter(|(o, (s, p))| p == &4usize);
	    
		let not_type_assertions_by_p = abox
		    .map(|(s, p, o)|(p, (s, o)))
		    .filter(|(p, (s, o))| p != &4usize);

		(type_assertions.leave(), not_type_assertions_by_p.leave())

	    });

	    let (sco_type, spo_type) = outer.region_named("SCO and SPO Abox iteration", |inn| {

		let type_assertions = type_assertions.enter(inn);
		let not_type_assertions_by_p = not_type_assertions_by_p.enter(inn);
		let sco_assertions = sco_assertions.enter(inn);
		let spo_assertions = spo_assertions.enter(inn);
		
		let (sco_type, spo_type) = inn.iterative::<usize,_,_>(|inner| {
		    
		    let sco_var = iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));
		    let spo_var = iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));

		    let sco_new = sco_var
			.distinct();
		    let spo_new = spo_var
			.distinct();

		    let sco_arr = sco_new
			.arrange_by_key();
		    let spo_arr = spo_new
			.arrange_by_key();

		    let sco_ass = sco_assertions
			.enter(inner);
		    let spo_ass = spo_assertions
			.enter(inner);

		    let sco_iter_step = sco_ass
			.join_core(&sco_arr, |key, &(sco, y), &(z, type_)| Some((y, (z, type_))));

		    let spo_iter_step = spo_ass
			.join_core(&spo_arr, |key, &(spo, b), &(x, y)| Some((b, (x, y))));
		    
		    sco_var.set(&type_assertions.enter(inner).concat(&sco_iter_step));
		    spo_var.set(&not_type_assertions_by_p.enter(inner).concat(&spo_iter_step));

		    (sco_new.leave(), spo_new.leave())
			
		});
		
		(sco_type.leave(), spo_type.leave())
	    });
	    let not_type_assertions_by_p = spo_type
		.concat(&not_type_assertions_by_p)
		.consolidate();

	    let not_type_assertions_by_p_arr = not_type_assertions_by_p
		.arrange_by_key();

	    let (domain_type, range_type) = outer.region_named("Domain and Range type rules", |inner| {

		let domain_assertions = domain_assertions.enter(inner);

		let not_type_assertions_by_p_arr = not_type_assertions_by_p_arr.enter(inner);
		
		let domain_type = domain_assertions
		    .join_core(&not_type_assertions_by_p_arr, |a, &(domain, x), &(y, z)| Some((y, 4usize, x)));

		let range_assertions = range_assertions.enter(inner);
		
		let range_type = range_assertions
		    .join_core(&not_type_assertions_by_p_arr, |a, &(range, x), &(y, z)| Some((z, 4usize, x)));

		(domain_type.leave(), range_type.leave())

	    });

	    abox = outer.region_named("Concatenating all rules", |inner| {
		let type_assertions = type_assertions.enter(inner);
		let sco_type = sco_type.enter(inner);
		let not_type_assertions_by_p = not_type_assertions_by_p.enter(inner);
		let domain_type = domain_type.enter(inner);
		let range_type = range_type.enter(inner);

		type_assertions
		    .concat(&sco_type)
		    .concat(&not_type_assertions_by_p)
		    .map(|(y, (z, type_))| (z, type_, y))
		    .concat(&domain_type)
		    .concat(&range_type)
		    .consolidate()
		    .leave()

	    });

	    let not_type_assertions_by_p_arr = abox
		.map(|(s, p, o)|(p, (s, o)))
		.filter(|(p, (s, o))| p != &4usize)
		.arrange_by_key();

	    let trans_p_only = general_trans_assertions
		.join_core(&not_type_assertions_by_p_arr, |&p, &(_type_kw, _trans_kw), &(s, o)| Some(((s, p), o)));

	    let trans_materialization = outer.region_named("General Transitivity", |inn| {

		let trans_p_only = trans_p_only.enter(inn);

		let trans_materialization = inn.iterative::<usize,_,_>(|inner| {

		    let trans_var = iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));

		    let trans_new = trans_var
			.distinct();

		    let trans_p_only = trans_p_only
			.enter(inner);

		    let trans_p_only_arr = trans_p_only
			.arrange_by_key();

		    let trans_p_only_reverse = trans_p_only
			.map(|((s, p), o)| ((o, p), s))
			.arrange_by_key();

		    let trans_iter_step = trans_p_only_reverse
			.join_core(&trans_p_only_arr, |&(o, p), &s, &o_prime| Some(((s, p), o_prime)));

		    trans_var.set(&trans_p_only.concat(&trans_iter_step));

		    trans_new.leave()
			
		});

		trans_materialization.leave()
		    
	    });

	    abox = trans_materialization
		.map(|((s, p), o)|(s, p, o))
		.concat(&abox)
		.consolidate();

	    let data_by_p = abox.map(|(s, p, o)| (p, (s, o)));
	    
	    let inverse_of_materialization = outer.region_named("Inverse of", |inner| {
		let data_by_p = data_by_p.enter(inner);
		let arranged_data_by_p = data_by_p.arrange_by_key();
		
		let inverse_only = inverse_of_assertions.enter(inner);
		let inverse_only_by_s = inverse_only.clone();
		let inverse_only_by_o = inverse_only
		    .as_collection(|s, (p, o)|(*o, (*p, *s)))
		    .arrange_by_key();
		
		let left_inverse_only_by_s = inverse_only_by_s
		    .join_core(&arranged_data_by_p, |&p0, &(inverseof, p1), &(s, o)| Some((o, p1, s)));

		let right_inverse_only_by_s = inverse_only_by_o
		    .join_core(&arranged_data_by_p, |&p1, &(inverseof, p0), &(s, o)| Some((o, p0, s)));

		left_inverse_only_by_s.concat(&right_inverse_only_by_s).leave()
		
	    });

	    abox = inverse_of_materialization
		.concat(&abox)
		.consolidate();
	    
            (_tbox_in, _abox_in)
        });

	// Tbox
	if 0 == worker.index() {
	    tbox_triples.
		for_each(|triple| {
		    tbox_input_stream.insert((triple.0, triple.1, triple.2)); });
	}
        tbox_input_stream.advance_to(1); tbox_input_stream.flush();
	worker.step_while(|| tbox_probe.less_than(tbox_input_stream.time()));

	first90_triples
	    .for_each(|triple| {
		abox_input_stream.insert((triple.0, triple.1, triple.2));
        });
        abox_input_stream.advance_to(1); abox_input_stream.flush();
	worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
	println!("abox materialization finished, first 90%; elapsed: {:?} at {:?}", timer.elapsed(), index);

	// next9_triples
	//     .for_each(|triple| {
	// 	abox_input_stream.insert((triple.0, triple.1, triple.2));
        // });
        // abox_input_stream.advance_to(2); abox_input_stream.flush();
	// worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
	// println!("abox materialization finished, next 9%; elapsed: {:?} at {:?}", timer.elapsed(), index);

	// next1_triples
	//     .for_each(|triple| {
	// 	abox_input_stream.insert((triple.0, triple.1, triple.2));
        // });
        // abox_input_stream.advance_to(3); abox_input_stream.flush();
	// worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
	// println!("abox materialization finished, next 1%; elapsed: {:?} at {:?}", timer.elapsed(), index);
	
	// Updates
	// abox_triples
	//     .for_each(|triple| {
	// 	abox_input_stream.insert((triple.0, triple.1, triple.2));
        // });
        // abox_input_stream.advance_to(2); abox_input_stream.flush();
	// worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
	
    });

    println!("outer timer: {:?}", outer_timer.elapsed());

}

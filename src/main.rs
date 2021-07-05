use std::env;
use std::net::TcpStream;

use differential_dataflow::input::Input;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::Threshold;

use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::iterate;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::CursorDebug;
use lasso::{Key, Rodeo, Spur};
use rdfs_materialization::load_encode_triples::{load3enc, load3nt};
use rdfs_materialization::rdfs_materialization::*;
use timely::dataflow::Scope;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::probe::Handle;
use timely::order::Product;

fn main() {

    let outer_timer = ::std::time::Instant::now();
    timely::execute_from_args(std::env::args(), move |worker| {
	let timer = worker.timer();
	let index = worker.index();
	let peers = worker.peers();
	let prefix = String::from("lubm50_split");
	let index_as_string = index.to_string();
	let suffix = String::from(".ntenc");
	let blerp = [prefix, index_as_string, suffix].join("");
	let abox_triples = load3enc("./tests/data/", &blerp[..]);
	let tbox_triples = load3enc("./tests/data/", "tbox.ntenc");

	let tbox_probe = Handle::new();
	let abox_probe = Handle::new();
	
	if let Ok(addr) = env::var("DIFFERENTIAL_LOG_ADDR") {
            if !addr.is_empty() {
		if let Ok(stream) = TcpStream::connect(&addr) {
		    differential_dataflow::logging::enable(worker, stream);
                }
	    } else {
                panic!("Could not connect to differential log address: {:?}", addr);
	    }
        } else {
	    println!("Failed to connect to diff logs")
	}
	
	
        let (mut tbox_input_stream, mut abox_input_stream) = worker.dataflow::<usize, _, _>(|outer| {
	    
            let (mut _abox_in, mut abox) = outer.new_collection::<(usize, usize, usize), isize>();
            let (mut _tbox_in, mut tbox) = outer.new_collection::<(usize, usize, usize), isize>();

            let sco = rule_11(&tbox);
            let spo = rule_5(&tbox);
	    let tbox = tbox
		.concat(&sco)
		.concat(&spo)
		.consolidate();

	    let tbox_by_s = tbox
		.map(|(s, p, o)|(s, (p, o)))
		.arrange_by_key();

	    let sco_assertions = tbox_by_s.filter(|s, (p, o)| p == &0usize);
	    let spo_assertions = tbox_by_s.filter(|s, (p, o)| p == &1usize);
	    let domain_assertions = tbox_by_s.filter(|s, (p, o)| p == &2usize);
	    let range_assertions = tbox_by_s.filter(|s, (p, o)| p == &3usize);
	    //let transitivity_assertions = tbox_by_s.filter(|s, (p, o)| p == &4usize);
	    //let inverseof_assertions = tbox_by_s.filter(|s, (p, o)| p == &5usize);

	    // preparing the abox

	    let abox_by_o = abox.map(|(s, p, o)|(o, (s, p)));
	    let abox_by_p = abox.map(|(s, p, o)|(p, (s, o)));
	    
	    let type_assertions = abox_by_o.filter(|(o, (s, p))| p == &4usize);
	    let not_type_assertions = abox_by_p.filter(|(p, (s, o))| p != &4usize);
 

	    // abox reasoning
	    /*
	    let sco_type = type_assertions
		.iterate(|inner| {
		    let arr = inner.arrange_by_key();
		    let tbox_in = sco_assertions.enter(&inner.scope());
		    
		    tbox_in
			.join_core(&arr, |key, &(sco, y), &(z, type_)| Some((y, (z, type_))))
			.concat(inner)
			.distinct()
		});

	    let spo_type = abox_by_p
		.iterate(|inner| {
		    let arr = inner.arrange_by_key();
		    let tbox_in = spo_assertions.enter(&inner.scope());

		    tbox_in
			.join_core(&arr, |key, &(spo, b), &(x, y)| Some((b, (x, y))))
			.concat(inner)
			.distinct()
		});
	     */

	    let (sco_type, spo_type) = outer.iterative::<usize,_,_>(|inner| {

		let sco_var = iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));
		let spo_var = iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));

		let sco_new = sco_var.distinct();
		let spo_new = spo_var.distinct();

		let sco_arr = sco_new.arrange_by_key();
		let spo_arr = spo_new.arrange_by_key();

		let sco_ass = sco_assertions.enter(inner);
		let spo_ass = spo_assertions.enter(inner);

		let sco_iter_step = sco_ass
		    .join_core(&sco_arr, |key, &(sco, y), &(z, type_)| Some((y, (z, type_))));

		let spo_iter_step = spo_ass
		    .join_core(&spo_arr, |key, &(spo, b), &(x, y)| Some((b, (x, y))));

		sco_var.set(&type_assertions.enter(inner).concatenate(vec![sco_iter_step]));
		spo_var.set(&not_type_assertions.enter(inner).concatenate(vec![spo_iter_step]));

		(sco_new.leave(), spo_new.leave())
		
	    });

	    abox = sco_type
		.concat(&spo_type)
		.map(|(b, (x, y))| (x, b, y))
		.concat(&abox)
		.consolidate();

	    let abox_by_p = abox
		.map(|(s, p, o)| (p, (s, o)))
		.arrange_by_key();

	    let domain_type = domain_assertions
		.join_core(&abox_by_p, |a, &(domain, x), &(y, z)| Some((y, 4usize, x)));

	    let range_type = range_assertions
		.join_core(&abox_by_p, |a, &(range, x), &(y, z)| Some((z, 4usize, x)));
	    
	    abox = abox
		.concat(&domain_type)
		.concat(&range_type)
		.consolidate();

            (_tbox_in, _abox_in)
        });

	if worker.index() == 0 {
	    tbox_triples.
		for_each(|triple| {
		    tbox_input_stream.insert((triple.0, triple.1, triple.2)); });
	}
        tbox_input_stream.advance_to(1); tbox_input_stream.flush();
	worker.step_while(|| tbox_probe.less_than(tbox_input_stream.time()));
	abox_triples
	    .for_each(|triple| {
		abox_input_stream.insert((triple.0, triple.1, triple.2));
        });
        abox_input_stream.advance_to(1); abox_input_stream.flush();
	worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
	println!("abox reasoning finished; elapsed: {:?} at {:?}", timer.elapsed(), index);
	
    });

    println!("outer timer: {:?}", outer_timer.elapsed());

}

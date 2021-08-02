use std::collections::BTreeMap;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::CursorDebug;
use differential_reasoner::load_encode_triples::{load3enc, loadkvenc, read_file};
use differential_reasoner::materializations::*;
use timely::dataflow::operators::probe::Handle;

#[test]
fn rdfspp_test() {
    let tbox_triples = load3enc("./encoded_data/test/tbox.ntenc");
    let abox_triples = load3enc("./encoded_data/test/abox.ntenc");

    let (mut tbox_summaries, mut abox_summaries) = timely::execute_directly(move |worker| {
	
	let mut tbox_probe = Handle::new();
	let mut abox_probe = Handle::new();
	
        let (mut tbox_input_stream, mut abox_input_stream, mut tbox_trace, mut abox_trace) = worker.dataflow::<usize, _, _>(|outer| {

	    let (mut _abox_in, abox) = outer
		.new_collection::<(usize, usize, usize), isize>();
            let (mut _tbox_in, tbox) = outer
		.new_collection::<(usize, usize, usize), isize>();

	    let (tbox, abox) = rdfspp(&tbox, &abox, outer);

	    tbox.probe_with(&mut tbox_probe);
	    abox.probe_with(&mut abox_probe);

	    let tbox_arr = tbox.arrange_by_self();
	    let abox_arr = abox.arrange_by_self();
	    
            (_tbox_in, _abox_in, tbox_arr.trace, abox_arr.trace)
        });

	tbox_triples.
	    for_each(|triple| {
		tbox_input_stream.insert((triple.0, triple.1, triple.2)); });
	
        tbox_input_stream.advance_to(1); tbox_input_stream.flush();
	worker.step_while(|| tbox_probe.less_than(tbox_input_stream.time()));

	abox_triples
	    .for_each(|triple| {
		abox_input_stream.insert((triple.0, triple.1, triple.2));
        });
        abox_input_stream.advance_to(1); abox_input_stream.flush();
	worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
	
	let (mut tbox_cursor, tbox_storage) = tbox_trace.cursor();
	let (mut abox_cursor, abox_storage) = abox_trace.cursor();

	(tbox_cursor.to_vec(&tbox_storage), abox_cursor.to_vec(&abox_storage))
    });

    let encoding_map_file = loadkvenc("./encoded_data/test/encoding_mapping.kv");

    let mut encoding_map = BTreeMap::<usize, String>::new();

    for key_value_par in encoding_map_file {
	let (key, value) = key_value_par;
	encoding_map.insert(key, value.clone());
    };

    let tbox_size = tbox_summaries.len();

    let abox_size = abox_summaries.len();

    for summary in abox_summaries.drain(..) {

	let encoded_triple = summary.0.0;

	let s = encoding_map.get(&encoded_triple.0).unwrap();
	let p = encoding_map.get(&encoded_triple.1).unwrap();
	let o = encoding_map.get(&encoded_triple.2).unwrap();
	
	println!("Abox entry: {:?}", (s, p, o))
    };

    for summary in tbox_summaries.drain(..) {
	
	let encoded_triple = summary.0.0;

	let s = encoding_map.get(&encoded_triple.0).unwrap();
	let p = encoding_map.get(&encoded_triple.1).unwrap();
	let o = encoding_map.get(&encoded_triple.2).unwrap();
	
	println!("Tbox entry: {:?}", (s, p, o))
	
    }
 
    /*
    This is the amount of materialized tuples that RDFox got with the same set of rules
     */

    assert_eq!(tbox_size, 29);
    assert_eq!(abox_size, 28);

}

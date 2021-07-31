use std::env;
use std::net::TcpStream;

use differential_dataflow::trace::cursor::CursorDebug;
use differential_dataflow::trace::TraceReader;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::iterate;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Threshold;
use differential_reasoner::load_encode_triples::load3enc;
use differential_reasoner::materializations;
use differential_reasoner::materializations::*;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::ResultStream;
use timely::dataflow::Scope;
use timely::order::Product;

fn main() {
    let outer_timer = ::std::time::Instant::now();
    let summaries = timely::execute_from_args(std::env::args(), move |worker| {
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

        let part1_data = String::from("abox_part");
        let part2_data = String::from("part2-lubm50_split");
        let part3_data = String::from("part3-lubm50_split");
        let index_as_string = index.to_string();
        let suffix = String::from(".ntenc");

        let first90 = [part1_data, index_as_string.clone(), suffix.clone()].join("");
        let first90_triples = load3enc(&format!("./encoded_data/lubms/50/{}", &first90));
        // let next9 = [part2_data, index_as_string.clone(), suffix.clone()].join("");
        // let next9_triples = load3enc("./tests/data/update_data/", &next9);
        // let next1 =  [part3_data, index_as_string.clone(), suffix.clone()].join("");
        // let next1_triples = load3enc("./tests/data/update_data/", &next1);
        let tbox_triples = load3enc("./encoded_data/lubms/50/tbox.ntenc");

        let mut tbox_probe = Handle::new();
        let mut abox_probe = Handle::new();

        let (mut tbox_input_stream, mut abox_input_stream, mut tbox_trace, mut abox_trace) = worker
            .dataflow::<usize, _, _>(|outer| {
                // Here we are creating the actual stream inputs
                let (mut _abox_in, abox) = outer.new_collection::<(usize, usize, usize), isize>();
                let (mut _tbox_in, tbox) = outer.new_collection::<(usize, usize, usize), isize>();

                let (tbox, abox) = rdfspp(&tbox, &abox, outer);

                tbox.probe_with(&mut tbox_probe);
                abox.probe_with(&mut abox_probe);

                let tbox_arr = tbox.arrange_by_self();
                let abox_arr = abox.arrange_by_self();

                (_tbox_in, _abox_in, tbox_arr.trace, abox_arr.trace)
            });

        // Tbox
        if 0 == worker.index() {
            tbox_triples.for_each(|triple| {
                tbox_input_stream.insert((triple.0, triple.1, triple.2));
            });
        }
        tbox_input_stream.advance_to(1);
        tbox_input_stream.flush();
        worker.step_while(|| tbox_probe.less_than(tbox_input_stream.time()));

        first90_triples.for_each(|triple| {
            abox_input_stream.insert((triple.0, triple.1, triple.2));
        });
        abox_input_stream.advance_to(1);
        abox_input_stream.flush();
        worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
        println!(
            "abox materialization finished, first 90%; elapsed: {:?} at {:?}",
            timer.elapsed(),
            index
        );

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

        let (mut tbox_cursor, tbox_storage) = tbox_trace.cursor();
        let (mut abox_cursor, abox_storage) = abox_trace.cursor();

        (
            tbox_cursor.to_vec(&tbox_storage),
            abox_cursor.to_vec(&abox_storage),
        )
    })
    .unwrap()
    .join();

    let mut abox_triples = 0;
    let mut tbox_triples = 0;

    for worker in summaries.into_iter() {
        let (tbox, abox) = worker.unwrap();

        println!("tbox len {:?} abox len {:?}", tbox.len(), abox.len());

        tbox_triples = tbox_triples + tbox.len();
        abox_triples = abox_triples + abox.len();
    }

    println!(
        "Full tbox size {:?} \nFull abox size {:?}",
        tbox_triples, abox_triples
    );
}

#![feature(duration_constants)]
use differential_dataflow::trace::cursor::CursorDebug;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::AsCollection;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::Consolidate;
use differential_reasoner::load_encode_triples::load3enc;
use differential_reasoner::materializations::*;
use differential_reasoner::owl2rl;
use differential_reasoner::{
    constants,
    constants::{owl, rdf, xml},
};
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::Map;

use ddshow_sink;

use clap::{App, Arg};
use lasso::{Key, Rodeo, Spur};
use timely::progress::frontier::AntichainRef;
use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io::{BufRead, BufReader};
use std::time::Duration;
use std::time::Instant;

fn read_file(filename: &str) -> impl Iterator<Item = String> {
    let file = BufReader::new(File::open(filename).unwrap());
    file.lines().filter_map(|line| line.ok())
}

pub fn load3nt<'a>(filename: &str) -> impl Iterator<Item = (String, String, String)> + 'a {
    read_file(filename).map(move |line| {
        let mut line_clean = line;

        //        line_clean.pop();

        //        line_clean.pop();

        let mut elts = line_clean.split(' ');

        (
            elts.next().unwrap().parse().unwrap(),
            elts.next().unwrap().parse().unwrap(),
            elts.next().unwrap().parse().unwrap(),
        )
    })
}

fn main() {
    let matches = App::new("differential-reasoner")
        .version("0.2.0")
        .about("Reasons in a differential manner 😎")
        .arg(
            Arg::new("TBOX_PATH")
                .about("Sets the tbox file path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("ABOX_PATH")
                .about("Sets the abox file path")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("EXPRESSIVITY")
                .about("Sets the expressivity")
                .required(true)
                .index(3),
        )
        .arg(
            Arg::new("WORKERS")
                .about("Sets the amount of workers")
                .required(true)
                .index(4),
        )
        .arg(
            Arg::new("BATCH_SIZE")
                .required(true)
                .index(5),
        )
        .arg(
            Arg::new("STEP_COUNT")
                .required(true)
                .index(6)
        )
        .arg(Arg::new("ENCODE").about("Encodes the input").short('e'))
        .get_matches();

    let t_path: String = matches.value_of("TBOX_PATH").unwrap().to_string();
    let a_path: String = matches.value_of("ABOX_PATH").unwrap().to_string();
    let expressivity: String = matches.value_of("EXPRESSIVITY").unwrap().to_string();
    let batch_size: u32 = matches.value_of("BATCH_SIZE").unwrap().parse().unwrap();
    let step_count: u64 = matches.value_of("STEP_COUNT").unwrap().parse().unwrap();
    let workers: usize = matches
        .value_of("WORKERS")
        .unwrap()
        .to_string()
        .parse::<usize>()
        .unwrap();
    let encode: bool = matches.is_present("ENCODE");

    let now = Instant::now();

    let mut config = timely::Config::process(workers);
    //config.worker = config.worker.progress_mode(timely::worker::ProgressMode::Eager);
    let summaries = timely::execute(config, move |worker| {
	let loggers= /*{ if let Some(folder) = */::std::env::var("TIMELY_WORKER_ALL_LOG_FOLDER").map(|folder| {
	    let timely_logs = ddshow_sink::save_timely_logs_to_disk(worker, &folder).unwrap();
	    let differential_logs = ddshow_sink::save_differential_logs_to_disk(worker, &folder).unwrap();
    	    let timely_progress_logs = ddshow_sink::save_timely_progress_to_disk(worker, &folder).unwrap();
	    (timely_logs, differential_logs, timely_progress_logs)
	}// else {Err(std::io::Error::new(std::io::ErrorKind::Other, "No logging requested"))}
	);
        let mut grand_ole_pry =
            Rodeo::<Spur, BuildHasherDefault<DefaultHasher>>::with_hasher(Default::default());
        let mut tbox_probe = Handle::new();
        let mut abox_probe = Handle::new();
        let mut tbox = None;

        if let true = encode {
            let tbox_raw = load3nt(&t_path);

            let rdfsco: &str = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
            let rdfspo: &str = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
            let rdfsd: &str = "<http://www.w3.org/2000/01/rdf-schema#domain>";
            let rdfsr: &str = "<http://www.w3.org/2000/01/rdf-schema#range>";
            let rdft: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
            let owltr: &str = "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
            let owlio: &str = "<http://www.w3.org/2002/07/owl#inverseOf>";
            let owlthing: &str = "<http://www.w3.org/2002/07/owl#Thing>";
            let rdfcomment: &str = "<http://www.w3.org/2000/01/rdf-schema#comment>";
            let rdfrest: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
            let rdffirst: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
            let owlmqc: &str = "<http://www.w3.org/2002/07/owl#maxQualifiedCardinality>";
            let owlsvf: &str = "<http://www.w3.org/2002/07/owl#someValuesFrom>";
            let owlec: &str = "<http://www.w3.org/2002/07/owl#equivalentClass>";
            let owlito: &str = "<http://www.w3.org/2002/07/owl#intersectionOf>";
            let owlm: &str = "<http://www.w3.org/2002/07/owl#members>";
            let owlep: &str = "<http://www.w3.org/2002/07/owl#equivalentProperty>";
            let owloprop: &str = "<http://www.w3.org/2002/07/owl#onProperty>";
            let owlpca: &str = "<http://www.w3.org/2002/07/owl#propertyChainAxiom>";
            let owldw: &str = "<http://www.w3.org/2002/07/owl#disjointWith>";
            let owlpdw: &str = "<http://www.w3.org/2002/07/owl#propertyDisjointWith>";
            let owluo: &str = "<http://www.w3.org/2002/07/owl#unionOf>";
            let rdflbl: &str = "<http://www.w3.org/2000/01/rdf-schema#label>";
            let owlhk: &str = "<http://www.w3.org/2002/07/owl#hasKey>";
            let owlavf: &str = "<http://www.w3.org/2002/07/owl#allValuesFrom>";
            let owlco: &str = "<http://www.w3.org/2002/07/owl#complementOf>";
            let owloc: &str = "<http://www.w3.org/2002/07/owl#onClass>";
            let owldm: &str = "<http://www.w3.org/2002/07/owl#distinctMembers>";
            let owlfp: &str = "<http://www.w3.org/2002/07/owl#FunctionalProperty>";
            let owlni: &str = "<http://www.w3.org/2002/07/owl#NamedIndividual>";
            let owlobjprop: &str = "<http://www.w3.org/2002/07/owl#ObjectProperty>";
            let rdfn: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
            let owlc: &str = "<http://www.w3.org/2002/07/owl#Class>";
            let xmlonenni: &str = "\"1\"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>";
            let xmlzeronni: &str = "\"0\"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>";
            let owladc: &str = "<http://www.w3.org/2002/07/owl#AllDisjointClasses>";
            let owlr: &str = "<http://www.w3.org/2002/07/owl#Restriction>";
            let owldp: &str = "<http://www.w3.org/2002/07/owl#DatatypeProperty>";
            let rdflit: &str = "<http://www.w3.org/2000/01/rdf-schema#Literal>";
            let owlo: &str = "<http://www.w3.org/2002/07/owl#Ontology>";
            let owlap: &str = "<http://www.w3.org/2002/07/owl#AsymmetricProperty>";
            let owlsp: &str = "<http://www.w3.org/2002/07/owl#SymmetricProperty>";
            let owlip: &str = "<http://www.w3.org/2002/07/owl#IrreflexiveProperty>";
            let owlad: &str = "<http://www.w3.org/2002/07/owl#AllDifferent>";
            let owlifp: &str = "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>";
            let owlsa: &str = "<http://www.w3.org/2002/07/owl#sameAs>";

            assert_eq!(
                grand_ole_pry.get_or_intern(rdfspo).into_inner().get(),
                rdf::subPropertyOf
            );
            grand_ole_pry.get_or_intern(rdfsd);
            grand_ole_pry.get_or_intern(rdfsr);
            grand_ole_pry.get_or_intern(rdft);
            grand_ole_pry.get_or_intern(owltr);
            grand_ole_pry.get_or_intern(owlio);
            grand_ole_pry.get_or_intern(owlthing);
            grand_ole_pry.get_or_intern(rdfcomment);
            grand_ole_pry.get_or_intern(rdfrest);
            grand_ole_pry.get_or_intern(rdffirst);
            grand_ole_pry.get_or_intern(owlmqc);
            assert_eq!(
                grand_ole_pry.get_or_intern(owlsvf).into_inner().get(),
                owl::someValuesFrom
            );
            grand_ole_pry.get_or_intern(owlec);
            grand_ole_pry.get_or_intern(owlito);
            grand_ole_pry.get_or_intern(owlm);
            grand_ole_pry.get_or_intern(owlep);
            grand_ole_pry.get_or_intern(owloprop);
            grand_ole_pry.get_or_intern(owlpca);
            grand_ole_pry.get_or_intern(owldw);
            grand_ole_pry.get_or_intern(owlpdw);
            grand_ole_pry.get_or_intern(owluo);
            grand_ole_pry.get_or_intern(rdflbl);
            assert_eq!(
                grand_ole_pry.get_or_intern(owlhk).into_inner().get(),
                owl::hasKey
            );
            grand_ole_pry.get_or_intern(owlavf);
            grand_ole_pry.get_or_intern(owlco);
            assert_eq!(
                grand_ole_pry.get_or_intern(owloc).into_inner().get(),
                owl::onClass
            );
            grand_ole_pry.get_or_intern(owldm);
            grand_ole_pry.get_or_intern(owlfp);
            assert_eq!(
                grand_ole_pry.get_or_intern(owlni).into_inner().get(),
                owl::NamedIndividual
            );
            assert_eq!(
                grand_ole_pry.get_or_intern(owlobjprop).into_inner().get(),
                owl::ObjectProperty
            );
            assert_eq!(
                grand_ole_pry.get_or_intern(rdfn).into_inner().get(),
                rdf::nil
            );
            assert_eq!(
                grand_ole_pry.get_or_intern(owlc).into_inner().get(),
                owl::Class
            );
            assert_eq!(
                grand_ole_pry.get_or_intern(xmlonenni).into_inner().get(),
                xml::nonNegativeInteger_1
            );
            grand_ole_pry.get_or_intern(xmlzeronni);
            grand_ole_pry.get_or_intern(owladc);
            grand_ole_pry.get_or_intern(owlr);
            grand_ole_pry.get_or_intern(owldp);
            grand_ole_pry.get_or_intern(rdflit);
            assert_eq!(
                grand_ole_pry.get_or_intern(owlo).into_inner().get(),
                owl::Ontology
            );
            grand_ole_pry.get_or_intern(owlap);
            grand_ole_pry.get_or_intern(owlsp);
            grand_ole_pry.get_or_intern(owlip);
            grand_ole_pry.get_or_intern(owlad);
            grand_ole_pry.get_or_intern(owlifp);
            assert_eq!(
                grand_ole_pry.get_or_intern(owlsa).into_inner().get(),
                owl::sameAs
            );
            assert_eq!(
                grand_ole_pry.get_or_intern(rdfsco).into_inner().get(),
                rdf::subClassOf
            );
            assert_eq!(rdf::subClassOf, constants::MAX_CONST);

            tbox = Some(
                tbox_raw
                    .map(|triple| {
                        let s = &triple.0[..];
                        let p = &triple.1[..];
                        let o = &triple.2[..];

                        let key_s = grand_ole_pry.get_or_intern(s);
                        let key_p = grand_ole_pry.get_or_intern(p);
                        let key_o = grand_ole_pry.get_or_intern(o);

                        let key_s_int = key_s.into_inner().get().try_into().unwrap();
                        let key_p_int = key_p.into_inner().get().try_into().unwrap();
                        let key_o_int = key_o.into_inner().get().try_into().unwrap();

                        println!(
                            "{}, {}, {}: {}, {}, {}",
                            key_s_int, key_p_int, key_o_int, s, p, o
                        );
                        (key_s_int, key_p_int, key_o_int)
                    })
                    .collect::<Vec<_>>(),
            );
        }

        let (mut tbox_input_stream, mut abox_input_stream, mut tbox_trace, mut abox_trace)  = worker
            .dataflow::<u64, _, _>(|outer| {
                let (mut _abox_in, abox) = outer.new_collection::<(usize, usize, usize), isize>();

                println!("T-box location: {}", &t_path);

                let (_tbox_in, (tbox, abox)) = match &expressivity[..] {
                    "rdfs" => {
                        let (mut _tbox_in, tbox_collection) =
                            outer.new_collection::<(usize, usize, usize), isize>();
                        (Some(_tbox_in), rdfs(&tbox_collection, &abox, outer))
                    }
                    "owl2rl" => differential_reasoner::owl2rl::build_dataflow::digest_tbox::<_,_,_,_, u64>(
                        (&tbox).as_ref().unwrap().iter().cloned(),
                        abox.clone(),
                    )
                    .map(|abox_out_raw| {
                        let tbox_out = abox.clone();
                        let abox_out = abox_out_raw
                            .consolidate()
                            .inner
                            .map(|((s, p, o), t, _r)| ((s as usize, p as usize, o as usize), t, 1))
                            .as_collection();
                        println!("Got normal ABox stream output after building dataflow.");
                        (None, (tbox_out, abox_out))
                    })
                    .unwrap(),
                    _ => {
                        let (mut _tbox_in, tbox) =
                            outer.new_collection::<(usize, usize, usize), isize>();
                        (Some(_tbox_in), rdfspp(&tbox, &abox, outer))
                    }
                };

                tbox.probe_with(&mut tbox_probe);
                abox.probe_with(&mut abox_probe);

                let tbox_arr = tbox.arrange_by_self();
                let abox_arr = abox.arrange_by_self();

                (_tbox_in, _abox_in, tbox_arr.trace, abox_arr.trace)
            });
        if let Some(tbox) = tbox {
            if 0 == worker.index() {
                let mut abox = load3nt(&a_path);
                println!("A-box location: {}", &a_path);

                if let Some(mut tbox_input_stream) = tbox_input_stream.take() {
                    tbox.iter().for_each(|&triple| {
                        tbox_input_stream.insert(triple);
                    });
                    tbox_input_stream.close();
                }

                let BATCH_SIZE: u32 = batch_size;   //100000;

                let mut iteration_count = 0;

                'outer: loop {
                    for _ in 0..BATCH_SIZE {
                        if let Some(triple) = abox.next() {
                            let s = &triple.0[..];
                            let p = &triple.1[..];
                            let o = &triple.2[..];

                            let key_s = grand_ole_pry.get_or_intern(s);
                            let key_p = grand_ole_pry.get_or_intern(p);
                            let key_o = grand_ole_pry.get_or_intern(o);

                            let key_s_int = key_s.into_inner().get().try_into().unwrap();
                            let key_p_int = key_p.into_inner().get().try_into().unwrap();
                            let key_o_int = key_o.into_inner().get().try_into().unwrap();

                            abox_input_stream.insert((key_s_int, key_p_int, key_o_int));
                        } else {
                            break 'outer;
                        }
                    }
                    let worker_index = worker.index();
                    let STEP_COUNT: u64 = step_count;
                    abox_input_stream.advance_to(abox_input_stream.epoch() + STEP_COUNT/*(u32::MAX as u64)*/);
                    abox_input_stream.flush();
                    for _ in 0..STEP_COUNT {
                        worker.step();
                        println!(
                            "{}-inside-abox-step-loop1, iteration {}",
                            worker_index, iteration_count
                        );
                        iteration_count += 1;
                    }
                    /*
                    worker.step_while(|| {
                                    println!(
                                        "{}-inside-abox-probe-loop1, iteration {}",
                                        worker_index, iteration_count
                                    );
                                    iteration_count += 1;
                                    abox_probe.less_than(abox_input_stream.time())
                                });
                    */
                }
            }
        } else {
            let abox = load3enc(&a_path);
            if let Some(ref mut tbox_input_stream) = tbox_input_stream {
                let tbox = load3enc(&t_path);
                tbox.for_each(|triple| {
                    tbox_input_stream.insert((triple.0, triple.1, triple.2));
                });
            };
            if 0 == worker.index() {
                abox.for_each(|triple| {
                    abox_input_stream.insert((triple.0, triple.1, triple.2));
                });
            }
        };

        if let Some(mut tbox_input_stream) = tbox_input_stream.take() {
            tbox_input_stream.advance_to(1);
            tbox_input_stream.flush();
            tbox_input_stream.close();
        };
        //        worker.step();
        //        abox_input_stream.advance_to(1);
        abox_input_stream.flush();
        let abox_input_stream_time = *abox_input_stream.time();
        abox_input_stream.close();
        worker.step();
        println!("{}-pre-abox-probe-loop", worker.index());
        let mut iteration_count = 0;
        let worker_index = worker.index();
	tbox_trace.set_logical_compaction(AntichainRef::new(&[(u32::MAX as u64)*100]));
	abox_trace.set_logical_compaction(AntichainRef::new(&[(u32::MAX as u64)*100]));
	abox_trace.set_physical_compaction(AntichainRef::new(&[(u32::MAX as u64) * 100]));
        worker.step_or_park_while(Some(Duration::SECOND), || {
            abox_probe.with_frontier(|frontier| {
                println!(
                    "{}-inside-abox-probe-loop2, iteration {}, frontier: {:?}",
                    worker_index, iteration_count, &frontier
                );
            });
            let frontier = abox_probe.with_frontier(|x| (*x).to_owned());
            iteration_count += 1;
            //abox_probe.less_than(&abox_input_stream_time)
            !abox_probe.done()
        });
        println!("{}-post-abox-probe-loop", worker.index());
        let (mut tbox_cursor, tbox_storage) = tbox_trace.cursor();
        let (mut abox_cursor, abox_storage) = abox_trace.cursor();
        let vectors = (
            tbox_cursor.to_vec(&tbox_storage),
            abox_cursor.to_vec(&abox_storage),
        );
        println!("{}-post-cursor.to_vec", worker_index);
        vectors
    })
    .unwrap()
    .join();

    let mut abox_triples = 0;
    let mut tbox_triples = 0;

    for worker in summaries.into_iter() {
        let (tbox, abox) = worker.unwrap();
        tbox_triples += tbox.len();
        abox_triples += abox.len();
    }

    println!(
        "Full tbox size {:?} \nFull abox size {:?}",
        tbox_triples, abox_triples
    );

    if let true = encode {
        println!(
            "loading+interning+materialization time: {:?}",
            now.elapsed()
        )
    } else {
        println!("loading+materialization time: {:?}", now.elapsed())
    }
}

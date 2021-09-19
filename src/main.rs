use differential_dataflow::trace::cursor::CursorDebug;
use differential_dataflow::trace::TraceReader;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_reasoner::load_encode_triples::load3enc;
use differential_reasoner::materializations::*;
use timely::dataflow::operators::probe::Handle;

use clap::{App, Arg};
use lasso::{Key, Rodeo};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn read_file(filename: &str) -> impl Iterator<Item = String> {
    let file = BufReader::new(File::open(filename).unwrap());
    file.lines().filter_map(|line| line.ok())
}

pub fn load3nt<'a>(filename: &str) -> impl Iterator<Item = (String, String, String)> + 'a {
    read_file(filename).map(move |line| {
        let mut line_clean = line;

        line_clean.pop();

        line_clean.pop();

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
        .arg(Arg::new("ENCODE").about("Encodes the input").short('e'))
        .get_matches();

    let t_path: String = matches.value_of("TBOX_PATH").unwrap().to_string();
    let a_path: String = matches.value_of("ABOX_PATH").unwrap().to_string();
    let expressivity: String = matches.value_of("EXPRESSIVITY").unwrap().to_string();
    let workers: usize = matches
        .value_of("WORKERS")
        .unwrap()
        .to_string()
        .parse::<usize>()
        .unwrap();
    let encode: bool = matches.is_present("ENCODE");

    let now = Instant::now();

    let summaries = timely::execute(timely::Config::process(workers), move |worker| {
        let mut tbox_probe = Handle::new();
        let mut abox_probe = Handle::new();

        let (mut tbox_input_stream, mut abox_input_stream, mut tbox_trace, mut abox_trace) = worker
            .dataflow::<usize, _, _>(|outer| {
                let (mut _abox_in, abox) = outer.new_collection::<(usize, usize, usize), isize>();
                let (mut _tbox_in, tbox) = outer.new_collection::<(usize, usize, usize), isize>();

                let (tbox, abox) = match &expressivity[..] {
                    "rdfs" => rdfs(&tbox, &abox, outer),
                    _ => rdfspp(&tbox, &abox, outer),
                };

                tbox.probe_with(&mut tbox_probe);
                abox.probe_with(&mut abox_probe);

                let tbox_arr = tbox.arrange_by_self();
                let abox_arr = abox.arrange_by_self();

                (_tbox_in, _abox_in, tbox_arr.trace, abox_arr.trace)
            });

        if 0 == worker.index() {
            if let true = encode {
                let abox = load3nt(&a_path);
                println!("A-box location: {}", &a_path);
                let tbox = load3nt(&t_path);
                println!("T-box location: {}", &t_path);

                let mut grand_ole_pry = Rodeo::default();

                let rdfsco: &str = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
                let rdfspo: &str = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
                let rdfsd: &str = "<http://www.w3.org/2000/01/rdf-schema#domain>";
                let rdfsr: &str = "<http://www.w3.org/2000/01/rdf-schema#range>";
                let rdft: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
                let owltr: &str = "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
                let owlio: &str = "<http://www.w3.org/2002/07/owl#inverseOf>";

                grand_ole_pry.get_or_intern(rdfsco);
                grand_ole_pry.get_or_intern(rdfspo);
                grand_ole_pry.get_or_intern(rdfsd);
                grand_ole_pry.get_or_intern(rdfsr);
                grand_ole_pry.get_or_intern(rdft);
                grand_ole_pry.get_or_intern(owltr);
                grand_ole_pry.get_or_intern(owlio);

                tbox.for_each(|triple| {
                    let s = &triple.0[..];
                    let p = &triple.1[..];
                    let o = &triple.2[..];

                    let key_s = grand_ole_pry.get_or_intern(s);
                    let key_p = grand_ole_pry.get_or_intern(p);
                    let key_o = grand_ole_pry.get_or_intern(o);

                    let key_s_int = key_s.into_usize();
                    let key_p_int = key_p.into_usize();
                    let key_o_int = key_o.into_usize();

                    tbox_input_stream.insert((key_s_int, key_p_int, key_o_int));
                });
                abox.for_each(|triple| {
                    let s = &triple.0[..];
                    let p = &triple.1[..];
                    let o = &triple.2[..];

                    let key_s = grand_ole_pry.get_or_intern(s);
                    let key_p = grand_ole_pry.get_or_intern(p);
                    let key_o = grand_ole_pry.get_or_intern(o);

                    let key_s_int = key_s.into_usize();
                    let key_p_int = key_p.into_usize();
                    let key_o_int = key_o.into_usize();

                    abox_input_stream.insert((key_s_int, key_p_int, key_o_int));
                });
            } else {
                {
                    let tbox = load3enc(&t_path);
                    let abox = load3enc(&a_path);
                    tbox.for_each(|triple| {
                        tbox_input_stream.insert((triple.0, triple.1, triple.2));
                    });
                    abox.for_each(|triple| {
                        abox_input_stream.insert((triple.0, triple.1, triple.2));
                    });
                }
            };
        }
        tbox_input_stream.advance_to(1);
        tbox_input_stream.flush();
        worker.step();
        abox_input_stream.advance_to(1);
        abox_input_stream.flush();
        worker.step();
        worker.step_while(|| abox_probe.less_than(abox_input_stream.time()));
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

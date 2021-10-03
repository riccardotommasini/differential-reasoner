use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::lazy::OnceCell;

use differential_dataflow::collection::concatenate;
use differential_dataflow::difference::Present;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{iterate::SemigroupVariable, Consolidate};
use differential_dataflow::{AsCollection, Collection};
use dogsdogsdogs::altneu::AltNeu;
use timely::dataflow::operators::{Map, Partition};
use timely::dataflow::ScopeParent;
use timely::progress::timestamp::Refines;

use crate::constants::{owl, rdf, xml};
use crate::owl2rl::{consolidate_stream_aggressively::ConsolidateStreamAggressive, SameAs};

use super::class_rules::*;
use super::property_rules::*;
use super::IRI;

type Timestamp = u64;

#[derive(Clone, Debug)]
pub struct DisjointSet(Vec<u32>);

impl DisjointSet {
    fn new<I>(set: I) -> DisjointSet
    where
        I: IntoIterator<Item = u32>,
    {
        let max = set.into_iter().fold(0, u32::max);
        DisjointSet((0..=max).collect())
    }

    fn union(&mut self, x: u32, y: u32) {
        let mut x_found = self.find(x);
        let mut y_found = self.find(y);
        if x_found <= crate::constants::MAX_CONST {
            std::mem::swap(&mut x_found, &mut y_found);
        };
        self.0[x_found as usize] = y_found;
    }

    fn find(&mut self, x: u32) -> u32 {
        let mut x_found = x;
        while self.0[x_found as usize] != x_found {
            let x_found_saved = x_found;
            x_found = self.0[x_found as usize];
            self.0[x_found_saved as usize] = self.0[x_found as usize];
        }
        assert!(x > crate::constants::MAX_CONST || x_found <= x);
        x_found
    }
}

pub fn digest_tbox<I, D, G, R, T>(
    tbox: I,
    abox: Collection<G, (D, D, D), R>,
) -> Result<Collection<G, (u32, u32, u32), Present>, <D as TryInto<IRI>>::Error>
where
    I: IntoIterator<Item = (D, D, D)>,
    D: TryInto<u32> + Clone,
    R: differential_dataflow::difference::Semigroup,
    G: timely::dataflow::Scope,
    G: ScopeParent<Timestamp = Timestamp>,
    (D, D, D): timely::Data,
    AltNeu<T>: Refines<u64>,
    T: timely::progress::Timestamp + Lattice,
{
    let triples = {
        let mut triples = Vec::new();
        for (s, p, o) in tbox.into_iter() {
            triples.push((s.try_into()?, p.try_into()?, o.try_into()?));
        }
        triples
    };

    let mut scope = abox.scope();

    struct Property {
        id: IRI,
        domain: Option<IRI>,
        range: Option<IRI>,
    }

    let defined_properties = triples
        .iter()
        .flat_map(|&triple| match triple {
            (
                p,
                rdf::r#type,
                owl::ObjectProperty
                | owl::SymmetricProperty
                | owl::AsymmetricProperty
                | owl::FunctionalProperty
                | owl::InverseFunctionalProperty
                | owl::DatatypeProperty
                | owl::IrreflexiveProperty
                | owl::TransitiveProperty
                | owl::equivalentProperty
                | owl::inverseOf,
            ) => [
                Some(Property {
                    id: p,
                    domain: None,
                    range: None,
                }),
                None,
            ],
            (p, rdf::domain, c) => [
                Some(Property {
                    id: p,
                    domain: Some(c),
                    range: None,
                }),
                None,
            ],
            (p, rdf::range, c) => [
                Some(Property {
                    id: p,
                    domain: None,
                    range: Some(c),
                }),
                None,
            ],
            (p1, rdf::subPropertyOf, p2) => [
                Some(Property {
                    id: p1,
                    domain: None,
                    range: None,
                }),
                Some(Property {
                    id: p2,
                    domain: None,
                    range: None,
                }),
            ],
            _ => [None, None],
        })
        .flatten();

    let id_set = triples
        .iter()
        .flat_map(|&(s, p, o)| [s, p, o])
        .collect::<BTreeSet<_>>();
    let mut property_feedback = id_set
        .iter()
        .map(|&x| (x, SemigroupVariable::new(&mut scope, 1/*u32::MAX as u64*/)))
        .collect::<BTreeMap<_, _>>();
    let mut class_feedback = id_set
        .iter()
        .map(|&x| (x, SemigroupVariable::new(&mut scope, 1/*u32::MAX as u64*/)))
        .collect::<BTreeMap<_, _>>();
    let same_as_feedback = SemigroupVariable::new(&mut scope, 1/*u32::MAX as u64*/);
    let output = scope
        .clone()
        .scoped::<AltNeu<T>, _, _>("ABox iteration", |inner| {
            let mut properties = property_feedback
                .iter()
                .map(|(&id, feedback)| {
                    (
                        id,
                        super::Property {
                            by_s_alt_: OnceCell::new(),
                            by_o_alt_: OnceCell::new(),
                            by_s_neu_: OnceCell::new(),
                            by_o_neu_: OnceCell::new(),
                            stream_: (*feedback).enter(&inner),
                            feedback_: Vec::new(),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();

            let mut classes = class_feedback
                .iter()
                .map(|(&id, feedback)| {
                    (
                        id,
                        super::Class {
                            alt_: OnceCell::new(),
                            neu_: OnceCell::new(),
                            alt_extender_: OnceCell::new(),
                            neu_extender_: OnceCell::new(),
                            stream_: (*feedback).enter(&inner),
                            feedback_: Vec::new(),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();

            let mut same_as = SameAs {
                alt_: OnceCell::new(),
                neu_: OnceCell::new(),
                stream_: (*same_as_feedback).enter(&inner),
                feedback_: Vec::new(),
            };

            let mut abox_assertions_in_tbox = BTreeSet::new();

            let mut sco = BTreeMap::new();
            let mut spo = BTreeMap::new();

            let first_index = triples
                .iter()
                .flat_map(|&triple| match triple {
                    (h1, rdf::first, e1) => Some((h1, e1)),
                    _ => None,
                })
                .collect::<BTreeMap<_, _>>();
            println!("first_index={:?}", first_index);
            let rest_index = triples
                .iter()
                .flat_map(|&triple| match triple {
                    (h1, rdf::rest, e1) => Some((h1, e1)),
                    _ => None,
                })
                .collect::<BTreeMap<_, _>>();

            // Extract SCO and SPO for unioning
            for &triple in triples.iter() {
                match triple {
                    (c1, rdf::subClassOf, c2) => {
                        sco.entry(c1).or_insert_with(BTreeSet::new).insert(c2);
                    }
                    (c1, owl::equivalentClass, c2) => {
                        sco.entry(c1).or_insert_with(BTreeSet::new).insert(c2);
                        sco.entry(c2).or_insert_with(BTreeSet::new).insert(c1);
                    }
                    (p1, rdf::subPropertyOf, p2) => {
                        spo.entry(p1).or_insert_with(BTreeSet::new).insert(p2);
                    }
                    (p1, owl::equivalentProperty, p2) => {
                        spo.entry(p1).or_insert_with(BTreeSet::new).insert(p2);
                        spo.entry(p2).or_insert_with(BTreeSet::new).insert(p1);
                    }
                    (c, owl::intersectionOf, x) => {
                        let mut rest = x;
                        while rest != rdf::nil {
                            println!("intersectionOf: loop: rest={:?}", rest);
                            sco.entry(c)
                                .or_insert_with(BTreeSet::new)
                                .insert(first_index[&rest]);
                            rest = rest_index[&rest];
                        }
                    }
                    (c, owl::unionOf, x) => {
                        let mut rest = x;
                        while rest != rdf::nil {
                            sco.entry(first_index[&rest])
                                .or_insert_with(BTreeSet::new)
                                .insert(c);
                            rest = rest_index[&rest];
                        }
                    }
                    _ => {}
                }
            }

            let (sco, mut canon_class) = {
                let mut sco = sco;
                let mut canonized_sco = BTreeMap::<u32, BTreeSet<u32>>::new();
                let mut new_new_sco = BTreeMap::<u32, BTreeSet<u32>>::new();
                let mut canonizer = DisjointSet::new(id_set.clone());
                loop {
		    let mut changed = false;
                    // Canonize IDs in `sco`
                    for (&c, subclasses) in sco.iter() {
			let canon_c = canonizer.find(c);
			let canon_subclasses = subclasses.iter().map(|&x| canonizer.find(x)).filter(|&x| x != canon_c).collect::<BTreeSet<_>>();
                        let target = canonized_sco.entry(canon_c).or_default();
                        target.extend(canon_subclasses.iter());
                    }
		    let mut canonized_sco_scm_sco = canonized_sco.clone();
		    for (&c, c2s) in canonized_sco.clone().iter() {
			let canon_c = c;
                        let target = canonized_sco_scm_sco.entry(canon_c).or_default();
			for &c2 in c2s.iter() {
			    if let Some(c3s) = canonized_sco.get(&c2) {
				target.extend(c3s.iter());
			    }
			}
                    }
                    // Union those that contain someone who contains themselves, with that someone
		    for (&c, subclasses) in canonized_sco.iter() {
			for &subclass in subclasses.iter() {
			    if let Some(subclasses_of_subclass) = canonized_sco.get(&subclass) {
				if subclasses_of_subclass.contains(&c) {
				    canonizer.union(c, subclass);
				    println!("calling union: {}, {}", c, subclass);
				    changed = true;
				}
			    }
			}
		    }
		    /*
                    for (&c, subclasses) in canonized_sco.iter() {
                        if subclasses.iter().any(|&c2| {
                            let canon_c2 = canonizer.find(c2);
                            canonized_sco
                                .get(&canon_c2)
                                .and_then(|x| x.contains(&c).then_some(()))
                                .is_some()
                        }) {
                            for &c2 in subclasses.iter() {
                                canonizer.union(c, c2);
                            }
                        }
                    }
		    
                    for (&c1, c2s) in canonized_sco.iter() {
                        for &c3 in c2s.iter().flat_map(|&c2| canonized_sco.get(&c2)).flatten() {
                            (new_new_sco.entry(canonizer.find(c1)).or_default())
                                .insert(canonizer.find(c3));
                        }
                    }
		    */
                    println!(
                        "sco.len: {}, new_sco.len: {}, new_new_sco.len: {}, canonized_sco_scm_sco.len: {}, changed: {}",
                        sco.len(),
                        canonized_sco.len(),
                        new_new_sco.len(),
			canonized_sco_scm_sco.len(),
			&changed
                    );
                    if !changed && sco == canonized_sco && canonized_sco == canonized_sco_scm_sco /*&& canonized_sco == new_new_sco */ {
                        break (sco, canonizer);
                    } else {
                        sco = canonized_sco_scm_sco;
                        canonized_sco = BTreeMap::new();//new_new_sco;
                        new_new_sco = BTreeMap::new();
                    }
                }
            };

	     let (spo, mut canon_property) = {
                let mut spo = spo;
                let mut canonized_spo = BTreeMap::<u32, BTreeSet<u32>>::new();
                let mut new_new_spo = BTreeMap::<u32, BTreeSet<u32>>::new();
                let mut canonizer = DisjointSet::new(id_set.clone());
                loop {
		    let mut changed = false;
                    // Canonize IDs in `spo`
                    for (&p, subproperties) in spo.iter() {
			let canon_p = canonizer.find(p);
			let canon_subproperties = subproperties.iter().map(|&x| canonizer.find(x)).filter(|&x| x != canon_p).collect::<BTreeSet<_>>();
                        let target = canonized_spo.entry(canon_p).or_default();
                        target.extend(canon_subproperties.iter());
                    }
		    let mut canonized_spo_scm_spo = canonized_spo.clone();
		    for (&p, p2s) in canonized_spo.clone().iter() {
			let canon_p = p;
                        let target = canonized_spo_scm_spo.entry(canon_p).or_default();
			for &p2 in p2s.iter() {
			    if let Some(p3s) = canonized_spo.get(&p2) {
				target.extend(p3s.iter());
			    }
			}
                    }
                    // Union those that contain someone who contains themselves, with that someone
		    for (&p, subproperties) in canonized_spo.iter() {
			for &subproperty in subproperties.iter() {
			    if let Some(subproperties_of_subproperty) = canonized_spo.get(&subproperty) {
				if subproperties_of_subproperty.contains(&p) {
				    canonizer.union(p, subproperty);
				    println!("calling union: {}, {}", p, subproperty);
				    changed = true;
				}
			    }
			}
		    }
		    /*
                    for (&c, subproperties) in canonized_spo.iter() {
                        if subproperties.iter().any(|&c2| {
                            let canon_c2 = canonizer.find(c2);
                            canonized_spo
                                .get(&canon_c2)
                                .and_then(|x| x.contains(&c).then_some(()))
                                .is_some()
                        }) {
                            for &c2 in subproperties.iter() {
                                canonizer.union(c, c2);
                            }
                        }
                    }
		    
                    for (&c1, c2s) in canonized_spo.iter() {
                        for &c3 in c2s.iter().flat_map(|&c2| canonized_spo.get(&c2)).flatten() {
                            (new_new_spo.entry(canonizer.find(c1)).or_default())
                                .insert(canonizer.find(c3));
                        }
                    }
		    */
                    println!(
                        "spo.len: {}, new_spo.len: {}, new_new_spo.len: {}, canonized_spo_scm_spo.len: {}, changed: {}",
                        spo.len(),
                        canonized_spo.len(),
                        new_new_spo.len(),
			canonized_spo_scm_spo.len(),
			&changed
                    );
                    if !changed && spo == canonized_spo && canonized_spo == canonized_spo_scm_spo /*&& canonized_spo == new_new_spo */ {
                        break (spo, canonizer);
                    } else {
                        spo = canonized_spo_scm_spo;
                        canonized_spo = BTreeMap::new();//new_new_spo;
                        new_new_spo = BTreeMap::new();
                    }
                }
            };

	    /*
            let (spo, mut canon_property) = {
                let mut spo = spo;
                let mut new_spo = BTreeMap::<u32, BTreeSet<u32>>::new();
                let mut new_new_spo = BTreeMap::<u32, BTreeSet<u32>>::new();
                let mut canonizer = DisjointSet::new(id_set.clone());
                loop {
                    for (&p, subproperties) in spo.iter() {
                        (new_spo.entry(canonizer.find(p)).or_default())
                            .extend(subproperties.iter().map(|&x| canonizer.find(x)));
                    }
                    for (&p, subproperties) in new_spo.iter() {
                        if subproperties.contains(&p) {
                            for &p2 in subproperties.iter() {
                                canonizer.union(p, p2);
                            }
                        }
                    }
                    for (&p1, p2s) in new_spo.iter() {
                        for &p3 in p2s.iter().flat_map(|&p2| new_spo.get(&p2)).flatten() {
                            (new_new_spo.entry(canonizer.find(p1)).or_default())
                                .insert(canonizer.find(p3));
                        }
                    }
                    if spo == new_spo && new_spo == new_new_spo {
                        break (spo, canonizer);
                    } else {
                        spo = new_spo;
                        new_spo = new_new_spo;
                        new_new_spo = BTreeMap::new();
                    }
                }
            };
	    */

            let list = |start: u32| -> Vec<u32> {
                let mut rest = start;
                let mut output = Vec::new();
                while rest != rdf::nil {
                    output.push(first_index[&rest]);
                    rest = rest_index[&rest];
                }
                output
            };

            // apply deduplication
            let triples = {
                let mut triples = spo
                    .into_iter()
                    .flat_map(|(p1, p2s)| {
                        p2s.into_iter().map(move |p2| (p1, rdf::subPropertyOf, p2))
                    })
                    .chain(
                        triples
                            .iter()
                            .flat_map(|&triple| match triple {
                                (s, owl::inverseOf, o) => {
                                    [Some((s, owl::inverseOf, o)), Some((o, owl::inverseOf, s))]
                                }
                                (s, p, o) => [Some((s, p, o)), None],
                            })
                            .flatten(),
                    )
                    .map(|(s, p, o)| {
                        (
                            canon_class.find(canon_property.find(s)),
                            canon_class.find(canon_property.find(p)),
                            canon_class.find(canon_property.find(o)),
                        )
                    })
                    .collect::<Vec<_>>();
                triples.sort_unstable();
                triples.dedup();
                triples
            };
            // Construct dataflow
            for &triple in triples.iter() {
                match triple {
                    (thing, rdf::r#type, class) => {
                        match class {
                            owl::AllDIfferent => {}
                            owl::AllDisjointClasses => {}
                            owl::AsymmetricProperty => {}
                            owl::Class => {}
                            owl::DatatypeProperty => (),
                            owl::FunctionalProperty => {
                                prp_fp(&properties[&canon_property.find(thing)], &mut same_as);
                            }
                            owl::InverseFunctionalProperty => {
                                prp_ifp(&properties[&canon_property.find(thing)], &mut same_as);
                            }
                            owl::IrreflexiveProperty => {}
                            owl::NamedIndividual => {
                                // TODO: Investigate if this needs special treatment due to some semantic importance
                            }
                            owl::ObjectProperty => {}
                            owl::Ontology => {}
                            owl::Restriction => {
                                // TODO: Consider exploiting OWL2RL's limitations on restrictions, specifically for inlining intersections and similar optimizations
                            }
                            owl::SymmetricProperty => {
                                prp_symp(
                                    &mut properties.get_mut(&canon_property.find(thing)).unwrap(),
                                );
                            }
                            owl::TransitiveProperty => {
                                prp_trp(
                                    &mut properties.get_mut(&canon_property.find(thing)).unwrap(),
                                );
                            }
                            _ => {
                                abox_assertions_in_tbox.insert((thing, rdf::r#type, class));
                            }
                        }
                    }
                    (p, rdf::domain, c) => {
                        prp_dom(
                            &properties[&canon_property.find(p)],
                            &mut classes.get_mut(&canon_class.find(c)).unwrap(),
                        );
                    }
                    (p, rdf::range, c) => {
                        prp_rng(
                            &properties[&canon_property.find(p)],
                            &mut classes.get_mut(&canon_class.find(c)).unwrap(),
                        );
                    }
                    (c1, rdf::subClassOf, c2) => {
                        // already taken care of above
                        // sco.entry(c1).or_insert_with(BTreeSet::new).insert(c2);
                    }
                    (p1, rdf::subPropertyOf, p2)
                        if canon_property.find(p1) != canon_property.find(p2) =>
                    {
                        let prop1 = properties.remove(&canon_property.find(p1)).unwrap();
                        let mut prop2 = properties.remove(&canon_property.find(p2)).unwrap();
                        prp_spo1(&prop1, &mut prop2);
                        properties.insert(canon_property.find(p1), prop1);
                        properties.insert(canon_property.find(p2), prop2);
                    }
                    (x, owl::allValuesFrom, y) => {
                        let class = classes.remove(&canon_class.find(x)).unwrap();
                        let mut target_class = classes.remove(&canon_class.find(y)).unwrap();
                        let p = triples
                            .iter()
                            .find_map(|&triple| match triple {
                                (x_search, owl::onProperty, p)
                                    if canon_class.find(x_search) == canon_class.find(x) =>
                                {
                                    Some(canon_property.find(p))
                                }
                                _ => None,
                            })
                            .unwrap();
                        cls_avf(&class, &properties[&p], &mut target_class);
                        classes.insert(x, class);
                        classes.insert(y, target_class);
                    }
                    (c, owl::hasKey, u) => {
                        let property_list = list(u)
                            .iter()
                            .map(|&pi| &properties[&canon_property.find(pi)])
                            .collect::<Vec<_>>();
                        prp_key(property_list, &classes[&c], &mut same_as);
                    }
                    (c, owl::intersectionOf, x) => {
                        let mut target_class = classes.remove(&c).unwrap();
                        let mut class_list = list(x)
                            .iter()
                            .map(|&ci| canon_class.find(ci))
                            .collect::<Vec<_>>();
                        class_list.sort_unstable();
                        class_list.dedup();
                        if !class_list.contains(&c) {
                            let class_list = class_list.iter().map(|&ci| &classes[&ci]).collect();
                            cls_int1(class_list, &mut target_class);
                        }
                        classes.insert(c, target_class);
                    }
                    (p1, owl::inverseOf, p2) if p1 != p2 => {
                        let property1 = properties.remove(&p1).unwrap();
                        let mut property2 = properties.remove(&p2).unwrap();
                        prp_inv1(&property1, &mut property2);
                        properties.insert(p1, property1);
                        properties.insert(p2, property2);
                    }
                    (x, owl::maxQualifiedCardinality, xml::nonNegativeInteger_1) => {
                        let p = triples
                            .iter()
                            .find_map(|&triple| match triple {
                                (x_search, owl::onProperty, p) if x_search == x => Some(p),
                                _ => None,
                            })
                            .unwrap();
                        let c = triples
                            .iter()
                            .find_map(|&triple| match triple {
                                (x_search, owl::onClass, c) if x_search == x => Some(c),
                                _ => None,
                            })
                            .unwrap();
                        cls_maxqc3(&properties[&p], &classes[&x], &classes[&c], &mut same_as);
                    }
                    (p, owl::propertyChainAxiom, x) => {
                        let mut target_property = properties.remove(&p).unwrap();
                        let mut property_list = list(x)
                            .iter()
                            .map(|&pi| canon_property.find(pi))
                            .collect::<Vec<_>>();
                        property_list.sort_unstable();
                        property_list.dedup();
                        if !property_list.contains(&p) {
                            let property_list =
                                property_list.iter().map(|&pi| &properties[&pi]).collect();
                            prp_spo2(property_list, &mut target_property);
                        }
                        properties.insert(p, target_property);
                    }
                    (x, owl::someValuesFrom, y) => {
                        let mut target_class = classes.remove(&x).unwrap();
                        let p = triples
                            .iter()
                            .find_map(|&triple| match triple {
                                (x_search, owl::onProperty, p) if x_search == x => Some(p),
                                _ => None,
                            })
                            .unwrap();
                        if y == canon_class.find(owl::Thing) {
                            // TODO: Handle via PRP-DOM
                            cls_svf2(&properties[&p], &mut target_class);
                        } else {
                            cls_svf1(&properties[&p], &classes[&y], &mut target_class);
                        }
                        classes.insert(x, target_class);
                    }
                    _ => (),
                }
            }

            let class_inputs = {
                abox.flat_map(|(s, p, o)| {
                    if let Ok(p) = TryInto::<u32>::try_into(p) {
                        if let Ok(x) = TryInto::<u32>::try_into(s) {
                            if let Ok(c) = TryInto::<u32>::try_into(o) {
                                return match p {
                                    rdf::r#type => Some((c, x)),
                                    _ => None,
                                };
                            }
                        }
                    }
                    None
                })
            };

	    let class_bypass =
            {
                let c_map = classes
                    .keys()
                    .enumerate()
                    .map(|(i, &c)| (c, TryInto::<u64>::try_into(i).unwrap()))
                    .collect::<BTreeMap<_, _>>();

		let partition_count = TryInto::<u64>::try_into(classes.len()).unwrap() + 1;
                let mut partitioned_class_inputs = class_inputs.inner.partition(
		    partition_count,
                    // TODO: Consider doing diff-conversion somewhere else
                    move |((c, x), timestamp, _diff)|
		    match c_map.get(&c) {
			Some(&mapped_c) => (mapped_c, ((x, c), timestamp, Present)),
			    None => {
				println!("class_partitioning_issue: c={}, x={}", c, x);
				(partition_count - 1, ((x, c), timestamp, Present))
			    }
			}
                );

		let class_bypass = partitioned_class_inputs.pop();

                for (class, input) in classes
                    .values_mut()
                    .zip(partitioned_class_inputs.into_iter())
                {
                    class.add(input.as_collection().map(|(x, _c)| x).enter(inner));
                }
		class_bypass
            };

            let property_inputs = {
                abox.flat_map(|(s, p, o)| {
                    if let Ok(p) = TryInto::<u32>::try_into(p) {
                        if let Ok(s) = TryInto::<u32>::try_into(s) {
                            if let Ok(o) = TryInto::<u32>::try_into(o) {
                                return match p {
                                    0..=crate::constants::MAX_CONST => None,
                                    p => Some((p, (s, o))),
                                };
                            }
                        }
                    }
                    None
                })
            };

            {
                let p_map = properties
                    .keys()
                    .enumerate()
                    .map(|(i, &c)| (c, TryInto::<u64>::try_into(i).unwrap()))
                    .collect::<BTreeMap<_, _>>();

                let partitioned_class_inputs = property_inputs.inner.partition(
                    properties.len().try_into().unwrap(),
                    // TODO: Consider doing diff-conversion somewhere else
                    move |((p, (s, o)), timestamp, _diff)| {
			match p_map.get(&p) {
			    Some(&mapped_p) =>       (mapped_p, ((s, o), timestamp, Present)),
			    None => {
					println!("property_partitioning_issue: s={}, p={}, o={}", s, p, o);
				(*p_map.iter().next().unwrap().1, ((s, o), timestamp, Present))
			    }
			}
                    },
                );

                for (property, input) in properties
                    .values_mut()
                    .zip(partitioned_class_inputs.into_iter())
                {
                    property.add(input.as_collection().enter(inner));
                }
            }

            let same_as_input = {
                abox.flat_map(|(s, p, o)| {
                    if let Ok(p) = TryInto::<u32>::try_into(p) {
                        if let Ok(x) = TryInto::<u32>::try_into(s) {
                            if let Ok(y) = TryInto::<u32>::try_into(o) {
                                return match p {
                                    owl::sameAs => Some((x, y)),
                                    _ => None,
                                };
                            }
                        }
                    }
                    None
                })
                .inner
                .map(|(d, t, _r)| (d, t, Present))
                .as_collection()
                .enter(inner)
            };

            {
                let mut class_feedback_streams: BTreeMap<_, Vec<_>> = BTreeMap::new();

                for (id, class) in classes.iter() {
                    if let Some(target_ids) = sco.get(id) {
                        for target_id in target_ids.iter() {
                            if id != target_id {
                                class_feedback_streams
                                    .entry(target_id)
                                    .or_default()
                                    .extend_from_slice(&class.feedback_);
                            }
                        }
                    }
                    class_feedback_streams
                        .entry(id)
                        .or_default()
                        .extend_from_slice(&class.feedback_);
                }

                for (id, input_streams) in class_feedback_streams.into_iter() {
                    class_feedback.remove(id).map(|feedback_handle| {
                        feedback_handle.set(
                            &concatenate(inner, input_streams)
                                .consolidate()
//                                .consolidate_stream_aggressively()
                                .leave()
                                .consolidate(),
                        );
                    });
                }
            }

            for (id, property) in properties.iter() {
                property_feedback.remove(id).map(|feedback_handle| {
                    feedback_handle.set(
                        &concatenate(inner, property.feedback_.clone())
                            .consolidate()
//                            .consolidate_stream_aggressively()
                            .leave()
                            .consolidate(),
                    );
                });
            }

            same_as_feedback.set(
                &concatenate(
                    inner,
                    same_as.feedback_.into_iter().chain(Some(same_as_input)),
                )
                    .consolidate()
//                .consolidate_stream_aggressively()
                .leave()
                .consolidate(),
            );

            /*
                    abox.inner.partition(
                        classes.len().try_into().unwrap(),
                        |((s, p, o), t, diff)| match p.clone().try_into() {
                            Ok(rdf::r#type) => match o.clone().try_into() {
                                Ok(_o1) => (o.clone().into(), ((s, p, o), t, diff)),
                                Err(_) => (0, ((s, p, o), t, diff)),
                            },
                            Ok(p1) => (p1.into(), ((s, p, o), t, diff)),
                            Err(_) => (0, ((s, p, o), t, diff)),
                        },
                    );
            */
            concatenate(
                &mut scope,
                properties
                    .iter()
                    .map(|(&p, property)| property.stream().leave().map(move |(s, o)| (s, p, o)))
                    .chain(classes.iter().map(|(&c, class)| {
                        class.stream().leave().map(move |x| (x, rdf::r#type, c))
                    }))
                    .chain(class_bypass.map(|class_bypass| class_bypass.as_collection().map(|(x, c)| (x, rdf::r#type, c)))),
            )
        });
    Ok(output)
}

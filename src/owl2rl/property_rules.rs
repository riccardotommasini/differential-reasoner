use differential_dataflow::{lattice::Lattice, ExchangeData};
use dogsdogsdogs::{ProposeExtensionMethod};
use timely::{
    dataflow::{Scope, ScopeParent},
    progress::Timestamp,
};

use crate::owl2rl::{AltNeu, IRI};

use super::{Class, Property, SameAs};

pub(crate) fn prp_dom<G, T>(property: &Property<G, T>, class: &mut Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    class.add(
        property
            .stream()
            .map(|(x, _y)| x)
            .inspect_batch(|capability, records| {
                println!(
                    "prp_dom: capability={:?}, records: {:?}",
                    capability, records
                );
            }),
    );
}

pub(crate) fn prp_rng<G, T>(property: &Property<G, T>, class: &mut Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    class.add(
        property
            .stream()
            .map(|(_x, y)| y)
            .inspect_batch(|capability, records| {
                println!(
                    "prp_rng: capability={:?}, records: {:?}",
                    capability, records
                );
            }),
    );
}

/*
T(?p, rdf:type, owl:FunctionalProperty)
T(?x, ?p, ?y1)
T(?x, ?p, ?y2)
=>
T(?y1, owl:sameAs, ?y2)
 */

pub(crate) fn prp_fp<G, T>(property: &Property<G, T>, same_as: &mut SameAs<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let derived = property
        .stream()
        .propose_using(&mut property.by_s_alt().extend_using(|&(x, _y1)| x))
        .map(|((_x, y1), y2)| (y1, y2))
        .inspect_batch(|capability, records| {
            println!(
                "prp_fp: capability={:?}, records: {:?}",
                capability, records
            );
        });
    same_as.add(derived);
}

/*
T(?p, rdf:type, owl:InverseFunctionalProperty)
T(?x1, ?p, ?y)
T(?x2, ?p, ?y)
=>
T(?x1, owl:sameAs, ?x2)
 */
pub(crate) fn prp_ifp<G, T>(property: &Property<G, T>, same_as: &mut SameAs<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let derived = property
        .stream()
        .propose_using(&mut property.by_o_alt().extend_using(|&(_x1, y)| y))
        .map(|((x1, _y), x2)| (x1, x2))
        .inspect_batch(|capability, records| {
            println!(
                "prp_ifp: capability={:?}, records: {:?}",
                capability, records
            );
        });
    same_as.add(derived);
}

/*
T(?p, rdf:type, owl:SymmetricProperty)
T(?x, ?p, ?y)
=>
T(?y, ?p, ?x)
*/
pub(crate) fn prp_symp<G, T>(property: &mut Property<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let derived = property
        .stream()
        .map(|(x, y)| (y, x))
        .inspect_batch(|capability, records| {
            println!(
                "prp_symp: capability={:?}, records: {:?}",
                capability, records
            );
        });
    property.add(derived);
}

/*
T(?p, rdf:type, owl:TransitiveProperty)
T(?x, ?p, ?y)
T(?y, ?p, ?z)
=>
T(?x, ?p, ?z)
 */
pub(crate) fn prp_trp<G, T>(property: &mut Property<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let d_xpy = property
        .stream()
        .propose_using(&mut property.by_s_alt().extend_using(|&(_x, y)| y))
        .map(|((x, _y), z)| (x, z))
        .inspect_batch(|capability, records| {
            println!(
                "prp_trp_dxpy: capability={:?}, records: {:?}",
                capability, records
            );
        });

    let d_ypz = property
        .stream()
        .propose_using(&mut property.by_o_alt().extend_using(|&(y, _z)| y))
        .map(|((_y, z), x)| (x, z))
        .inspect_batch(|capability, records| {
            println!(
                "prp_trp_dxpy: capability={:?}, records: {:?}",
                capability, records
            );
        });

    property.add(d_xpy);
    property.add(d_ypz);
}

/*
T(?p1, rdfs:subPropertyOf, ?p2)
T(?x, ?p1, ?y)
=>
T(?x, ?p2, ?y)
 */
pub(crate) fn prp_spo1<G, T>(property1: &Property<G, T>, property2: &mut Property<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    property2.add(
        property1
            .stream()
            .clone()
            .inspect_batch(|capability, records| {
                println!(
                    "prp_spo1: capability={:?}, records: {:?}",
                    capability, records
                );
            }),
    );
}

/*
T(?p, owl:propertyChainAxiom, ?x)
LIST[?x, ?p1, ..., ?pn]
T(?u1, ?p1, ?u2)
T(?u2, ?p2, ?u3)
...
T(?un, ?pn, ?un+1)
=>
T(?u1, ?p, ?un+1)
 */
pub(crate) fn prp_spo2<G, T>(
    property_chain: Vec<&Property<G, T>>,
    target_property: &mut Property<G, T>,
) where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    for (i, delta_relation) in property_chain.iter().enumerate() {
        let mut d_prop_i = delta_relation.stream().clone();

        // Handle earlier links in the chain
        for j in (0..i).rev() {
            d_prop_i = d_prop_i
                .propose_using(
                    &mut property_chain[j]
                        .by_o_alt()
                        .extend_using(|&(u_j_plus1, _u_i_plus1)| u_j_plus1),
                )
                .map(|((_u_j_plus1, u_i_plus1), u_j)| (u_j, u_i_plus1));
        }

        #[allow(clippy::needless_range_loop)]
        // Handle later links in the chain
        for j in (i + 1)..property_chain.len() {
            d_prop_i = d_prop_i
                .propose_using(
                    &mut property_chain[j]
                        .by_s_neu()
                        .extend_using(|&(_u_0, u_j)| u_j),
                )
                .map(|((u_0, _u_j), u_j_plus1)| (u_0, u_j_plus1));
        }

        let i = i;
        target_property.add(d_prop_i.inspect_batch(move |capability, records| {
            println!(
                "prp_spo2: i={}, capability={:?}, records: {:?}",
                i, capability, records
            );
        }));
    }
}

/*
T(?p1, owl:inverseOf, ?p2)
T(?x, ?p1, ?y)
=>
T(?y, ?p2, ?x)
*/
pub(crate) fn prp_inv1<G, T>(property1: &Property<G, T>, property2: &mut Property<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let derived = property1
        .stream()
        .map(|(x, y)| (y, x))
        .inspect_batch(|capability, records| {
            println!(
                "prp_inv1: capability={:?}, records: {:?}",
                capability, records
            );
        });
    property2.add(derived);
}

#[allow(dead_code)]
/*
T(?p1, owl:inverseOf, ?p2)
T(?x, ?p2, ?y)
=>
T(?y, ?p1, ?x)
*/
pub(crate) fn prp_inv2<G, T>(property1: &mut Property<G, T>, property2: &Property<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let derived = property2
        .stream()
        .map(|(x, y)| (y, x))
        .inspect_batch(|capability, records| {
            println!(
                "prp_inv2: capability={:?}, records: {:?}",
                capability, records
            );
        });
    property1.add(derived);
    // IGNORE; Hangled by TBox expansion via dedup
}

/*
T(?c, owl:hasKey, ?u)
LIST[?u, ?p1, ..., ?pn]
T(?x, rdf:type, ?c)
T(?x, ?p1, ?z1)
...
T(?x, ?pn, ?zn)
T(?y, rdf:type, ?c)
T(?y, ?p1, ?z1)
...
T(?y, ?pn, ?zn)
=>
T(?x, owl:sameAs, ?y)
 */
pub(crate) fn prp_key<G, T>(
    property_list: Vec<&Property<G, T>>,
    class: &Class<G, T>,
    same_as: &mut SameAs<G, T>,
) where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    assert_eq!(
        1,
        property_list.len(),
        "Multi-property keys not yet supported"
    );

    let p0 = property_list[0];

    let d_xp0 = {
        p0.stream()
            .propose_using(&mut class.extender_alt().extend_using(|&(x, _z0)| x))
            .map(|((x, z0), ()): ((IRI, IRI), ())| (x, z0))
    };

    /*
    let d_xp0_ = lookup_map(
        p0.stream(),
        class.alt().clone(),
        move |&(x, _z0), key| {
            *key = x;
        },
        |&(x, z0), _diff, _, _sum| ((x, z0), Present),
        Default::default(),
        Default::default(),
        Default::default(),
    );
    */

    let d_xc = {
        class
            .stream()
            .propose_using(&mut p0.by_s_alt().extend_using(|&x| x))
            .concat(&d_xp0)
            .propose_using(&mut p0.by_o_alt().extend_using(|&(_x, z0)| z0))
            .flat_map(|((x, _z0), y)| if x == y { None } else { Some((y, x)) })
            .propose_using(&mut class.extender_alt().extend_using(|&(y, _x)| y))
            .map(|((y, x), ()): ((IRI, IRI), ())| (std::cmp::min(x, y), std::cmp::max(x, y)))
    };
    /*
    let d_xc = lookup_map(
        &d_xc,
        class.alt().clone(),
        move |&((_x, _z0), y), key| {
            *key = y;
        },
        |&((x, _z0), y), _diff, _, _sum| ((x, y), Present),
        Default::default(),
        Default::default(),
        Default::default(),
    );
    */

    same_as.add(d_xc.inspect_batch(|capability, records| {
        println!(
            "prp_key: capability={:?}, records: {:?}",
            capability, records
        );
    }));
}

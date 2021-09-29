use std::cmp::Ordering::{Equal, Greater, Less};

use differential_dataflow::{collection::concatenate, lattice::Lattice, ExchangeData};
use dogsdogsdogs::{
    altneu::AltNeu, PrefixExtender, ProposeExtensionMethod, ValidateExtensionMethod,
};
use timely::{
    dataflow::{Scope, ScopeParent},
    progress::Timestamp,
};

use crate::owl2rl::IRI;

use super::{Class, Property, SameAs};

/*
T(?c, owl:intersectionOf, ?x)
LIST[?x, ?c1, ..., ?cn]
T(?y, rdf:type, ?c1)
T(?y, rdf:type, ?c2)
...
T(?y, rdf:type, ?cn)
=>
T(?y, rdf:type, ?c)
*/
pub(crate) fn cls_int1<G, T>(class_list: Vec<&Class<G, T>>, target_class: &mut Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let extenders = class_list
        .iter()
        .map(|class| class.extender_alt().extend_using(move |&x| x))
        .collect::<Vec<_>>();

    let mut matches = concatenate(
        &mut target_class.stream().scope(),
        class_list.iter().map(|class| class.stream()).cloned(),
    )
    .map(|y| (y, ()));

    for mut validator in extenders {
        matches = matches.validate_using(&mut validator);
    }

    target_class.add(matches.map(|(x, ())| x));
}

#[allow(dead_code)]
/*
T(?c, owl:intersectionOf, ?x)
LIST[?x, ?c1, ..., ?cn]
T(?y, rdf:type, ?c)
=>
T(?y, rdf:type, ?c1)
T(?y, rdf:type, ?c2)
...
T(?y, rdf:type, ?cn)
 */
pub(crate) fn cls_int2<G, T>(mut class_list: Vec<&mut Class<G, T>>, target_class: &Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    for ref mut c_i in class_list.iter_mut() {
        c_i.add(target_class.stream().clone());
    }
    // IGNORE; Handled by TBox expansion via sco
}

// cls-uni
// IGNORE; Handled by TBox expansion via sco

/*
T(?x, owl:someValuesFrom, ?y)
T(?x, owl:onProperty, ?p)
T(?u, ?p, ?v)
T(?v, rdf:type, ?y)
=>
T(?u, rdf:type, ?x)
 */
pub(crate) fn cls_svf1<G, T>(property: &Property<G, T>, class: &Class<G, T>, target_class: &mut Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    target_class.add(
        property
            .stream()
            .propose_using(&mut class.extender_neu().extend_using(|&(_u, v)| v))
            .map(|((u, _v), ())| u),
    );
    target_class.add(
        class
            .stream()
            .propose_using(&mut property.by_o_alt().extend_using(|&v| v))
            .map(|(_v, u)| u),
    );
}

/*
T(?x, owl:someValuesFrom, owl:Thing)
T(?x, owl:onProperty, ?p)
T(?u, ?p, ?v)
=>
T(?u, rdf:type, ?x)
 */
pub(crate) fn cls_svf2<G, T>(property: &Property<G, T>, target_class: &mut Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    target_class.add(property.stream().map(|(u, _v)| u));
}

/*
T(?x, owl:allValuesFrom, ?y)
T(?x, owl:onProperty, ?p)
T(?u, rdf:type, ?x)
T(?u, ?p, ?v)
=>
T(?v, rdf:type, ?y)
 */
pub(crate) fn cls_avf<G, T>(class: &Class<G, T>, property: &Property<G, T>, target_class: &mut Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    target_class.add(
        class
            .stream()
            .propose_using(&mut property.by_s_neu().extend_using(|&u| u))
            .map(|(_u, v)| v),
    );
    target_class.add(
        property
            .stream()
            .propose_using(&mut class.extender_alt().extend_using(|&(u, _v)| u))
            .map(|((_u, v), ())| v),
    );
}

/*
T(?x, owl:hasValue, ?y)
T(?x, owl:onProperty, ?p)
T(?u, rdf:type, ?x)
=>
T(?u, ?p, ?y)
 */
pub(crate) fn cls_hv1<G, T>(value: &[IRI], class: &Class<G, T>, property: &mut Property<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let value = value.to_owned();
    let value: &'static [u32] = value.leak(); // TODO: use Box::leak and attach a destructor to the dataflow to free the allocation
    property.add(
        class
            .stream()
            .flat_map(move |u| value.iter().map(move |&y| (u, y))),
    );
}

/*
T(?x, owl:hasValue, ?y)
T(?x, owl:onProperty, ?p)
T(?u, ?p, ?y)
=>
T(?u, rdf:type, ?x)
*/
pub(crate) fn cls_hv2<G, T>(value: &[IRI], property: &Property<G, T>, class: &mut Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let mut value = value.to_owned();
    #[allow(clippy::stable_sort_primitive)]
    value.sort();
    let value: &'static [u32] = value.leak(); // TODO: use Box::leak and attach a destructor to the dataflow to free the allocation
    class.add(
        property
            .stream()
            .flat_map(move |(u, y)| match value.binary_search(&y) {
                Ok(_) => Some(u),
                Err(_) => None,
            }),
    );
}

/*
T(?x, owl:maxCardinality, "1"^^xsd:nonNegativeInteger)
T(?x, owl:onProperty, ?p)
T(?u, ?p, ?y1)
T(?u, ?p, ?y2)
T(?u, rdf:type, ?x)
=>
T(?y1, owl:sameAs, ?y2)
 */
pub(crate) fn cls_maxc2<G, T>(property: &Property<G, T>, class: &Class<G, T>, same_as: &mut SameAs<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let d_ux = {
        let mut derived = property.by_s_alt().extend_using(|&u| u);
        let prefixes = class.stream().map(|u| (u, 1 << 31, 0));
        derived
            .count(&prefixes, 1)
            .flat_map(|(u, count, _index)| if count > 1 { Some(u) } else { None })
            .propose_using(&mut property.by_s_alt().extend_using(|&u| u))
            .propose_using(&mut property.by_s_alt().extend_using(|&(u, _y1)| u))
            .flat_map(|((_u, y1), y2)| match y1.cmp(&y2) {
                Less => Some((y1, y2)),
                Equal | Greater => None,
            })
    };
    let d_upy = {
        property
            .stream()
            .propose_using(&mut class.extender_neu().extend_using(|&(u, _y2)| u))
            .propose_using(&mut property.by_s_alt().extend_using(|&((u, _y2), ())| u))
            .flat_map(|(((_u, y2), ()), y1)| match y1.cmp(&y2) {
                Less => Some((y1, y2)),
                Equal | Greater => None,
            })
    };
    same_as.add(d_ux);
    same_as.add(d_upy);
}

#[allow(clippy::type_complexity)]
/*
T(?x, owl:maxQualifiedCardinality, "1"^^xsd:nonNegativeInteger)
T(?x, owl:onProperty, ?p)
T(?x, owl:onClass, ?c)
T(?u, ?p, ?y1)
T(?u, ?p, ?y2)
T(?y1, rdf:type, ?c)
T(?y2, rdf:type, ?c)
T(?u, rdf:type, ?x)
=>
T(?y1, owl:sameAs, ?y2)
 */
pub(crate) fn cls_maxqc3<G, T>(
    property: &Property<G, T>,
    class_x: &Class<G, T>,
    class_c: &Class<G, T>,
    same_as: &mut SameAs<G, T>,
) where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    let d_ux = {
        let mut derived = property.by_s_alt().extend_using(|&u| u);
        let prefixes = class_x.stream().map(|u| (u, 1 << 31, 0));
        derived
            .count(&prefixes, 1)
            .flat_map(|(u, count, _index)| if count > 1 { Some(u) } else { None })
            .propose_using(&mut property.by_s_alt().extend_using(|&u: &IRI| u))
            .propose_using(
                &mut class_c
                    .extender_alt()
                    .extend_using(|&(_u, y1): &(IRI, IRI)| y1),
            )
            .propose_using(
                &mut property
                    .by_s_alt()
                    .extend_using(|&((u, _y1), ()): &((IRI, IRI), ())| u),
            )
            .propose_using(
                &mut class_c
                    .extender_alt()
                    .extend_using(|&(((_u, _y1), ()), y2): &(((IRI, IRI), ()), IRI)| y2),
            )
            .flat_map(
                |((((_u, y1), ()), y2), ()): ((((IRI, IRI), ()), IRI), ())| match y1.cmp(&y2) {
                    Less => Some((y1, y2)),
                    Equal | Greater => None,
                },
            )
    };
    let d_upy =
        {
            property
                .stream()
                .propose_using(
                    &mut class_x
                        .extender_neu()
                        .extend_using(|&(u, _y2): &(IRI, IRI)| u),
                )
                .propose_using(
                    &mut class_c
                        .extender_alt()
                        .extend_using(|&((_u, y1), ()): &((IRI, IRI), ())| y1),
                )
                .propose_using(
                    &mut property
                        .by_s_alt()
                        .extend_using(|&(((u, _y2), ()), ()): &(((IRI, IRI), ()), ())| u),
                )
                .propose_using(&mut class_c.extender_alt().extend_using(
                    |&((((_u, _y2), ()), ()), y1): &((((IRI, IRI), ()), ()), IRI)| y1,
                ))
                .flat_map(
                    |(((((_u, y2), ()), ()), y1), ()): (((((IRI, IRI), ()), ()), IRI), ())| match y1
                        .cmp(&y2)
                    {
                        Less => Some((y1, y2)),
                        Equal | Greater => None,
                    },
                )
        };
    let d_yc = {
        class_c
            .stream()
            .propose_using(&mut property.by_o_alt().extend_using(|&y1| y1))
            .propose_using(
                &mut class_x
                    .extender_neu()
                    .extend_using(|&(_y1, u): &(IRI, IRI)| u),
            )
            .propose_using(
                &mut property
                    .by_s_alt()
                    .extend_using(|&((_y1, u), ()): &((IRI, IRI), ())| u),
            )
            .propose_using(
                &mut class_c
                    .extender_alt()
                    .extend_using(|&(((_y1, _u), ()), y2): &(((IRI, IRI), ()), IRI)| y2),
            )
            .flat_map(
                |((((y1, _u), ()), y2), ()): ((((IRI, IRI), ()), IRI), ())| match y1.cmp(&y2) {
                    Less => Some((y1, y2)),
                    Equal | Greater => None,
                },
            )
    };
    same_as.add(d_ux);
    same_as.add(d_upy);
    same_as.add(d_yc);
}

// cls-maxqc4
// IGNORE; Handled by TBox expansion via cls-maxc2

/*
T(?c, owl:oneOf, ?x)
LIST[?x, ?y1, ..., ?yn]
=>
T(?y1, rdf:type, ?c)
...
T(?yn, rdf:type, ?c)
 */
pub(crate) fn cls_oo<G, T>(mut class_list: Vec<&mut Class<G, T>>, target_class: &Class<G, T>)
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    for ref mut c_i in class_list.iter_mut() {
        c_i.add(target_class.stream().clone());
    }
}

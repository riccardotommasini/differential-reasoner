pub(crate) mod class_rules;
pub(crate) mod property_rules;
use std::lazy::OnceCell;

use differential_dataflow::operators::arrange::{arrangement::ArrangeBySelf, Arranged, TraceAgent};
use differential_dataflow::{
    difference::Present, lattice::Lattice, trace::implementations::ord::OrdKeySpine, Collection,
    ExchangeData,
};

use dogsdogsdogs::altneu::AltNeu;
use dogsdogsdogs::CollectionIndex;
use timely::{
    dataflow::{Scope, ScopeParent},
    progress::Timestamp,
};

#[allow(clippy::upper_case_acronyms)]
pub(crate) type IRI = u32;
pub(crate) type Time = u64;
pub(crate) type Diff = Present;
pub(crate) type SingleArrangement<G> =
    Arranged<G, TraceAgent<OrdKeySpine<IRI, <G as ScopeParent>::Timestamp, Diff>>>;

pub(crate) type DoubleIndex<G> = CollectionIndex<IRI, IRI, <G as ScopeParent>::Timestamp, Diff>;
pub(crate) type SingleIndex<G> = CollectionIndex<IRI, (), <G as ScopeParent>::Timestamp, Diff>;

pub(crate) struct Property<G, T>
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    by_s_alt_: OnceCell<DoubleIndex<G>>,
    by_o_alt_: OnceCell<DoubleIndex<G>>,
    by_s_neu_: OnceCell<DoubleIndex<G>>,
    by_o_neu_: OnceCell<DoubleIndex<G>>,
    stream_: Collection<G, (IRI, IRI), Diff>,
    feedback_: Vec<Collection<G, (IRI, IRI), Diff>>,
}

pub(crate) struct Class<G, T>
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    /*y_s_alt: DoubleIndex<G>,
    by_o_alt: DoubleIndex<G>,
    by_s_neu: DoubleIndex<G>,
    by_o_neu: DoubleIndex<G>,*/
    alt_: OnceCell<SingleArrangement<G>>,
    neu_: OnceCell<SingleArrangement<G>>,
    alt_extender_: OnceCell<SingleIndex<G>>,
    neu_extender_: OnceCell<SingleIndex<G>>,
    stream_: Collection<G, IRI, Diff>,
    feedback_: Vec<Collection<G, IRI, Diff>>,
}

pub(crate) struct SameAs<G, T>
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    /*y_s_alt: DoubleIndex<G>,
    by_o_alt: DoubleIndex<G>,
    by_s_neu: DoubleIndex<G>,
    by_o_neu: DoubleIndex<G>,*/
    alt_: OnceCell<DoubleIndex<G>>,
    neu_: OnceCell<DoubleIndex<G>>,
    stream_: Collection<G, (IRI, IRI), Diff>,
    feedback_: Vec<Collection<G, (IRI, IRI), Diff>>,
}

impl<G, T> Property<G, T>
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    fn by_s_alt(&self) -> &DoubleIndex<G> {
        self.by_s_alt_
            .get_or_init(|| CollectionIndex::index(&self.stream_))
    }

    fn by_o_alt(&self) -> &DoubleIndex<G> {
        self.by_o_alt_.get_or_init(|| {
            CollectionIndex::index(&self.stream_.map_in_place(|(x, y)| std::mem::swap(x, y)))
        })
    }

    fn by_s_neu(&self) -> &DoubleIndex<G> {
        self.by_s_neu_.get_or_init(|| {
            CollectionIndex::index(&self.stream_.delay(|t| {
                let mut t_neu = t.clone();
                t_neu.neu = true;
                t_neu
            }))
        })
    }

    fn by_o_neu(&self) -> &DoubleIndex<G> {
        self.by_o_neu_.get_or_init(|| {
            CollectionIndex::index(
                &self
                    .stream_
                    .delay(|t| {
                        let mut t_neu = t.clone();
                        t_neu.neu = true;
                        t_neu
                    })
                    .map_in_place(|(x, y)| std::mem::swap(y, x)),
            )
        })
    }

    fn stream(&self) -> &Collection<G, (IRI, IRI), Diff> {
        &self.stream_
    }

    fn add(&mut self, collection: Collection<G, (IRI, IRI), Diff>) {
        self.feedback_.push(collection);
    }
}

impl<G, T> Class<G, T>
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    fn add(&mut self, collection: Collection<G, IRI, Diff>) {
        self.feedback_.push(collection);
    }

    fn alt(&self) -> &SingleArrangement<G> {
        self.alt_.get_or_init(|| self.stream_.arrange_by_self())
    }

    fn neu(&self) -> &SingleArrangement<G> {
        self.neu_.get_or_init(|| {
            self.stream_
                .delay(|t| {
                    let mut t_neu = t.clone();
                    t_neu.neu = true;
                    t_neu
                })
                .arrange_by_self()
        })
    }

    fn extender_alt(&self) -> &SingleIndex<G> {
        self.alt_extender_
            .get_or_init(|| CollectionIndex::index(&self.stream_.map(|x| (x, ()))))
    }

    fn extender_neu(&self) -> &SingleIndex<G> {
        self.neu_extender_.get_or_init(|| {
            CollectionIndex::index(
                &self
                    .stream_
                    .delay(|t| {
                        let mut t_neu = t.clone();
                        t_neu.neu = true;
                        t_neu
                    })
                    .map(|x| (x, ())),
            )
        })
    }

    fn stream(&self) -> &Collection<G, IRI, Diff> {
        &self.stream_
    }
}

impl<G, T> SameAs<G, T>
where
    G: Scope,
    G: ScopeParent<Timestamp = AltNeu<T>>,
    T: Lattice + ExchangeData + Timestamp,
{
    fn alt(&self) -> &DoubleIndex<G> {
        self.alt_
            .get_or_init(|| CollectionIndex::index(&self.stream_))
    }

    fn neu(&self) -> &DoubleIndex<G> {
        self.neu_.get_or_init(|| {
            CollectionIndex::index(&self.stream_.delay(|t| {
                let mut t_neu = t.clone();
                t_neu.neu = true;
                t_neu
            }))
        })
    }

    fn add(&mut self, collection: Collection<G, (IRI, IRI), Diff>) {
        self.feedback_.push(collection);
    }
}

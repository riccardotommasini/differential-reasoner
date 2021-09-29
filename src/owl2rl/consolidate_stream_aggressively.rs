use std::cmp::Ord;
use std::collections::{hash_map, HashMap};

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::Collection;

use timely::communication::message::RefOrMut::{Mut, Ref};
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::Scope;

// Adapted from Differential Dataflow
/// An extension method for consolidating weighted streams.
pub trait ConsolidateStreamAggressive<D: timely::Data + Ord> {
    /// Aggregates the weights of equal records.
    ///
    /// Like `consolidate_stream`, this method does not exchange data and does not
    /// ensure that at most one copy of each `(data, time)` pair exists in the
    /// results. Unlike `consolidate_stream`, however, it acts on all available batches of
    /// data and collapses equivalent `(data, time)` pairs found therein, suppressing any
    /// that accumulate to zero.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::consolidate::ConsolidateStream;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///         // nothing to assert, as no particular guarantees.
    ///         x.negate()
    ///          .concat(&x)
    ///          .consolidate_stream();
    ///     });
    /// }
    /// ```
    fn consolidate_stream_aggressively(&self) -> Self;
}

impl<G: Scope, D, R> ConsolidateStreamAggressive<D> for Collection<G, D, R>
where
    D: timely::Data + Ord,
    R: Semigroup,
    G::Timestamp: Ord,
{
    fn consolidate_stream_aggressively(&self) -> Self {
        use differential_dataflow::collection::AsCollection;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::Operator;

        enum DirtyOrClean<D, T, R> {
            Dirty(Vec<(D, T, R)>),
            Clean(Vec<(D, T, R)>),
        }

        impl<D1, T1, R1> DirtyOrClean<D1, T1, R1>
        where
            D1: Ord,
            T1: Ord,
            R1: Semigroup,
        {
            fn modify<U, F>(&mut self, mut logic: F) -> U
            where
                F: FnMut(&mut Vec<(D1, T1, R1)>) -> U,
            {
                let inner = match std::mem::replace(self, DirtyOrClean::Clean(vec![])) {
                    DirtyOrClean::Dirty(dirty) => dirty,
                    DirtyOrClean::Clean(clean) => clean,
                };
                std::mem::swap(self, &mut DirtyOrClean::Dirty(inner));
                let inner = match self {
                    DirtyOrClean::Dirty(dirty) => dirty,
                    DirtyOrClean::Clean(clean) => clean,
                };
                logic(inner)
            }

            fn clean(&mut self) -> &mut Vec<(D1, T1, R1)> {
                match self {
                    DirtyOrClean::Dirty(dirty) => {
                        consolidate_updates(dirty);
                        dirty
                    }
                    DirtyOrClean::Clean(clean) => clean,
                }
            }
        }

        self.inner
            .unary_frontier(Pipeline, "ConsolidateStream", |_cap, _info| {
                let mut notificator = FrontierNotificator::new();
                let mut batches: HashMap<_, DirtyOrClean<D, _, _>> = HashMap::new();
                move |input, output| {
                    let frontier = &[input.frontier()];
                    input.for_each(
                        |time, mut data| match batches.entry(time.time().to_owned()) {
                            hash_map::Entry::Occupied(mut occupied) => {
                                match data {
                                    Ref(new) => {
                                        occupied.get_mut().modify(|stashed| {
                                            stashed.extend_from_slice(new);
                                        });
                                    }
                                    Mut(ref mut new) => {
                                        let mut cleaning_could_compact = match occupied.get() {
                                            DirtyOrClean::Dirty(_) => true,
                                            DirtyOrClean::Clean(_) => false,
                                        };
                                        let mut should_clean = false;
                                        occupied.get_mut().modify(|stashed| {
                                            if stashed.capacity() < new.capacity() {
                                                std::mem::swap(*new, stashed);
                                                cleaning_could_compact = true;
                                            }
                                            if (stashed.capacity() - stashed.len()) < new.len() {
                                                if cleaning_could_compact {
                                                    consolidate_updates(stashed);
                                                    cleaning_could_compact = false;
                                                }
                                                if (stashed.capacity() - stashed.len()) < new.len()
                                                {
                                                    stashed.reserve(
                                                        if new.len() * std::mem::size_of::<D>()
                                                            > 64 * 1024 * 1024
                                                        {
                                                            new.len()
                                                        } else {
                                                            new.len() * 2
                                                        },
                                                    );
                                                    if new.len() * 3 > stashed.len() {
                                                        should_clean = true;
                                                    }
                                                }
                                            }
                                            if new.len() * 3 > (stashed.len() * 2) {
                                                should_clean = true;
                                            }
                                            stashed.extend_from_slice(new);
                                        });
                                        if should_clean {
                                            occupied.get_mut().clean();
                                        }
                                    }
                                };
                            }
                            hash_map::Entry::Vacant(vacant) => {
                                match data {
                                    Ref(new) => {
                                        let mut stashed = Vec::with_capacity(new.len() * 2);
                                        stashed.extend_from_slice(new);
                                        notificator.notify_at(time.retain());
                                        consolidate_updates(&mut stashed);
                                        vacant.insert(DirtyOrClean::Clean(stashed));
                                    }
                                    Mut(new) => {
                                        notificator.notify_at(time.retain());
                                        vacant.insert(DirtyOrClean::Dirty(std::mem::take(new)));
                                    }
                                };
                            }
                        },
                    );
                    notificator.for_each(frontier, |capability, _| {
                        match batches.remove(capability.time()) {
                            Some(DirtyOrClean::Clean(ref mut data)) => {
                                output.session(&capability).give_vec(data);
                            }
                            Some(DirtyOrClean::Dirty(ref mut data)) => {
                                consolidate_updates(data);
                                output.session(&capability).give_vec(data);
                            }
                            _ => (),
                        }
                    })
                }
            })
            .as_collection()
    }
}

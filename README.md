## A `Friendly RDFS+` materalizer.

### Why is called `Friendly`?

It's nigh guaranteed that whenever someone tries to explain transitivity, they will give the "friendsOf" example i.e if a is friends with b and b is friends with c, then a is friends with c.

### What does it materialize?

All of `rhoDF` + `owl:inverseOf` + `owl:TransitiveProperty`.

### How to run:

```
cargo run --release -- -w8
```

### Over which kind of data does it reason?

It reasons over **already encoded** triples, at the moment, specifically, the [lubm](http://swat.cse.lehigh.edu/projects/lubm/) dataset.

It can be found, already encoded, and split by-worker(very dirty I know) at `/tests/data/update_data/`.

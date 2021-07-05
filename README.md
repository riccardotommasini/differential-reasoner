A `Friendly RDFS+` materalizer.

Why is called `Friendly`?

It's nigh guaranteed that whenever someone tries to explain transitivity, they will give the "friendsOf" example i.e if a is friends with b and b is friends with c, then a is friends with c.

All of `rhoDF` + `owl:inverseOf` + `owl:TransitiveProperty`.

How to run:

```
cargo run --release -- -w8
```

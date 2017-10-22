scoped-stateful-threadpool-rs
==============

A library for scoped and cached threadpools **that keep a state so that you don't need a connection pool**.

This library is a fork of [scoped_threadpool](https://crates.io/crates/scoped_threadpool),
with statefulness.

# Getting Started

[scoped-stateful-threadpool-rs is available on crates.io](https://crates.io/crates/scoped_stateful_threadpool).
Add the following dependency to your Cargo manifest to get the latest version of the 0.1 branch:
```toml
[dependencies]
scoped_stateful_threadpool = "0.1.*"
```
# Example

```rust
extern crate scoped_stateful_threadpool;
use scoped_stateful_threadpool::Pool;

fn main() {
    // Create a threadpool holding 4 threads and an initial state
    let mut pool = Pool::new(4, &|| 0);

    let mut vec = vec![0, 1, 2, 3, 4, 5, 6, 7];

    // Use the threads as scoped threads that can
    // reference anything outside this closure
    pool.scoped(|scoped| {
        // Create references to each element in the vector ...
        for e in &mut vec {
            // ... and add 1 to it in a seperate thread
            scoped.execute(move |state| {
                *state += 1; // I can change the state
                *e += 1;
            });
        }
    });

    assert_eq!(vec, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}
```

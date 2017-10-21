extern crate scoped_stateful_threadpool;

use scoped_stateful_threadpool::Pool;

#[test]
fn test_statefulness() {

    let mut pool = Pool::new(4, || 0);

    pool.scoped(|scope| {
        for _ in 0..100 {
            scope.execute(|counter| {
                *counter += 1;

                println!("thread {:?} was used {} times",
                    std::thread::current().id(),
                    *counter
                );
            });
        }
    });
}


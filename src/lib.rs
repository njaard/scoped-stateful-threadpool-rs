//! This crate provides a stable, safe and scoped threadpool.
//!
//! This crate differs from the original `scoped_threadpool` in that
//! each thread can keep a state. Specifically, this allows each thread
//! to hold a persistent database connection or a resource
//! with a costly setup.
//!
//! It can be used to execute a number of short-lived jobs in parallel
//! without the need to respawn the underlying threads.
//!
//! Jobs are runnable by borrowing the pool for a given scope, during which
//! an arbitrary number of them can be executed. These jobs can access data of
//! any lifetime outside of the pools scope, which allows working on
//! non-`'static` references in parallel.
//!
//! For safety reasons, a panic inside a worker thread will not be isolated,
//! but rather propagate to the outside of the pool.
//!
//! # Examples:
//!
//! ```rust
//! extern crate scoped_stateful_threadpool;
//! use scoped_stateful_threadpool::Pool;
//!
//! fn main() {
//!     // Create a threadpool holding 4 threads
//!     let mut pool = Pool::new(4, &|| 1);
//!
//!     let mut vec = vec![0, 1, 2, 3, 4, 5, 6, 7];
//!
//!     // Use the threads as scoped threads that can
//!     // reference anything outside this closure
//!     pool.scoped(|scope| {
//!         // Create references to each element in the vector ...
//!         for e in &mut vec {
//!             // ... and add 1 to it in a seperate thread
//!             scope.execute(move |v| {
//!                 *e += *v;
//!             });
//!         }
//!     });
//!
//!     assert_eq!(vec, vec![1, 2, 3, 4, 5, 6, 7, 8]);
//! }
//! ```

#![cfg_attr(all(feature="nightly", test), feature(test))]
#![cfg_attr(feature="nightly", feature(drop_types_in_const))]
#![cfg_attr(all(feature="nightly", test), feature(core_intrinsics))]
#![cfg_attr(feature="nightly", feature(const_fn))]
#![cfg_attr(feature="nightly", feature(const_unsafe_cell_new))]

#![warn(missing_docs)]

#[macro_use]
#[cfg(test)]
extern crate lazy_static;

use std::thread::{self, JoinHandle};
use std::sync::mpsc::{channel, Sender, Receiver, SyncSender, sync_channel, RecvError};
use std::sync::{Arc, Mutex};
use std::marker::PhantomData;
use std::mem;

enum Message<State> {
    NewJob(Thunk<'static, State>),
    Join,
}

trait FnBox<State> {
    fn call_box(self: Box<Self>, state : &mut State);
}

impl<F: FnOnce(&mut State), State> FnBox<State> for F {
    fn call_box(self: Box<F>, state : &mut State) {
        (*self)(state)
    }
}


type Thunk<'a, State> = Box<FnBox<State> + Send + 'a>;

impl<State> Drop for Pool<State> {
    fn drop(&mut self) {
        self.job_sender = None;
    }
}

/// A threadpool that acts as a handle to a number
/// of threads spawned at construction.
pub struct Pool<State> {
    threads: Vec<ThreadData>,
    job_sender: Option<Sender<Message<State>>>
}

struct ThreadData {
    _thread_join_handle: JoinHandle<()>,
    pool_sync_rx: Receiver<()>,
    thread_sync_tx: SyncSender<()>,
}

type StateCreatorFn<'a, State> = Box<Fn()-> State + 'a>;
struct StateGenerator<Val>(*const Val);

unsafe impl<Val> Send for StateGenerator<Val> {}
unsafe impl<Val> Sync for StateGenerator<Val> {}


impl<'pool, State> Pool<State>
    where State : 'static + Send {
    /// Construct a threadpool with the given number of threads.
    /// Minimum value is `1`.
    ///
    /// For each thread, call the build function and store its
    /// result. It will be passed to scoped closure.
    ///
    /// The build function is called from the current thread
    /// (and not the spawned threads).
    pub fn new<'sc, StateCreator>(n: u32, state_creator : &'sc StateCreator) -> Self
        where StateCreator : Fn() -> State {
        assert!(n >= 1);

        let state_creator = &unsafe {
            mem::transmute::<StateCreatorFn<'sc, State>, StateCreatorFn<'static, State>>(
                Box::new(state_creator)
            )
        } as *const StateCreatorFn<'static, State>;

        let (job_sender, job_receiver) = channel();
        let job_receiver = Arc::new(Mutex::new(job_receiver));

        let mut threads = Vec::with_capacity(n as usize);

        // spawn n threads, put them in waiting mode
        for _ in 0..n {
            let job_receiver = job_receiver.clone();

            let (pool_sync_tx, pool_sync_rx) =
                sync_channel::<()>(0);
            let (thread_sync_tx, thread_sync_rx) =
                sync_channel::<()>(0);

            let state_creator = StateGenerator(state_creator);
            let mut state = unsafe {(*state_creator.0)()};

            let thread = thread::spawn(move || {

                loop {
                    let message = {
                        // Only lock jobs for the time it takes
                        // to get a job, not run it.
                        let lock = job_receiver.lock().unwrap();
                        lock.recv()
                    };

                    match message {
                        Ok(Message::NewJob(job)) => {
                            job.call_box(&mut state);
                        }
                        Ok(Message::Join) => {
                            // Syncronize/Join with pool.
                            // This has to be a two step
                            // process to ensure that all threads
                            // finished their work before the pool
                            // can continue

                            // Wait until the pool started syncing with threads
                            if pool_sync_tx.send(()).is_err() {
                                // The pool was dropped.
                                break;
                            }

                            // Wait until the pool finished syncing with threads
                            if thread_sync_rx.recv().is_err() {
                                // The pool was dropped.
                                break;
                            }
                        }
                        Err(..) => {
                            // The pool was dropped.
                            break
                        }
                    }
                }
            });

            threads.push(ThreadData {
                _thread_join_handle: thread,
                pool_sync_rx: pool_sync_rx,
                thread_sync_tx: thread_sync_tx,
            });
        }

        Pool {
            threads: threads,
            job_sender: Some(job_sender),
        }
    }

    /// Borrows the pool and allows executing jobs on other
    /// threads during that scope via the argument of the closure.
    ///
    /// This method will block until the closure and all its jobs have
    /// run to completion.
    pub fn scoped<'scope, F, R>(&'pool mut self, f: F) -> R
        where F: FnOnce(&Scope<'pool, 'scope, State>) -> R
    {
        let scope = Scope {
            pool: self,
            _marker: PhantomData,
        };
        f(&scope)
    }

    /// Returns the number of threads inside this pool.
    pub fn thread_count(&self) -> u32 {
        self.threads.len() as u32
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Handle to the scope during which the threadpool is borrowed.
pub struct Scope<'pool, 'scope, State : 'pool> {
    pool: &'pool mut Pool<State>,
    // The 'scope needs to be invariant... it seems?
    _marker: PhantomData<::std::cell::Cell<&'scope mut ()>>,
}

impl<'pool, 'scope, State> Scope<'pool, 'scope, State> {
    /// Execute a job on the threadpool.
    ///
    /// The body of the closure will be send to one of the
    /// internal threads, and this method itself will not wait
    /// for its completion.
    pub fn execute<F>(&self, f: F) where F: FnOnce(&mut State) + Send + 'scope {
        self.execute_(f)
    }

    fn execute_<F>(&self, f: F) where F: FnOnce(&mut State) + Send + 'scope {
        let b = unsafe {
            mem::transmute::<Thunk<'scope, State>, Thunk<'static, State>>(Box::new(f))
        };
        self.pool.job_sender.as_ref().unwrap().send(Message::NewJob(b)).unwrap();
    }

    /// Blocks until all currently queued jobs have run to completion.
    pub fn join_all(&self) {
        for _ in 0..self.pool.threads.len() {
            self.pool.job_sender.as_ref().unwrap().send(Message::Join).unwrap();
        }

        // Synchronize/Join with threads
        // This has to be a two step process
        // to make sure _all_ threads received _one_ Join message each.

        // This loop will block on every thread until it
        // received and reacted to its Join message.
        let mut worker_panic = false;
        for thread_data in &self.pool.threads {
            if let Err(RecvError) = thread_data.pool_sync_rx.recv() {
                worker_panic = true;
            }
        }
        if worker_panic {
            // Now that all the threads are paused, we can safely panic
            panic!("Thread pool worker panicked");
        }

        // Once all threads joined the jobs, send them a continue message
        for thread_data in &self.pool.threads {
            thread_data.thread_sync_tx.send(()).unwrap();
        }
    }
}

impl<'pool, 'scope, State> Drop for Scope<'pool, 'scope, State> {
    fn drop(&mut self) {
        self.join_all();
    }
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    #![cfg_attr(feature="nightly", allow(unused_unsafe))]

    use super::Pool;
    use std::thread;
    use std::sync;
    use std::time;

    fn sleep_ms(ms: u64) {
        thread::sleep(time::Duration::from_millis(ms));
    }

    #[test]
    fn smoketest() {
        let mut pool = Pool::new(4, &|| "state");

        for i in 1..7 {
            let mut vec = vec![0, 1, 2, 3, 4];
            pool.scoped(|s| {
                for e in vec.iter_mut() {
                    s.execute(move |_| {
                        *e += i;
                    });
                }
            });

            let mut vec2 = vec![0, 1, 2, 3, 4];
            for e in vec2.iter_mut() {
                *e += i;
            }

            assert_eq!(vec, vec2);
        }
    }

    #[test]
    #[should_panic]
    fn thread_panic() {
        let mut pool = Pool::new(4, &|| "state");
        pool.scoped(|scoped| {
            scoped.execute(move |_| {
                panic!()
            });
        });
    }

    #[test]
    #[should_panic]
    fn scope_panic() {
        let mut pool = Pool::new(4, &|| "state");
        pool.scoped(|_scoped| {
            panic!()
        });
    }

    #[test]
    #[should_panic]
    fn pool_panic() {
        let _pool = Pool::new(4, &|| "state");
        panic!()
    }

    #[test]
    fn join_all() {
        let mut pool = Pool::new(4, &|| "state");

        let (tx_, rx) = sync::mpsc::channel();

        pool.scoped(|scoped| {
            let tx = tx_.clone();
            scoped.execute(move |_| {
                sleep_ms(1000);
                tx.send(2).unwrap();
            });

            let tx = tx_.clone();
            scoped.execute(move |_| {
                tx.send(1).unwrap();
            });

            scoped.join_all();

            let tx = tx_.clone();
            scoped.execute(move |_| {
                tx.send(3).unwrap();
            });
        });

        assert_eq!(rx.iter().take(3).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn join_all_with_thread_panic() {
        use std::sync::mpsc::Sender;
        struct OnScopeEnd(Sender<u8>);
        impl Drop for OnScopeEnd {
            fn drop(&mut self) {
                self.0.send(1).unwrap();
                sleep_ms(200);
            }
        }
        let (tx_, rx) = sync::mpsc::channel();
        // Use a thread here to handle the expected panic from the pool. Should
        // be switched to use panic::recover instead when it becomes stable.
        let handle = thread::spawn(move || {
            let mut pool = Pool::new(8, &|| "state");
            let _on_scope_end = OnScopeEnd(tx_.clone());
            pool.scoped(|scoped| {
                scoped.execute(move |_| {
                    sleep_ms(100);
                    panic!();
                });
                for _ in 1..8 {
                    let tx = tx_.clone();
                    scoped.execute(move |_| {
                        sleep_ms(200);
                        tx.send(0).unwrap();
                    });
                }
            });
        });
        if let Ok(..) = handle.join() {
            panic!("Pool didn't panic as expected");
        }
        // If the `1` that OnScopeEnd sent occurs anywhere else than at the
        // end, that means that a worker thread was still running even
        // after the `scoped` call finished, which is unsound.
        let values: Vec<u8> = rx.into_iter().collect();
        assert_eq!(&values[..], &[0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn safe_execute() {
        let mut pool = Pool::new(4, &|| "str");
        pool.scoped(|scoped| {
            scoped.execute(move |_| {
            });
        });
    }
}

#[cfg(all(test, feature="nightly"))]
mod benches {
    extern crate test;

    use self::test::{Bencher, black_box};
    use super::Pool;
    use std::sync::Mutex;

    // const MS_SLEEP_PER_OP: u32 = 1;

    lazy_static! {
        static ref POOL_1: Mutex<Pool> = Mutex::new(Pool::new(1));
        static ref POOL_2: Mutex<Pool> = Mutex::new(Pool::new(2));
        static ref POOL_3: Mutex<Pool> = Mutex::new(Pool::new(3));
        static ref POOL_4: Mutex<Pool> = Mutex::new(Pool::new(4));
        static ref POOL_5: Mutex<Pool> = Mutex::new(Pool::new(5));
        static ref POOL_8: Mutex<Pool> = Mutex::new(Pool::new(8));
    }

    fn fib(n: u64) -> u64 {
        let mut prev_prev: u64 = 1;
        let mut prev = 1;
        let mut current = 1;
        for _ in 2..(n+1) {
            current = prev_prev.wrapping_add(prev);
            prev_prev = prev;
            prev = current;
        }
        current
    }

    fn threads_interleaved_n(pool: &mut Pool)  {
        let size = 1024; // 1kiB

        let mut data = vec![1u8; size];
        pool.scoped(|s| {
            for e in data.iter_mut() {
                s.execute(move || {
                    *e += fib(black_box(1000 * (*e as u64))) as u8;
                    for i in 0..10000 { black_box(i); }
                    //sleep_ms(MS_SLEEP_PER_OP);
                });
            }
        });
    }

    #[bench]
    fn threads_interleaved_1(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_1.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_2(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_2.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_4(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_4.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_8(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_8.lock().unwrap()))
    }

    fn threads_chunked_n(pool: &mut Pool) {
        // Set this to 1GB and 40 to get good but slooow results
        let size = 1024 * 1024 * 10 / 4; // 10MiB
        let bb_repeat = 50;

        let n = pool.thread_count();
        let mut data = vec![0u32; size];
        pool.scoped(|s| {
            let l = (data.len() - 1) / n as usize + 1;
            for es in data.chunks_mut(l) {
                s.execute(move || {
                    if es.len() > 1 {
                        es[0] = 1;
                        es[1] = 1;
                        for i in 2..es.len() {
                            // Fibonnaci gets big fast,
                            // so just wrap around all the time
                            es[i] = black_box(es[i-1].wrapping_add(es[i-2]));
                            for i in 0..bb_repeat { black_box(i); }
                        }
                    }
                    //sleep_ms(MS_SLEEP_PER_OP);
                });
            }
        });
    }

    #[bench]
    fn threads_chunked_1(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_1.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_2(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_2.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_3(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_3.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_4(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_4.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_5(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_5.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_8(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_8.lock().unwrap()))
    }
}

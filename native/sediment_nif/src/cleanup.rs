//! Resource destructors run on whichever BEAM scheduler garbage collects the
//! term, so they must not block or do I/O. They hand their work to this
//! thread instead.

use std::sync::mpsc::{channel, Sender};
use std::sync::{Mutex, OnceLock};

type Job = Box<dyn FnOnce() + Send>;

static CLEANER: OnceLock<Mutex<Sender<Job>>> = OnceLock::new();

fn sender() -> &'static Mutex<Sender<Job>> {
    CLEANER.get_or_init(|| {
        let (tx, rx) = channel::<Job>();
        std::thread::Builder::new()
            .name("sediment_cleanup".to_string())
            .spawn(move || {
                for job in rx {
                    // A panicking cleanup must not take the thread down.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                }
            })
            .expect("spawn sediment cleanup thread");
        Mutex::new(tx)
    })
}

/// Runs `job` on the cleanup thread.
pub fn defer(job: impl FnOnce() + Send + 'static) {
    let tx = crate::conn::lock(sender()).clone();
    if let Err(err) = tx.send(Box::new(job)) {
        (err.0)();
    }
}

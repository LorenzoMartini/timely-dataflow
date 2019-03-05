//! Types and traits for sharing `Bytes`.

use std::thread::Thread;
use std::sync::{Arc, RwLock};
use std::collections::VecDeque;

use npnc::bounded::spsc;

use bytes::arc::Bytes;
use super::bytes_slab::BytesSlab;

/// A target for `Bytes`.
pub trait BytesPush {
    // /// Pushes bytes at the instance.
    // fn push(&mut self, bytes: Bytes);
    /// Pushes many bytes at the instance.
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iter: I);
}
/// A source for `Bytes`.
pub trait BytesPull {
    // /// Pulls bytes from the instance.
    // fn pull(&mut self) -> Option<Bytes>;
    /// Drains many bytes from the instance.
    fn drain_into(&mut self, vec: &mut Vec<Bytes>);
}

/// A signal appropriate to wake a single thread.
///
/// Internally this type uses thread parking and unparking, where the first thread to call
/// `wait` is registered as the thread to wake. Other threads that call `wait` will just be
/// parked without registering themselves, which would probably be a bug (of theirs).
#[derive(Clone)]
pub struct Signal {
    thread: Arc<RwLock<Option<Thread>>>,
}

impl Signal {
    /// Creates a new signal.
    pub fn new() -> Self {
        Signal { thread: Arc::new(RwLock::new(None)) }
    }
    /// Blocks unless or until ping is called.
    pub fn wait(&self) {
        // It is important not to block on the first call; doing so would fail to unblock
        // from pings before the first call to wait. This may appear as a spurious wake-up,
        // and ideally the caller is prepared for that.
        if self.thread.read().expect("failed to read thread").is_none() {
            *self.thread.write().expect("failed to set thread") = Some(::std::thread::current())
        }
        else {
            ::std::thread::park();
        }
    }
    /// Unblocks the current or next call to wait.
    pub fn ping(&self) {
        if let Some(thread) = self.thread.read().expect("failed to read thread").as_ref() {
            thread.unpark();
        }
    }
}

use std::sync::atomic::{AtomicBool, Ordering};

/// The receiver end of a merge queue.
pub struct MergeQueueProducer {
    producer: Option<spsc::Producer<Bytes>>,    // queue of bytes.
    dirty: Signal,                      // indicates whether there may be data present.
    panic: Arc<AtomicBool>,
    to_send: VecDeque<Bytes>,
}

/// The sender end of a merge queue.
pub struct MergeQueueConsumer {
    consumer: spsc::Consumer<Bytes>, // queue of bytes.
    panic: Arc<AtomicBool>,
    complete: bool,
}

/// Allocates a new unbounded queue of bytes intended for point-to-point communication
/// between threads.
///
/// TODO: explain "extend"
pub fn merge_queue(signal: Signal) -> (MergeQueueProducer, MergeQueueConsumer) {
    let (producer, consumer) = spsc::channel(512);
    let panic = Arc::new(AtomicBool::new(false));
    (MergeQueueProducer {
        producer: Some(producer),
        dirty: signal,
        panic: panic.clone(),
        to_send: VecDeque::with_capacity(32),
    },
     MergeQueueConsumer {
         consumer,
         panic,
         complete: false,
     })
}

impl MergeQueueConsumer {
    /// Indicates that all input handles to the queue have dropped.
    pub fn is_complete(&self) -> bool {
        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }
        self.complete
    }
}

impl BytesPush for MergeQueueProducer {
    fn extend<I: IntoIterator<Item=Bytes>>(&mut self, iterator: I) {

        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }

        let mut iterator = iterator.into_iter();

        if let Some(bytes) = iterator.next() {
            let mut tail = if let Some(mut tail) = self.to_send.pop_back() {
                if let Err(bytes) = tail.try_merge(bytes) {
                    self.to_send.push_back(::std::mem::replace(&mut tail, bytes));
                }
                tail
            }
            else {
                bytes
            };

            for bytes in iterator {
                if let Err(bytes) = tail.try_merge(bytes) {
                    self.to_send.push_back(::std::mem::replace(&mut tail, bytes));
                }
            }
            self.to_send.push_back(tail);
        }

        // Wakeup corresponding thread *after* releasing the lock
        if self.to_send.len() > 0 {
            for bytes in self.to_send.drain(..) {
                let mut sending = Some(bytes);
                while sending.is_some() {
                    match self.producer.as_mut().unwrap().produce(sending.take().unwrap()) {
                        Ok(()) => (),
                        Err(::npnc::ProduceError::Disconnected(_)) => panic!("MergeQueueConsumer disconnected"),
                        Err(::npnc::ProduceError::Full(item)) => { sending = Some(item) }, // try again
                    }
                }
            }
            self.dirty.ping();  // only signal from empty to non-empty.
        }
    }
}

impl BytesPull for MergeQueueConsumer {
    fn drain_into(&mut self, vec: &mut Vec<Bytes>) {
        if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }

        while match self.consumer.consume() {
            Ok(item) => { vec.push(item); true },
            Err(::npnc::ConsumeError::Disconnected) => { self.complete = true; false },
            Err(::npnc::ConsumeError::Empty) => { false },
        } {}
    }
}

// We want to ping in the drop because a channel closing can unblock a thread waiting on
// the next bit of data to show up.
impl Drop for MergeQueueProducer {
    fn drop(&mut self) {
        // Propagate panic information, to distinguish between clean and unclean shutdown.
        if ::std::thread::panicking() {
            self.panic.store(true, Ordering::SeqCst);
        }
        else {
            // TODO: Perhaps this aggressive ordering can relax orderings elsewhere.
            if self.panic.load(Ordering::SeqCst) { panic!("MergeQueue poisoned."); }
        }
        // Drop the queue before pinging.
        ::std::mem::drop(self.producer.take().unwrap());
        self.dirty.ping();
    }
}


/// A `BytesPush` wrapper which stages writes.
pub struct SendEndpoint<P: BytesPush> {
    send: P,
    buffer: BytesSlab,
}

impl<P: BytesPush> SendEndpoint<P> {

    /// Moves `self.buffer` into `self.send`, replaces with empty buffer.
    fn send_buffer(&mut self) {
        let valid_len = self.buffer.valid().len();
        if valid_len > 0 {
            self.send.extend(Some(self.buffer.extract(valid_len)));
        }
    }

    /// Allocates a new `BytesSendEndpoint` from a shared queue.
    pub fn new(queue: P) -> Self {
        SendEndpoint {
            send: queue,
            buffer: BytesSlab::new(20),
        }
    }
    /// Makes the next `bytes` bytes valid.
    ///
    /// The current implementation also sends the bytes, to ensure early visibility.
    pub fn make_valid(&mut self, bytes: usize) {
        self.buffer.make_valid(bytes);
        self.send_buffer();
    }
    /// Acquires a prefix of `self.empty()` of length at least `capacity`.
    pub fn reserve(&mut self, capacity: usize) -> &mut [u8] {

        if self.buffer.empty().len() < capacity {
            self.send_buffer();
            self.buffer.ensure_capacity(capacity);
        }

        assert!(self.buffer.empty().len() >= capacity);
        self.buffer.empty()
    }
    /// Marks all written data as valid, makes visible.
    pub fn publish(&mut self) {
        self.send_buffer();
    }
}

impl<P: BytesPush> Drop for SendEndpoint<P> {
    fn drop(&mut self) {
        self.send_buffer();
    }
}


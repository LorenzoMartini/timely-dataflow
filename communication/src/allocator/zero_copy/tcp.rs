//!
extern crate streaming_harness_hdrhist;
extern crate amd64_timer;
use std::io::{Read, Write};
use std::net::TcpStream;
use self::amd64_timer::ticks;
use networking::MessageHeader;

use super::bytes_slab::BytesSlab;
use super::bytes_exchange::{MergeQueue, Signal};

use logging_core::Logger;

use ::logging::{CommunicationEvent, CommunicationSetup, MessageEvent, StateEvent};
use std::io::Error;
use std::io::ErrorKind;
use std::io;

/// Repeatedly reads from a TcpStream and carves out messages.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
/// If the stream ends without being shut down, the receive thread panics in an attempt to
/// take down the computation and cause the failures to cascade.
pub fn recv_loop(
    mut reader: TcpStream,
    mut targets: Vec<MergeQueue>,
    worker_offset: usize,
    process: usize,
    remote: usize,
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>)
{
    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: true }));
    let mut buffer = BytesSlab::new(20);

    // Where we stash Bytes before handing them off.
    let mut stageds = Vec::with_capacity(targets.len());
    for _ in 0 .. targets.len() {
        stageds.push(Vec::new());
    }

    // Each loop iteration adds to `self.Bytes` and consumes all complete messages.
    // At the start of each iteration, `self.buffer[..self.length]` represents valid
    // data, and the remaining capacity is available for reading from the reader.
    //
    // Once the buffer fills, we need to copy uncomplete messages to a new shared
    // allocation and place the existing Bytes into `self.in_progress`, so that it
    // can be recovered once all readers have read what they need to.
    let mut active = true;

    while active {
        buffer.ensure_capacity(1);

        assert!(!buffer.empty().is_empty());

        // Attempt to read some more bytes into self.buffer.
        let read = match reader.read(&mut buffer.empty()) {
            Ok(n) => n,
            Err(x) => {
                // We don't expect this, as socket closure results in Ok(0) reads.
                println!("Error: {:?}", x);
                0
            },
        };

        assert!(read > 0);
        buffer.make_valid(read);
        // Consume complete messages from the front of self.buffer.
        while let Some(header) = MessageHeader::try_read(buffer.valid()) {

            // TODO: Consolidate message sequences sent to the same worker?
            let peeled_bytes = header.required_bytes();
            let bytes = buffer.extract(peeled_bytes);

            // Record message receipt.
            logger.as_mut().map(|logger| {
                logger.log(MessageEvent { is_send: false, header, });
            });

            if header.length > 0 {
                stageds[header.target - worker_offset].push(bytes);
            }
            else {
                // Shutting down; confirm absence of subsequent data.
                active = false;
                if !buffer.valid().is_empty() {
                    panic!("Clean shutdown followed by data.");
                }
                buffer.ensure_capacity(1);
                if reader.read(&mut buffer.empty()).expect("read failure") > 0 {
                    panic!("Clean shutdown followed by data.");
                }
            }
        }

        // Pass bytes along to targets.
        for (index, staged) in stageds.iter_mut().enumerate() {
            // FIXME: try to merge `staged` before handing it to BytesPush::extend
            use allocator::zero_copy::bytes_exchange::BytesPush;
            targets[index].extend(staged.drain(..));
        }

    }
    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: false, }));
}

/// Repeatedly sends messages into a TcpStream.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
pub fn send_loop(
    // TODO: Maybe we don't need BufWriter with consolidation in writes.
    writer: TcpStream,
    mut sources: Vec<MergeQueue>,
    signal: Signal,
    process: usize,
    remote: usize,
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>)
{

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: true, }));

    let mut writer = MyBuf::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();
    let mut hist = streaming_harness_hdrhist::HDRHist::new();
    let mut hist_n_bytes = streaming_harness_hdrhist::HDRHist::new();

    while !sources.is_empty() {

        // TODO: Round-robin better, to release resources fairly when overloaded.
        for source in sources.iter_mut() {
            use allocator::zero_copy::bytes_exchange::BytesPull;
            source.drain_into(&mut stash);
        }

        if stash.is_empty() {
            // No evidence of records to read, but sources not yet empty (at start of loop).
            // We are going to flush our writer (to move buffered data), double check on the
            // sources for emptiness and wait on a signal only if we are sure that there will
            // still be a signal incoming.
            //
            // We could get awoken by more data, a channel closing, or spuriously perhaps.
            let (sent, time) = writer.flush_and_count().expect("Failed to flush writer.");
            if sent > 0 {
                hist_n_bytes.add_value(sent as u64);
                hist.add_value(time);
            }
            sources.retain(|source| !source.is_complete());
            if !sources.is_empty() {
                signal.wait();
            }
        }
            else {
                // TODO: Could do scatter/gather write here.
                for mut bytes in stash.drain(..) {

                    // Record message sends.
                    logger.as_mut().map(|logger| {
                        let mut offset = 0;
                        while let Some(header) = MessageHeader::try_read(&mut bytes[offset..]) {
                            logger.log(MessageEvent { is_send: true, header, });
                            offset += header.required_bytes();
                        }
                    });

                    let flushed = writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
                    if flushed {
                        // Don't bother measuring this flush, it never happens.
                        // If for some reason it happens just repeat experiment
                        panic!("FLUSHED out of flushing");
                    }
                }
            }
    }

    // Write final zero-length header.
    // Would be better with meaningful metadata, but as this stream merges many
    // workers it isn't clear that there is anything specific to write here.
    let header = MessageHeader {
        channel:    0,
        source:     0,
        target:     0,
        length:     0,
        seqno:      0,
    };
    header.write_to(&mut writer).expect("Failed to write header!");
    writer.flush().expect("Failed to flush writer.");
    writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    logger.as_mut().map(|logger| logger.log(MessageEvent { is_send: true, header }));

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: false, }));

    println!("------------\nTCPWrite summary\n---------------");
    println!("{}", hist.summary_string());
    for entry in hist.ccdf() {
        println!("{:?}", entry);
    }
    println!("------------\nbytes summary\n---------------");
    println!("{}", hist_n_bytes.summary_string());
    for entry in hist_n_bytes.ccdf() {
        println!("{:?}", entry);
    }
}


struct MyBuf {
    inner: Option<TcpStream>,
    buf: Vec<u8>,
    panicked: bool,
}

impl MyBuf {
    fn write_all(&mut self, mut buf: &[u8]) -> io::Result<bool> {
        let mut flush = false;
        while !buf.is_empty() {
            match self.write_and_time(buf) {
                Ok((0, _)) => return Err(Error::new(ErrorKind::WriteZero,
                                               "failed to write whole buffer")),
                Ok((n, flushed)) => {
                    buf = &buf[n..];
                    if !flush {
                        // Check a one time flush
                        flush = flushed;
                    }
                },
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(flush)
    }

    pub fn with_capacity(cap: usize, inner: TcpStream) -> MyBuf {
        MyBuf {
            inner: Some(inner),
            buf: Vec::with_capacity(cap),
            panicked: false,
        }
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        let mut written = 0;
        let len = self.buf.len();
        let mut ret = Ok(());
        while written < len {
            self.panicked = true;
            let r = self.inner.as_mut().unwrap().write(&self.buf[written..]);
            self.panicked = false;

            match r {
                Ok(0) => {
                    ret = Err(Error::new(ErrorKind::WriteZero,
                                         "failed to write the buffered data"));
                    break;
                }
                Ok(n) => written += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => { ret = Err(e); break }

            }
        }
        if written > 0 {
            self.buf.drain(..written);
        }
        ret
    }

    pub fn get_mut(&mut self) -> &mut TcpStream { self.inner.as_mut().unwrap() }

    fn write_and_time(&mut self, buf: &[u8]) -> io::Result<(usize, bool)> {
        if self.buf.len() + buf.len() > self.buf.capacity() {
            self.flush_buf()?;
        }
        if buf.len() >= self.buf.capacity() {
            self.panicked = true;
            let r = self.inner.as_mut().unwrap().write(buf);
            self.panicked = false;
            r.map(|result| (result, true))
        } else {
            Write::write(&mut self.buf, buf).map(|result| (result, false))
        }
    }
    fn flush_and_count(&mut self) -> io::Result<(usize, u64)> {
        let len = self.buf.len();
        let t0 = ticks();
        self.flush_buf().and_then(|()| self.get_mut().flush());
        Ok((len, ticks() - t0))
    }
}

impl Write for MyBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buf.len() + buf.len() > self.buf.capacity() {
            self.flush_buf()?;
        }
        if buf.len() >= self.buf.capacity() {
            self.panicked = true;
            let r = self.inner.as_mut().unwrap().write(buf);
            self.panicked = false;
            r
        } else {
            Write::write(&mut self.buf, buf)
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flush_buf().and_then(|()| self.get_mut().flush())
    }
}
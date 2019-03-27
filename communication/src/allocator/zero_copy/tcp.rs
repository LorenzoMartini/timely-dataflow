//!\
extern crate hdrhist;
extern crate amd64_timer;

use std::io::{Read, Write};
use std::net::TcpStream;
use networking::MessageHeader;

use self::amd64_timer::ticks;

use super::bytes_slab::BytesSlab;
use super::bytes_exchange::{MergeQueue, Signal};

use logging_core::Logger;

use ::logging::{CommunicationEvent, CommunicationSetup, MessageEvent, StateEvent};
use std::io::Error;
use std::io;
use std::io::ErrorKind;

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
        let mut read = -1;

        while read <= 0 {
            read = match reader.read(&mut buffer.empty()) {
                Ok(n) => n as isize,
                Err(x) => {
                    match x.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            -1
                        },
                        _ => {
                            println!("Error: {:?}", x);
                            0
                        },
                    }
                    // We don't expect this, as socket closure results in Ok(0) reads.
                },
            };
        }

        assert!(read > 0);
        buffer.make_valid(read as usize);
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

                let mut read = 0;
                while read < 0 {
                    read = match reader.read(&mut buffer.empty()) {
                        Ok(n) => {
                            if n > 0 {
                                panic!("Clean shutdown followed by data.");
                            }
                            0
                        },
                        Err(x) => {
                            match x.kind() {
                                std::io::ErrorKind::WouldBlock => {
                                    -1
                                },
                                _ => {
                                    println!("Error: {:?}", x);
                                    0
                                },
                            }
                            // We don't expect this, as socket closure results in Ok(0) reads.
                        },
                    };
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
    writer.set_nonblocking(true).expect("CAN'T set nonblock");
    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: true, }));

    let mut writer = MyBufWriter::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();

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

            writer.flush().expect("Failed to flush writer.");

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
                    writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
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

    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: false, }));
}


struct MyBufWriter<W: Write> {
    inner: Option<W>,
    buf: Vec<u8>,
    // #30888: If the inner writer panics in a call to write, we don't want to
    // write the buffered data a second time in BufWriter's destructor. This
    // flag tells the Drop impl if it should skip the flush.
    panicked: bool,
    hist: hdrhist::HDRHist,
    hist_group: hdrhist::HDRHist,
}

struct IntoInnerError<W>(W, Error);

impl<W: Write> MyBufWriter<W> {

    pub fn new(inner: W) -> MyBufWriter<W> {
        MyBufWriter::with_capacity( 8 * 1024, inner)
    }

    pub fn with_capacity(cap: usize, inner: W) -> MyBufWriter<W> {
        MyBufWriter {
            inner: Some(inner),
            buf: Vec::with_capacity(cap),
            panicked: false,
            hist: hdrhist::HDRHist::new(),
            hist_group: hdrhist::HDRHist::new(),
        }
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        let mut written = 0;
        let len = self.buf.len();
        let mut ret = Ok(());

        let t0_group = ticks();

        while written < len {
            self.panicked = true;


            let writer = self.inner.as_mut().unwrap();
            let mut r = Ok(1);
            let mut success = false;
            let mut t0 = ticks();

            while !success {
                t0 = ticks();
                r = match writer.write(&self.buf[written..]) {
                    Ok(n) => {
                        success = true;
                        Ok(n)
                    },
                    Err(err) => {
                        match err.kind() {
                            std::io::ErrorKind::WouldBlock => {
                                Err(err)
                            },
                            _ => panic!("UNKNOWN ERROR")
                        }
                    }
                }
            }
            let t1 = ticks();

            self.panicked = false;

            match r {
                Ok(0) => {
                    ret = Err(Error::new(ErrorKind::WriteZero,
                                         "failed to write the buffered data"));
                    break;
                }
                Ok(n) => {
                    self.hist.add_value(t1 - t0);
                    written += n
                },
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => { ret = Err(e); break }

            }
        }
        if written > 0 {
            let t1_group = ticks();
            self.hist_group.add_value(t1_group - t0_group);

            self.buf.drain(..written);
        }
        ret
    }

    fn get_ref(&self) -> &W { self.inner.as_ref().unwrap() }

    fn get_mut(&mut self) -> &mut W { self.inner.as_mut().unwrap() }

    fn buffer(&self) -> &[u8] {
        &self.buf
    }

    fn into_inner(mut self) -> Result<W, IntoInnerError<MyBufWriter<W>>> {
        match self.flush_buf() {
            Err(e) => Err(IntoInnerError(self, e)),
            Ok(()) => Ok(self.inner.take().unwrap())
        }
    }
}

impl<W: Write> Write for MyBufWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buf.len() + buf.len() > self.buf.capacity() {
            panic!("FLUSHING OUT OF FLUSH, BUF TOO SMALL");
            self.flush_buf()?;
        }
        if buf.len() >= self.buf.capacity() {
            panic!("FLUSHING OUT OF FLUSH, BUF TOO SMALL");
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

impl<W: Write> Drop for MyBufWriter<W> {
    fn drop(&mut self) {
        println!("------------\nTime duration of tcpwrite (cycles)\n---------------");
        println!("{}", self.hist.summary_string());
        for entry in self.hist.ccdf_upper_bound() {
            println!("{:?}", entry);
        }
        println!("------------\nTime duration of tcpwrite outer (cycles)\n---------------");
        println!("{}", self.hist_group.summary_string());
        for entry in self.hist_group.ccdf_upper_bound() {
            println!("{:?}", entry);
        }
    }
}
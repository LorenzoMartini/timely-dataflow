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
    reader.set_nonblocking(true);
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

    let mut hist_read = streaming_harness_hdrhist::HDRHist::new();
    let mut hist_read_process = streaming_harness_hdrhist::HDRHist::new();
    let mut hist_processing = streaming_harness_hdrhist::HDRHist::new();
    let mut hist_lock = streaming_harness_hdrhist::HDRHist::new();
    let mut hist_n_bytes = streaming_harness_hdrhist::HDRHist::new();

    while active {
        buffer.ensure_capacity(1);

        assert!(!buffer.empty().is_empty());

        let mut t0_read = ticks();
        // Attempt to read some more bytes into self.buffer.
        let mut read = 0;
        while read <= 0 {
            read = match reader.read(&mut buffer.empty()) {
                Ok(n) => n,
                Err(err) => match err.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        t0_read = ticks();
                        0
                    },
                    _ => panic!("Error occurred while reading: {:?}", err),
                }
            }
        }

        // TODO done read && start processing
        let t1_read = ticks();
        assert!(read > 0);
        buffer.make_valid(read);

        let t1_read_process = ticks();
        hist_read.add_value(t1_read - t0_read);
        hist_read_process.add_value(t1_read_process - t0_read);
        hist_n_bytes.add_value(read as u64);
        // Consume complete messages from the front of self.buffer.
        let t0_processing = ticks();
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

                // Shutdown
                let mut last_read = -1;
                while last_read < 0 {
                    last_read = match reader.read(&mut buffer.empty()) {
                        Ok(n) => {
                            if n > 0 {
                                panic!("Clean shutdown followed by data.");
                            }
                            n as isize
                        },
                        Err(err) => match err.kind() {
                            std::io::ErrorKind::WouldBlock => {
                                -1
                            },
                            _ => panic!("Error occurred while shutting down stream: {:?}", err),
                        }
                    }
                }
            }
        }


        // Pass bytes along to targets.
        for (index, staged) in stageds.iter_mut().enumerate() {
            // FIXME: try to merge `staged` before handing it to BytesPush::extend
            use allocator::zero_copy::bytes_exchange::BytesPush;
            // TODO LOCK
            let t0_lock = ticks();
            targets[index].extend(staged.drain(..));
            let t1_lock = ticks();
            hist_processing.add_value(t1_lock - t0_processing);
            hist_lock.add_value(t1_lock - t0_lock);
        }

    }
    println!("------------\nNBytes read summary\n---------------");
    println!("{}", hist_n_bytes.summary_string());
    for entry in hist_n_bytes.ccdf() {
        println!("{:?}", entry);
    }
    println!("------------\nMessage read summary\n---------------");
    println!("{}", hist_read.summary_string());
    for entry in hist_read.ccdf() {
        println!("{:?}", entry);
    }
//    println!("------------\nMessage read processing summary\n---------------");
//    println!("{}", hist_read_process.summary_string());
//    for entry in hist_read_process.ccdf() {
//        println!("{:?}", entry);
//    }
//    println!("------------\nMergeQueue summary\n---------------");
//    println!("{}", hist_lock.summary_string());
//    for entry in hist_lock.ccdf() {
//        println!("{:?}", entry);
//    }
//    println!("------------\nMessage processing (interpret + put in mergequeue) summary\n---------------");
//    println!("{}", hist_processing.summary_string());
//    for entry in hist_processing.ccdf() {
//        println!("{:?}", entry);
//    }

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

    let mut writer = ::std::io::BufWriter::with_capacity(1 << 16, writer);
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
            let mut flushed = false;
            while !flushed {
                match writer.flush() {
                    Ok(_) => flushed = true,
                    Err(err) => match err.kind() {
                        std::io::ErrorKind::WouldBlock => {},
                        err => panic!("Failed to flush writer, err {:?}", err)
                    }
                }
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
                let mut written = false;
                while !written {
                    match writer.write_all(&bytes[..]) {
                        Ok(_) => written = true,
                        Err(err) => match err.kind() {
                            std::io::ErrorKind::WouldBlock => {},
                            err => panic!("Write failure in send_loop {:?}", err)
                        }
                    }
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
    let mut flushed = false;
    while !flushed {
        match writer.flush() {
            Ok(_) => flushed = true,
            Err(err) => match err.kind() {
                std::io::ErrorKind::WouldBlock => {},
                err => panic!("Failed to flush writer, err {:?}", err)
            }
        }
    }
    writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    logger.as_mut().map(|logger| logger.log(MessageEvent { is_send: true, header }));

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: false, }));
}

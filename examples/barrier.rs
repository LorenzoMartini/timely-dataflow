extern crate timely;
extern crate streaming_harness_hdrhist;
extern crate core_affinity;

use std::time::Instant;
use std::rc::Rc;
use std::cell::RefCell;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::generic::operator::Operator;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<usize>().unwrap_or(1_000_000);

    let hists = timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let index = worker.index();
        // Pin core
        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index % core_ids.len()]);

        let hist1 = Rc::new(RefCell::new(streaming_harness_hdrhist::HDRHist::new()));
        let hist2 = hist1.clone();

        worker.dataflow(move |scope| {
            let (handle, stream) = scope.feedback::<usize>(1);
            let mut t0 = Instant::now();
            stream.unary_notify(
                Pipeline,
                "Barrier",
                vec![0],
                move |_, _, notificator| {
                    while let Some((cap, _count)) = notificator.next() {
                        let t1 = Instant::now();
                        let duration = t1.duration_since(t0);
                        hist1.borrow_mut().add_value(duration.as_secs() * 1_000_000_000u64 + duration.subsec_nanos() as u64);
                        t0 = t1;
                        let time = *cap.time() + 1;
                        if time < iterations {
                            notificator.notify_at(cap.delayed(&time));
                        }
                    }
                }
            ).connect_loop(handle);
        });

        while worker.step() {}

        let hist = ::std::rc::Rc::try_unwrap(hist2).expect("Non unique owner");
        hist.into_inner()
    }).unwrap().join(); // asserts error-free execution;

    println!("-------------\nSummary:\n{}", hists[0].as_ref().unwrap().summary_string());
    for entry in hists[0].as_ref().unwrap().ccdf() {
        println!("{:?}", entry);
    }

}

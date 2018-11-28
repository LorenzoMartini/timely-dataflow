extern crate timely;

fn main() {

    let mut timers =
        timely::execute_from_args(std::env::args(), |_| ::std::time::Instant::now())
            .expect("Timely did not exit cleanly")
            .join()
            .into_iter()
            .map(|x| x.expect("Error from worker"))
            .collect::<Vec<_>>();

    timers.sort();

    for timer in timers.iter() {
        println!("{:?}", timer.duration_since(timers[0]));
    }
}
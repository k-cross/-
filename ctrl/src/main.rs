use ctrl::buffer::{Buffer, Controller, closed_loop, open_loop};
use ctrl::chart;

fn main() {
    let tm = 5000;

    let mut process = Buffer::new(10, 5);
    let open_data = open_loop(&mut process, tm);
    chart::draw(&open_data, "Open Loop", "open_loop.png").expect("Failed to draw open loop chart");

    let mut process = Buffer::new(10, 5);
    let mut controller = Controller::new(1.25, 0.01);
    let closed_data = closed_loop(&mut controller, &mut process, tm);
    chart::draw(&closed_data, "Closed Loop", "closed_loop.png")
        .expect("Failed to draw closed loop chart");

    hitrate_sim();
}

// calculating cache from chapter 2
fn cache(size: i64) -> f64 {
    if size < 0 {
        0.0
    } else if size > 100 {
        1.0
    } else {
        size as f64 / 100.0
    }
}

fn hitrate_sim() {
    let r = 0.6; // reference signal
    // 50..175 might change later
    let k = 50; // gain factor
    let mut y = 0.0;
    let mut c = 0.0;

    for _ in 0..200 {
        let e = r - y; // tracking error
        c += e; // cumulative error
        let u = k as f64 * c; // control action: cache size
        y = cache(u.round() as i64); // process output: hitrace

        println!("{r} {e} {c} {u} {y}");
    }
}

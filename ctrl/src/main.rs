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
}

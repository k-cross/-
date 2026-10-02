use ctrl::buffer::{Buffer, Controller, closed_loop, open_loop};

fn main() {
    let mut process = Buffer::new(10, 5);
    let mut controller = Controller::new(1.25, 0.01);
    let tm = 5000;

    //open_loop(&mut process, tm);
    closed_loop(&mut controller, &mut process, tm);
}

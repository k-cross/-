pub struct Buffer {
    queued: i64,
    wip: i64,
    max_flow: i64,
    max_wip: i64,
}

impl Buffer {
    pub fn new(mf: i64, mw: i64) -> Self {
        Buffer {
            queued: 0,
            wip: 0,
            max_flow: mf,
            max_wip: mw,
        }
    }

    pub fn work(&mut self, u: f64) -> i64 {
        // Add to ready pool
        let mut uu: i64 = std::cmp::max(0, u.round() as i64);
        uu = std::cmp::min(uu, self.max_wip);
        self.wip += uu;

        // Transfer from ready pool to queue
        let r: i64 = if self.wip == 0 {
            0
        } else {
            rand::random_range(0..self.wip)
        };
        self.wip -= r;
        self.queued += r;

        // Release from queue to downstream process
        let mut r: i64 = rand::random_range(0..self.max_flow);
        r = std::cmp::min(r, self.queued);
        self.queued -= r;

        self.queued
    }
}

pub struct Controller {
    kp: f64,
    ki: f64,
    i: f64,
}

impl Controller {
    pub fn new(kp: f64, ki: f64) -> Self {
        Self { kp, ki, i: 0.0 }
    }

    pub fn work(&mut self, e: f64) -> f64 {
        self.i += e;
        (self.kp * e) + (self.ki * self.i)
    }
}

pub fn open_loop(process: &mut Buffer, tm: i64) {
    // might become a function one day?
    const TARGET: f64 = 5.0;

    for _ in 0..tm {
        let u = TARGET;
        let y = process.work(u);
        println!("{TARGET} {u} 0 {u} {y}");
    }
}

pub fn closed_loop(controller: &mut Controller, process: &mut Buffer, tm: i64) {
    let mut y = 0;
    for t in 0..tm {
        let r = setpoint(t);
        let e = r - y;
        let u = controller.work(e as f64);
        y = process.work(u);
        println!("{t} {r} {e} {u} {y}");
    }
}

fn setpoint(t: i64) -> i64 {
    if t < 100 {
        return 0;
    } else if t < 300 {
        return 50;
    }
    10
}

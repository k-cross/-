use ctrl::buffer::{Buffer, Controller, StepRecord, closed_loop, open_loop};
use ctrl::chart::{self, ChartData, HitrateRecord};

fn main() -> eframe::Result<()> {
    let tm = 5000;

    let mut process = Buffer::new(10, 5);
    let open_data = open_loop(&mut process, tm);

    let mut process = Buffer::new(10, 5);
    let mut controller = Controller::new(1.25, 0.01);
    let closed_data = closed_loop(&mut controller, &mut process, tm);
    let hitrate_data = hitrate_sim();
    let ch3_data = chapter_3_sim(1.0, 0.8);

    chart::run(vec![
        ChartData::Step {
            name: "Open Loop".to_string(),
            data: open_data,
        },
        ChartData::Step {
            name: "Closed Loop".to_string(),
            data: closed_data,
        },
        ChartData::Hitrate {
            name: "Hit Rate Sim".to_string(),
            data: hitrate_data,
        },
        ChartData::Step {
            name: "Chapter 3".to_string(),
            data: ch3_data,
        },
    ])
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

fn hitrate_sim() -> Vec<HitrateRecord> {
    let r = 0.6; // reference signal
    // 50..175 might change later
    let k = 50; // gain factor
    let mut y = 0.0;
    let mut c = 0.0;
    let mut records = Vec::with_capacity(200);

    for t in 0..200 {
        let e = r - y; // tracking error
        c += e; // cumulative error
        let u = k as f64 * c; // control action: cache size
        y = cache(u.round() as i64); // process output: hitrace
        records.push(HitrateRecord {
            t: t as f64,
            r,
            e,
            c,
            u,
            y,
        });
    }

    records
}

fn chapter_3_sim(r: f64, k: f64) -> Vec<StepRecord> {
    let mut records = Vec::with_capacity(200);
    let mut u = 0.0;

    for t in 0..200 {
        let y = u;
        let e = r - y;
        u = k * e;
        records.push(StepRecord {
            t: t as f64,
            r,
            e,
            u,
            y,
        });
    }
    records
}

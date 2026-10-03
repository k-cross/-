use plotters::prelude::*;

use crate::buffer::StepRecord;

pub fn draw(
    records: &[StepRecord],
    title: &str,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = BitMapBackend::new(output_path, (1200, 800)).into_drawing_area();
    root.fill(&WHITE)?;

    let x_max = records.last().map(|r| r.t).unwrap_or(1.0);

    let (y_min, y_max) = records
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), r| {
            let vals = [r.r, r.e, r.u, r.y];
            (
                lo.min(vals.into_iter().fold(f64::INFINITY, f64::min)),
                hi.max(vals.into_iter().fold(f64::NEG_INFINITY, f64::max)),
            )
        });

    let margin = (y_max - y_min).abs() * 0.05;

    let mut chart = ChartBuilder::on(&root)
        .caption(title, ("sans-serif", 28))
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0.0..x_max, (y_min - margin)..(y_max + margin))?;

    chart
        .configure_mesh()
        .x_desc("Step")
        .y_desc("Value")
        .draw()?;

    let series: &[(&str, fn(&StepRecord) -> f64, &RGBColor)] = &[
        ("Setpoint (r)", |r| r.r, &RED),
        ("Error (e)", |r| r.e, &BLUE),
        ("Control (u)", |r| r.u, &GREEN),
        ("Output (y)", |r| r.y, &MAGENTA),
    ];

    for &(label, accessor, color) in series {
        chart
            .draw_series(LineSeries::new(
                records.iter().map(|r| (r.t, accessor(r))),
                color,
            ))?
            .label(label)
            .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color));
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .background_style(WHITE.mix(0.8))
        .border_style(&BLACK)
        .draw()?;

    root.present()?;
    println!("Chart saved to {output_path}");
    Ok(())
}

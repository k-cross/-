use eframe::egui::{self, Color32, RichText, Vec2b, vec2};
use egui_plot::{Corner, Legend, Line, Plot, PlotPoint, PlotPoints};

use crate::buffer::StepRecord;

pub const COLOR_R: Color32 = Color32::from_rgb(235, 75, 75);
pub const COLOR_E: Color32 = Color32::from_rgb(60, 140, 240);
pub const COLOR_C: Color32 = Color32::from_rgb(245, 160, 40);
pub const COLOR_U: Color32 = Color32::from_rgb(50, 195, 110);
pub const COLOR_Y: Color32 = Color32::from_rgb(190, 70, 230);

#[derive(Debug, Clone, Copy)]
pub struct HitrateRecord {
    pub t: f64,
    pub r: f64,
    pub e: f64,
    pub c: f64,
    pub u: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimTab {
    OpenLoop,
    ClosedLoop,
    Hitrate,
}

struct LoopSeries {
    r: Vec<PlotPoint>,
    e: Vec<PlotPoint>,
    u: Vec<PlotPoint>,
    y: Vec<PlotPoint>,
    total_steps: usize,
}

impl LoopSeries {
    fn from_records(records: &[StepRecord]) -> Self {
        let mut r = Vec::with_capacity(records.len());
        let mut e = Vec::with_capacity(records.len());
        let mut u = Vec::with_capacity(records.len());
        let mut y = Vec::with_capacity(records.len());

        for rec in records {
            r.push(PlotPoint::new(rec.t, rec.r));
            e.push(PlotPoint::new(rec.t, rec.e));
            u.push(PlotPoint::new(rec.t, rec.u));
            y.push(PlotPoint::new(rec.t, rec.y));
        }

        Self {
            r,
            e,
            u,
            y,
            total_steps: records.len(),
        }
    }
}

struct HitrateSeries {
    r: Vec<PlotPoint>,
    e: Vec<PlotPoint>,
    c: Vec<PlotPoint>,
    u: Vec<PlotPoint>,
    y: Vec<PlotPoint>,
    total_steps: usize,
}

impl HitrateSeries {
    fn from_records(records: &[HitrateRecord]) -> Self {
        let mut r = Vec::with_capacity(records.len());
        let mut e = Vec::with_capacity(records.len());
        let mut c = Vec::with_capacity(records.len());
        let mut u = Vec::with_capacity(records.len());
        let mut y = Vec::with_capacity(records.len());

        for rec in records {
            r.push(PlotPoint::new(rec.t, rec.r));
            e.push(PlotPoint::new(rec.t, rec.e));
            c.push(PlotPoint::new(rec.t, rec.c));
            u.push(PlotPoint::new(rec.t, rec.u));
            y.push(PlotPoint::new(rec.t, rec.y));
        }

        Self {
            r,
            e,
            c,
            u,
            y,
            total_steps: records.len(),
        }
    }
}

pub struct SimPlotApp {
    open_series: LoopSeries,
    closed_series: LoopSeries,
    hitrate_series: HitrateSeries,
    active_tab: SimTab,

    show_open_r: bool,
    show_open_e: bool,
    show_open_u: bool,
    show_open_y: bool,

    show_closed_r: bool,
    show_closed_e: bool,
    show_closed_u: bool,
    show_closed_y: bool,

    show_hr_r: bool,
    show_hr_e: bool,
    show_hr_c: bool,
    show_hr_u: bool,
    show_hr_y: bool,

    reset_view: bool,
}

impl SimPlotApp {
    pub fn new(
        open_data: Vec<StepRecord>,
        closed_data: Vec<StepRecord>,
        hitrate_data: Vec<HitrateRecord>,
    ) -> Self {
        Self {
            open_series: LoopSeries::from_records(&open_data),
            closed_series: LoopSeries::from_records(&closed_data),
            hitrate_series: HitrateSeries::from_records(&hitrate_data),
            active_tab: SimTab::OpenLoop,

            show_open_r: true,
            show_open_e: true,
            show_open_u: true,
            show_open_y: true,

            show_closed_r: true,
            show_closed_e: true,
            show_closed_u: true,
            show_closed_y: true,

            show_hr_r: true,
            show_hr_e: true,
            show_hr_c: true,
            show_hr_u: true,
            show_hr_y: true,

            reset_view: true,
        }
    }
}

impl eframe::App for SimPlotApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        ui.vertical(|ui| {
            // Top navigation header
            ui.horizontal(|ui| {
                ui.heading("Control Systems Simulation Plotter");
                ui.separator();

                if ui
                    .selectable_label(self.active_tab == SimTab::OpenLoop, "Open Loop")
                    .clicked()
                {
                    self.active_tab = SimTab::OpenLoop;
                    self.reset_view = true;
                }
                if ui
                    .selectable_label(self.active_tab == SimTab::ClosedLoop, "Closed Loop")
                    .clicked()
                {
                    self.active_tab = SimTab::ClosedLoop;
                    self.reset_view = true;
                }
                if ui
                    .selectable_label(self.active_tab == SimTab::Hitrate, "Hit Rate Sim")
                    .clicked()
                {
                    self.active_tab = SimTab::Hitrate;
                    self.reset_view = true;
                }

                ui.separator();
                if ui.button("Fit / Reset View").clicked() {
                    self.reset_view = true;
                }
            });

            ui.separator();

            // Controls & stats bar for current tab
            ui.horizontal(|ui| {
                ui.label(RichText::new("Series:").strong());

                match self.active_tab {
                    SimTab::OpenLoop => {
                        ui.checkbox(
                            &mut self.show_open_r,
                            RichText::new("Setpoint (r)").color(COLOR_R),
                        );
                        ui.checkbox(
                            &mut self.show_open_e,
                            RichText::new("Error (e)").color(COLOR_E),
                        );
                        ui.checkbox(
                            &mut self.show_open_u,
                            RichText::new("Control (u)").color(COLOR_U),
                        );
                        ui.checkbox(
                            &mut self.show_open_y,
                            RichText::new("Output (y)").color(COLOR_Y),
                        );
                        ui.separator();
                        ui.label(format!("Steps: {}", self.open_series.total_steps));
                    }
                    SimTab::ClosedLoop => {
                        ui.checkbox(
                            &mut self.show_closed_r,
                            RichText::new("Setpoint (r)").color(COLOR_R),
                        );
                        ui.checkbox(
                            &mut self.show_closed_e,
                            RichText::new("Error (e)").color(COLOR_E),
                        );
                        ui.checkbox(
                            &mut self.show_closed_u,
                            RichText::new("Control (u)").color(COLOR_U),
                        );
                        ui.checkbox(
                            &mut self.show_closed_y,
                            RichText::new("Output (y)").color(COLOR_Y),
                        );
                        ui.separator();
                        ui.label(format!("Steps: {}", self.closed_series.total_steps));
                    }
                    SimTab::Hitrate => {
                        ui.checkbox(
                            &mut self.show_hr_r,
                            RichText::new("Reference (r)").color(COLOR_R),
                        );
                        ui.checkbox(
                            &mut self.show_hr_e,
                            RichText::new("Error (e)").color(COLOR_E),
                        );
                        ui.checkbox(
                            &mut self.show_hr_c,
                            RichText::new("Cumulative (c)").color(COLOR_C),
                        );
                        ui.checkbox(
                            &mut self.show_hr_u,
                            RichText::new("Control (u)").color(COLOR_U),
                        );
                        ui.checkbox(
                            &mut self.show_hr_y,
                            RichText::new("Output (y)").color(COLOR_Y),
                        );
                        ui.separator();
                        ui.label(format!("Steps: {}", self.hitrate_series.total_steps));
                    }
                }
            });

            ui.separator();

            // Main Plot area
            let (plot_id, x_label, y_label) = match self.active_tab {
                SimTab::OpenLoop => ("open_loop_plot", "Step (t)", "Value"),
                SimTab::ClosedLoop => ("closed_loop_plot", "Step (t)", "Value"),
                SimTab::Hitrate => ("hitrate_plot", "Step (t)", "Value"),
            };

            let mut plot = Plot::new(plot_id)
                .legend(Legend::default().position(Corner::RightTop))
                .x_axis_label(x_label)
                .y_axis_label(y_label);

            if self.reset_view {
                plot = plot.auto_bounds(Vec2b::new(true, true));
                self.reset_view = false;
            }

            plot.show(ui, |plot_ui| match self.active_tab {
                SimTab::OpenLoop => {
                    if self.show_open_r {
                        plot_ui.line(
                            Line::new("Setpoint (r)", PlotPoints::Borrowed(&self.open_series.r))
                                .color(COLOR_R)
                                .width(1.8),
                        );
                    }
                    if self.show_open_e {
                        plot_ui.line(
                            Line::new("Error (e)", PlotPoints::Borrowed(&self.open_series.e))
                                .color(COLOR_E)
                                .width(1.8),
                        );
                    }
                    if self.show_open_u {
                        plot_ui.line(
                            Line::new("Control (u)", PlotPoints::Borrowed(&self.open_series.u))
                                .color(COLOR_U)
                                .width(1.8),
                        );
                    }
                    if self.show_open_y {
                        plot_ui.line(
                            Line::new("Output (y)", PlotPoints::Borrowed(&self.open_series.y))
                                .color(COLOR_Y)
                                .width(1.8),
                        );
                    }
                }
                SimTab::ClosedLoop => {
                    if self.show_closed_r {
                        plot_ui.line(
                            Line::new("Setpoint (r)", PlotPoints::Borrowed(&self.closed_series.r))
                                .color(COLOR_R)
                                .width(1.8),
                        );
                    }
                    if self.show_closed_e {
                        plot_ui.line(
                            Line::new("Error (e)", PlotPoints::Borrowed(&self.closed_series.e))
                                .color(COLOR_E)
                                .width(1.8),
                        );
                    }
                    if self.show_closed_u {
                        plot_ui.line(
                            Line::new("Control (u)", PlotPoints::Borrowed(&self.closed_series.u))
                                .color(COLOR_U)
                                .width(1.8),
                        );
                    }
                    if self.show_closed_y {
                        plot_ui.line(
                            Line::new("Output (y)", PlotPoints::Borrowed(&self.closed_series.y))
                                .color(COLOR_Y)
                                .width(1.8),
                        );
                    }
                }
                SimTab::Hitrate => {
                    if self.show_hr_r {
                        plot_ui.line(
                            Line::new(
                                "Reference (r)",
                                PlotPoints::Borrowed(&self.hitrate_series.r),
                            )
                            .color(COLOR_R)
                            .width(1.8),
                        );
                    }
                    if self.show_hr_e {
                        plot_ui.line(
                            Line::new("Error (e)", PlotPoints::Borrowed(&self.hitrate_series.e))
                                .color(COLOR_E)
                                .width(1.8),
                        );
                    }
                    if self.show_hr_c {
                        plot_ui.line(
                            Line::new(
                                "Cumulative (c)",
                                PlotPoints::Borrowed(&self.hitrate_series.c),
                            )
                            .color(COLOR_C)
                            .width(1.8),
                        );
                    }
                    if self.show_hr_u {
                        plot_ui.line(
                            Line::new("Control (u)", PlotPoints::Borrowed(&self.hitrate_series.u))
                                .color(COLOR_U)
                                .width(1.8),
                        );
                    }
                    if self.show_hr_y {
                        plot_ui.line(
                            Line::new("Output (y)", PlotPoints::Borrowed(&self.hitrate_series.y))
                                .color(COLOR_Y)
                                .width(1.8),
                        );
                    }
                }
            });
        });
    }
}

pub fn run(
    open_data: Vec<StepRecord>,
    closed_data: Vec<StepRecord>,
    hitrate_data: Vec<HitrateRecord>,
) -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size(vec2(1200.0, 800.0))
            .with_min_inner_size(vec2(640.0, 480.0))
            .with_title("Control Systems Simulation Plotter"),
        ..Default::default()
    };
    eframe::run_native(
        "Control Systems Simulation Plotter",
        native_options,
        Box::new(|_cc| {
            Ok(Box::new(SimPlotApp::new(
                open_data,
                closed_data,
                hitrate_data,
            )))
        }),
    )
}

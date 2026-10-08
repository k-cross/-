use eframe::egui::{self, Color32, RichText, vec2};
use egui_plot::{Corner, Legend, Line, Plot, PlotPoint, PlotPoints};

pub use crate::buffer::{
    HitrateRecord, StepRecord, chapter_3_sim_with_steps, closed_loop_with_params,
    hitrate_sim_with_params, open_loop_with_params,
};

pub const COLOR_R: Color32 = Color32::from_rgb(235, 75, 75);
pub const COLOR_E: Color32 = Color32::from_rgb(60, 140, 240);
pub const COLOR_C: Color32 = Color32::from_rgb(245, 160, 40);
pub const COLOR_U: Color32 = Color32::from_rgb(50, 195, 110);
pub const COLOR_Y: Color32 = Color32::from_rgb(190, 70, 230);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimTab {
    OpenLoop,
    ClosedLoop,
    Hitrate,
    Chapter3,
}

#[derive(Debug, Clone)]
pub enum GraphData {
    Step {
        name: String,
        data: Vec<StepRecord>,
    },
    Hitrate {
        name: String,
        data: Vec<HitrateRecord>,
    },
}

pub type ChartData = GraphData;

impl GraphData {
    pub fn name(&self) -> &str {
        match self {
            Self::Step { name, .. } => name,
            Self::Hitrate { name, .. } => name,
        }
    }

    pub fn step(name: impl Into<String>, data: Vec<StepRecord>) -> Self {
        Self::Step {
            name: name.into(),
            data,
        }
    }

    pub fn hitrate(name: impl Into<String>, data: Vec<HitrateRecord>) -> Self {
        Self::Hitrate {
            name: name.into(),
            data,
        }
    }
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

    fn render<'a>(
        &'a self,
        plot_ui: &mut egui_plot::PlotUi<'a>,
        show_r: bool,
        show_e: bool,
        show_u: bool,
        show_y: bool,
    ) {
        if show_r {
            plot_ui.line(
                Line::new("Setpoint (r)", PlotPoints::Borrowed(&self.r))
                    .color(COLOR_R)
                    .width(1.8),
            );
        }
        if show_e {
            plot_ui.line(
                Line::new("Error (e)", PlotPoints::Borrowed(&self.e))
                    .color(COLOR_E)
                    .width(1.8),
            );
        }
        if show_u {
            plot_ui.line(
                Line::new("Control (u)", PlotPoints::Borrowed(&self.u))
                    .color(COLOR_U)
                    .width(1.8),
            );
        }
        if show_y {
            plot_ui.line(
                Line::new("Output (y)", PlotPoints::Borrowed(&self.y))
                    .color(COLOR_Y)
                    .width(1.8),
            );
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

    fn render<'a>(
        &'a self,
        plot_ui: &mut egui_plot::PlotUi<'a>,
        show_r: bool,
        show_e: bool,
        show_c: bool,
        show_u: bool,
        show_y: bool,
    ) {
        if show_r {
            plot_ui.line(
                Line::new("Reference (r)", PlotPoints::Borrowed(&self.r))
                    .color(COLOR_R)
                    .width(1.8),
            );
        }
        if show_e {
            plot_ui.line(
                Line::new("Error (e)", PlotPoints::Borrowed(&self.e))
                    .color(COLOR_E)
                    .width(1.8),
            );
        }
        if show_c {
            plot_ui.line(
                Line::new("Cumulative (c)", PlotPoints::Borrowed(&self.c))
                    .color(COLOR_C)
                    .width(1.8),
            );
        }
        if show_u {
            plot_ui.line(
                Line::new("Control (u)", PlotPoints::Borrowed(&self.u))
                    .color(COLOR_U)
                    .width(1.8),
            );
        }
        if show_y {
            plot_ui.line(
                Line::new("Output (y)", PlotPoints::Borrowed(&self.y))
                    .color(COLOR_Y)
                    .width(1.8),
            );
        }
    }
}

struct StepGraphState {
    name: String,
    series: LoopSeries,
    show_r: bool,
    show_e: bool,
    show_u: bool,
    show_y: bool,
}

struct HitrateGraphState {
    name: String,
    series: HitrateSeries,
    show_r: bool,
    show_e: bool,
    show_c: bool,
    show_u: bool,
    show_y: bool,
}

enum GraphItem {
    Step(StepGraphState),
    Hitrate(HitrateGraphState),
}

impl GraphItem {
    fn name(&self) -> &str {
        match self {
            Self::Step(g) => &g.name,
            Self::Hitrate(g) => &g.name,
        }
    }

    fn from_graph_data(data: GraphData) -> Self {
        match data {
            GraphData::Step { name, data } => Self::Step(StepGraphState {
                name,
                series: LoopSeries::from_records(&data),
                show_r: true,
                show_e: true,
                show_u: true,
                show_y: true,
            }),
            GraphData::Hitrate { name, data } => Self::Hitrate(HitrateGraphState {
                name,
                series: HitrateSeries::from_records(&data),
                show_r: true,
                show_e: true,
                show_c: true,
                show_u: true,
                show_y: true,
            }),
        }
    }
}

pub struct SimPlotApp {
    graphs: Vec<GraphItem>,
    selected_graph: usize,

    // Interactive parameters for Chapter 3
    ch3_r: f64,
    ch3_k: f64,
    ch3_steps: usize,

    // Interactive parameters for Hit Rate Sim
    hr_r: f64,
    hr_k: f64,
    hr_steps: usize,

    // Interactive parameters for Closed Loop
    cl_kp: f64,
    cl_ki: f64,
    cl_mf: i64,
    cl_mw: i64,
    cl_steps: i64,

    // Interactive parameters for Open Loop
    ol_target: f64,
    ol_mf: i64,
    ol_mw: i64,
    ol_steps: i64,

    auto_fit_on_change: bool,
    reset_view: bool,
}

impl SimPlotApp {
    pub fn new(graphs: Vec<GraphData>) -> Self {
        let items: Vec<GraphItem> = graphs.into_iter().map(GraphItem::from_graph_data).collect();
        let selected_graph = items
            .iter()
            .position(|g| g.name().eq_ignore_ascii_case("chapter 3"))
            .unwrap_or(0);

        Self {
            graphs: items,
            selected_graph,

            ch3_r: 1.0,
            ch3_k: 0.8,
            ch3_steps: 200,

            hr_r: 0.6,
            hr_k: 50.0,
            hr_steps: 200,

            cl_kp: 1.25,
            cl_ki: 0.01,
            cl_mf: 10,
            cl_mw: 5,
            cl_steps: 5000,

            ol_target: 5.0,
            ol_mf: 10,
            ol_mw: 5,
            ol_steps: 5000,

            auto_fit_on_change: true,
            reset_view: true,
        }
    }

    fn recompute_ch3(&mut self) {
        let data = chapter_3_sim_with_steps(self.ch3_r, self.ch3_k, self.ch3_steps);
        if let Some(GraphItem::Step(state)) = self.graphs.get_mut(self.selected_graph) {
            state.series = LoopSeries::from_records(&data);
        }
        if self.auto_fit_on_change {
            self.reset_view = true;
        }
    }

    fn recompute_hitrate(&mut self) {
        let data = hitrate_sim_with_params(self.hr_r, self.hr_k, self.hr_steps);
        if let Some(GraphItem::Hitrate(state)) = self.graphs.get_mut(self.selected_graph) {
            state.series = HitrateSeries::from_records(&data);
        }
        if self.auto_fit_on_change {
            self.reset_view = true;
        }
    }

    fn recompute_closed(&mut self) {
        let data = closed_loop_with_params(
            self.cl_kp,
            self.cl_ki,
            self.cl_mf,
            self.cl_mw,
            self.cl_steps,
        );
        if let Some(GraphItem::Step(state)) = self.graphs.get_mut(self.selected_graph) {
            state.series = LoopSeries::from_records(&data);
        }
        if self.auto_fit_on_change {
            self.reset_view = true;
        }
    }

    fn recompute_open(&mut self) {
        let data = open_loop_with_params(self.ol_mf, self.ol_mw, self.ol_steps, self.ol_target);
        if let Some(GraphItem::Step(state)) = self.graphs.get_mut(self.selected_graph) {
            state.series = LoopSeries::from_records(&data);
        }
        if self.auto_fit_on_change {
            self.reset_view = true;
        }
    }
}

fn render_loop_checkboxes(
    ui: &mut egui::Ui,
    show_r: &mut bool,
    show_e: &mut bool,
    show_u: &mut bool,
    show_y: &mut bool,
    total_steps: usize,
) {
    ui.checkbox(show_r, RichText::new("Setpoint (r)").color(COLOR_R));
    ui.checkbox(show_e, RichText::new("Error (e)").color(COLOR_E));
    ui.checkbox(show_u, RichText::new("Control (u)").color(COLOR_U));
    ui.checkbox(show_y, RichText::new("Output (y)").color(COLOR_Y));
    ui.separator();
    ui.label(format!("Steps: {total_steps}"));
}

impl eframe::App for SimPlotApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if self.graphs.is_empty() {
            egui::CentralPanel::default().show(ui, |ui| {
                ui.heading("Control Systems Simulation Plotter");
                ui.separator();
                ui.label("No graphs to display.");
            });
            return;
        }

        if self.selected_graph >= self.graphs.len() {
            self.selected_graph = 0;
        }

        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical(|ui| {
                // Top navigation header with dropdown
                ui.horizontal(|ui| {
                    ui.heading("Control Systems Simulation Plotter");
                    ui.separator();

                    ui.label(RichText::new("Graph:").strong());
                    let prev_selected = self.selected_graph;
                    let current_name = self.graphs[self.selected_graph].name().to_string();
                    let graph_count = self.graphs.len();

                    egui::ComboBox::from_id_salt("graph_dropdown")
                        .selected_text(&current_name)
                        .show_ui(ui, |ui| {
                            for idx in 0..graph_count {
                                let name = self.graphs[idx].name().to_string();
                                ui.selectable_value(&mut self.selected_graph, idx, name);
                            }
                        });

                    if self.selected_graph != prev_selected {
                        self.reset_view = true;
                    }

                    ui.separator();
                    if ui.button("Fit / Reset View").clicked() {
                        self.reset_view = true;
                    }
                    ui.checkbox(&mut self.auto_fit_on_change, "Auto-fit on change");

                    ui.separator();
                    egui::widgets::global_theme_preference_buttons(ui);
                });

                ui.separator();

                // Interactive Variables Panel
                let active_name = self.graphs[self.selected_graph].name().to_lowercase();
                egui::Frame::group(ui.style()).show(ui, |ui| {
                    if active_name.contains("chapter 3") || active_name == "ch3" {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(RichText::new("Interactive Variables:").strong());
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.ch3_r, 0.0..=5.0)
                                        .text("Reference (r)")
                                        .step_by(0.1),
                                )
                                .changed()
                            {
                                self.recompute_ch3();
                            }
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.ch3_k, 0.0..=2.0)
                                        .text("Gain (k)")
                                        .step_by(0.02),
                                )
                                .changed()
                            {
                                self.recompute_ch3();
                            }
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.ch3_steps, 20..=500)
                                        .text("Steps")
                                        .step_by(10.0),
                                )
                                .changed()
                            {
                                self.recompute_ch3();
                            }
                        });

                        ui.horizontal_wrapped(|ui| {
                            ui.label(RichText::new("Presets:").italics());
                            if ui.button("Overdamped (k=0.3)").clicked() {
                                self.ch3_k = 0.3;
                                self.recompute_ch3();
                            }
                            if ui.button("Damped Oscillation (k=0.8)").clicked() {
                                self.ch3_k = 0.8;
                                self.recompute_ch3();
                            }
                            if ui.button("Sustained Oscillation (k=1.0)").clicked() {
                                self.ch3_k = 1.0;
                                self.recompute_ch3();
                            }
                            if ui.button("Unstable (k=1.1)").clicked() {
                                self.ch3_k = 1.1;
                                self.recompute_ch3();
                            }
                        });
                    } else if active_name.contains("hit rate") || active_name.contains("hitrate") {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(RichText::new("Interactive Variables:").strong());
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.hr_r, 0.0..=1.0)
                                        .text("Target Hit Rate (r)")
                                        .step_by(0.05),
                                )
                                .changed()
                            {
                                self.recompute_hitrate();
                            }
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.hr_k, 1.0..=200.0)
                                        .text("Gain (k)")
                                        .step_by(1.0),
                                )
                                .changed()
                            {
                                self.recompute_hitrate();
                            }
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.hr_steps, 20..=1000)
                                        .text("Steps")
                                        .step_by(10.0),
                                )
                                .changed()
                            {
                                self.recompute_hitrate();
                            }
                        });

                        ui.horizontal_wrapped(|ui| {
                            ui.label(RichText::new("Presets:").italics());
                            if ui.button("Default (r=0.6, k=50)").clicked() {
                                self.hr_r = 0.6;
                                self.hr_k = 50.0;
                                self.recompute_hitrate();
                            }
                            if ui.button("High Gain (r=0.8, k=120)").clicked() {
                                self.hr_r = 0.8;
                                self.hr_k = 120.0;
                                self.recompute_hitrate();
                            }
                            if ui.button("Low Gain (r=0.5, k=15)").clicked() {
                                self.hr_r = 0.5;
                                self.hr_k = 15.0;
                                self.recompute_hitrate();
                            }
                        });
                    } else if active_name.contains("closed loop") {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(RichText::new("Interactive Variables:").strong());
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.cl_kp, 0.0..=5.0)
                                        .text("Kp")
                                        .step_by(0.05),
                                )
                                .changed()
                            {
                                self.recompute_closed();
                            }
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.cl_ki, 0.0..=0.05)
                                        .text("Ki")
                                        .step_by(0.001),
                                )
                                .changed()
                            {
                                self.recompute_closed();
                            }
                            if ui
                                .add(egui::Slider::new(&mut self.cl_mf, 1..=50).text("Max Flow"))
                                .changed()
                            {
                                self.recompute_closed();
                            }
                            if ui
                                .add(egui::Slider::new(&mut self.cl_mw, 1..=30).text("Max WIP"))
                                .changed()
                            {
                                self.recompute_closed();
                            }
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.cl_steps, 500..=10000)
                                        .text("Steps")
                                        .step_by(100.0),
                                )
                                .changed()
                            {
                                self.recompute_closed();
                            }
                            if ui.button("↺ Re-simulate").clicked() {
                                self.recompute_closed();
                            }
                        });
                    } else if active_name.contains("open loop") {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(RichText::new("Interactive Variables:").strong());
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.ol_target, 0.0..=20.0)
                                        .text("Target (u)")
                                        .step_by(0.5),
                                )
                                .changed()
                            {
                                self.recompute_open();
                            }
                            if ui
                                .add(egui::Slider::new(&mut self.ol_mf, 1..=50).text("Max Flow"))
                                .changed()
                            {
                                self.recompute_open();
                            }
                            if ui
                                .add(egui::Slider::new(&mut self.ol_mw, 1..=30).text("Max WIP"))
                                .changed()
                            {
                                self.recompute_open();
                            }
                            if ui
                                .add(
                                    egui::Slider::new(&mut self.ol_steps, 500..=10000)
                                        .text("Steps")
                                        .step_by(100.0),
                                )
                                .changed()
                            {
                                self.recompute_open();
                            }
                            if ui.button("↺ Re-simulate").clicked() {
                                self.recompute_open();
                            }
                        });
                    } else {
                        ui.horizontal(|ui| {
                            ui.label(RichText::new("Interactive Variables:").strong());
                            ui.label(
                                RichText::new(
                                    "Static dataset (no interactive parameters configured).",
                                )
                                .italics(),
                            );
                        });
                    }
                });

                // Series visibility checkboxes bar
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Series:").strong());

                    match &mut self.graphs[self.selected_graph] {
                        GraphItem::Step(state) => {
                            render_loop_checkboxes(
                                ui,
                                &mut state.show_r,
                                &mut state.show_e,
                                &mut state.show_u,
                                &mut state.show_y,
                                state.series.total_steps,
                            );
                        }
                        GraphItem::Hitrate(state) => {
                            ui.checkbox(
                                &mut state.show_r,
                                RichText::new("Reference (r)").color(COLOR_R),
                            );
                            ui.checkbox(
                                &mut state.show_e,
                                RichText::new("Error (e)").color(COLOR_E),
                            );
                            ui.checkbox(
                                &mut state.show_c,
                                RichText::new("Cumulative (c)").color(COLOR_C),
                            );
                            ui.checkbox(
                                &mut state.show_u,
                                RichText::new("Control (u)").color(COLOR_U),
                            );
                            ui.checkbox(
                                &mut state.show_y,
                                RichText::new("Output (y)").color(COLOR_Y),
                            );
                            ui.separator();
                            ui.label(format!("Steps: {}", state.series.total_steps));
                        }
                    }
                });

                ui.separator();

                // Main Plot area
                let plot_id = format!("graph_plot_{}", self.selected_graph);
                let (x_label, y_label) = ("Step (t)", "Value");

                let mut plot = Plot::new(plot_id)
                    .legend(Legend::default().position(Corner::RightTop))
                    .x_axis_label(x_label)
                    .y_axis_label(y_label)
                    .allow_scroll(false);

                if self.reset_view {
                    plot = plot.reset();
                    self.reset_view = false;
                }

                let scroll_delta = ui.input(|i| i.smooth_scroll_delta.y);

                plot.show(ui, |plot_ui| {
                    if scroll_delta != 0.0 && plot_ui.response().contains_pointer() {
                        let zoom_factor = (scroll_delta * 0.005).exp();
                        plot_ui.zoom_bounds_around_hovered(egui::Vec2::splat(zoom_factor));
                    }

                    match &self.graphs[self.selected_graph] {
                        GraphItem::Step(state) => {
                            state.series.render(
                                plot_ui,
                                state.show_r,
                                state.show_e,
                                state.show_u,
                                state.show_y,
                            );
                        }
                        GraphItem::Hitrate(state) => {
                            state.series.render(
                                plot_ui,
                                state.show_r,
                                state.show_e,
                                state.show_c,
                                state.show_u,
                                state.show_y,
                            );
                        }
                    }
                });
            });
        });
    }
}

pub fn run(graphs: Vec<GraphData>) -> eframe::Result<()> {
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
        Box::new(|_cc| Ok(Box::new(SimPlotApp::new(graphs)))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_data_name_and_helpers() {
        let step = GraphData::step("Test Step", vec![]);
        assert_eq!(step.name(), "Test Step");

        let hr = GraphData::hitrate("Test Hitrate", vec![]);
        assert_eq!(hr.name(), "Test Hitrate");
    }

    #[test]
    fn test_sim_plot_app_empty() {
        let app = SimPlotApp::new(vec![]);
        assert_eq!(app.graphs.len(), 0);
        assert_eq!(app.selected_graph, 0);
    }

    #[test]
    fn test_sim_plot_app_defaults_to_chapter_3() {
        let g1 = GraphData::step("Open Loop", vec![]);
        let g2 = GraphData::step("Chapter 3", vec![]);
        let app = SimPlotApp::new(vec![g1, g2]);
        assert_eq!(app.selected_graph, 1);
        assert_eq!(app.graphs[app.selected_graph].name(), "Chapter 3");
    }

    #[test]
    fn test_sim_plot_app_recompute_chapter_3() {
        let mut app = SimPlotApp::new(vec![GraphData::step("Chapter 3", vec![])]);
        assert_eq!(app.selected_graph, 0);
        app.recompute_ch3();
        if let GraphItem::Step(state) = &app.graphs[0] {
            assert_eq!(state.series.total_steps, app.ch3_steps);
        } else {
            panic!("Expected Step graph");
        }
    }
}
